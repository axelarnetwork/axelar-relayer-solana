use crate::common::check_discriminators_and_address;
use crate::error::TransactionParsingError;
use crate::instruction_index::InstructionIndex;
use crate::message_matching_key::MessageMatchingKey;
use crate::parser::Parser;
use anchor_lang::AnchorDeserialize;
use async_trait::async_trait;
use base64::prelude::BASE64_STANDARD;
use base64::Engine as _;
use relayer_core::gmp_api::gmp_types::{
    CommonEventFields, Event, EventMetadata, SignersRotatedEventMetadata,
};
use solana_axelar_gateway::events::VerifierSetRotatedEvent;
use solana_sdk::pubkey::Pubkey;
use solana_transaction_status::UiCompiledInstruction;
use tracing::debug;
use uuid::Uuid;

pub struct ParserSignersRotated {
    signature: String,
    parsed: Option<VerifierSetRotatedEvent>,
    instruction: UiCompiledInstruction,
    expected_contract_address: Pubkey,
    index: InstructionIndex,
    accounts: Vec<String>,
    timestamp: String,
}

impl ParserSignersRotated {
    pub(crate) fn new(
        signature: String,
        instruction: UiCompiledInstruction,
        index: InstructionIndex,
        expected_contract_address: Pubkey,
        accounts: Vec<String>,
        timestamp: String,
    ) -> Result<Self, TransactionParsingError> {
        Ok(Self {
            signature,
            parsed: None,
            instruction,
            expected_contract_address,
            index,
            accounts,
            timestamp,
        })
    }

    fn try_extract_with_config(
        instruction: &UiCompiledInstruction,
        expected_contract_address: Pubkey,
        accounts: &[String],
    ) -> Result<VerifierSetRotatedEvent, TransactionParsingError> {
        let payload =
            check_discriminators_and_address(instruction, expected_contract_address, accounts)?;
        match VerifierSetRotatedEvent::deserialize(&mut payload.as_slice()) {
            Ok(event) => {
                debug!("Verifier Set Rotated event={:?}", event);
                Ok(event)
            }
            Err(_) => Err(TransactionParsingError::InvalidInstructionData(
                "invalid verifier set rotated event".to_string(),
            )),
        }
    }
}

#[async_trait]
impl Parser for ParserSignersRotated {
    async fn parse(&mut self) -> Result<bool, TransactionParsingError> {
        if self.parsed.is_none() {
            self.parsed = Some(Self::try_extract_with_config(
                &self.instruction,
                self.expected_contract_address,
                &self.accounts,
            )?);
        }
        Ok(self.parsed.is_some())
    }

    async fn key(&self) -> Result<MessageMatchingKey, TransactionParsingError> {
        Err(TransactionParsingError::Message(
            "MessageMatchingKey is not available for VerifierSetRotatedEvent".to_string(),
        ))
    }

    async fn event(&self, _: Option<String>) -> Result<Event, TransactionParsingError> {
        let parsed = self
            .parsed
            .clone()
            .ok_or_else(|| TransactionParsingError::Message("Missing parsed".to_string()))?;

        let message_id = self
            .message_id()
            .await?
            .ok_or_else(|| TransactionParsingError::Message("Missing message_id".to_string()))?;

        // let epoch = parsed.epoch.as_u64();
        let epoch = {
            let le = parsed.epoch.to_le_bytes();
            let first8 = le.get(..8).ok_or_else(|| {
                TransactionParsingError::InvalidInstructionData("epoch too short".to_string())
            })?;
            let arr: [u8; 8] = first8.try_into().map_err(|_| {
                TransactionParsingError::InvalidInstructionData("epoch cast failed".to_string())
            })?;
            u64::from_le_bytes(arr)
        };

        Ok(Event::SignersRotated {
            common: CommonEventFields {
                r#type: "SIGNERS_ROTATED".to_owned(),
                event_id: format!("{}-signers-rotated", Uuid::new_v4()),
                meta: Some(SignersRotatedEventMetadata {
                    common_meta: EventMetadata {
                        tx_id: Some(self.signature.clone()),
                        from_address: None,
                        finalized: None,
                        source_context: None,
                        timestamp: self.timestamp.clone(),
                    },
                    signers_hash: Some(BASE64_STANDARD.encode(parsed.verifier_set_hash)),
                    epoch: Some(epoch),
                }),
            },
            message_id,
        })
    }

    async fn message_id(&self) -> Result<Option<String>, TransactionParsingError> {
        Ok(Some(self.index.serialize()))
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use solana_transaction_status::UiInstruction;

    use super::*;
    use crate::parser_signers_rotated::ParserSignersRotated;
    use crate::test_utils::fixtures::transaction_fixtures;

    #[tokio::test]
    async fn test_parser() {
        let txs = transaction_fixtures();

        let tx = txs[11].clone();
        let compiled_ix: UiCompiledInstruction = match tx.ixs[0].instructions[0].clone() {
            UiInstruction::Compiled(ix) => ix,
            _ => panic!("expected a compiled instruction"),
        };

        let mut parser = ParserSignersRotated::new(
            tx.signature.to_string(),
            compiled_ix,
            InstructionIndex::new(tx.signature.to_string(), 1, 2),
            Pubkey::from_str("gtwT4uGVTYSPnTGv6rSpMheyFyczUicxVWKqdtxNGw9").unwrap(),
            tx.account_keys,
            tx.timestamp.unwrap_or_default().to_rfc3339(),
        )
        .unwrap();
        assert!(parser.parse().await.unwrap());
        let sig = tx.signature.clone().to_string();
        parser.parse().await.unwrap();
        let event = parser.event(None).await.unwrap();
        match event {
            Event::SignersRotated { ref common, .. } => {
                // Convert U256 epoch to u64 (same logic as in the event() method)
                let epoch_bytes = parser.parsed.as_ref().unwrap().epoch.to_le_bytes();
                let epoch = u64::from_le_bytes(epoch_bytes[..8].try_into().unwrap());

                let expected_event = Event::SignersRotated {
                    common: CommonEventFields {
                        r#type: "SIGNERS_ROTATED".to_owned(),
                        event_id: common.event_id.clone(),
                        meta: Some(SignersRotatedEventMetadata {
                            common_meta: EventMetadata {
                                tx_id: Some(sig.to_string()),
                                from_address: None,
                                finalized: None,
                                source_context: None,
                                timestamp: parser.timestamp.clone(),
                            },
                            signers_hash: Some(
                                BASE64_STANDARD
                                    .encode(parser.parsed.as_ref().unwrap().verifier_set_hash),
                            ),
                            epoch: Some(epoch),
                        }),
                    },
                    message_id: format!(
                        "{}-{}.{}",
                        sig, parser.index.outer_index, parser.index.inner_index
                    ),
                };
                assert_eq!(event, expected_event);
            }
            _ => panic!("Expected SignersRotated event"),
        }
    }

    #[tokio::test]
    async fn test_no_match() {
        let txs = transaction_fixtures();

        let tx = txs[0].clone();
        let compiled_ix: UiCompiledInstruction = match tx.ixs[0].instructions[0].clone() {
            UiInstruction::Compiled(ix) => ix,
            _ => panic!("expected a compiled instruction"),
        };
        let mut parser = ParserSignersRotated::new(
            tx.signature.to_string(),
            compiled_ix,
            InstructionIndex::new(tx.signature.to_string(), 1, 2),
            Pubkey::from_str("gtwT4uGVTYSPnTGv6rSpMheyFyczUicxVWKqdtxNGw9").unwrap(),
            tx.account_keys,
            tx.timestamp.unwrap_or_default().to_rfc3339(),
        )
        .unwrap();

        assert!(parser.parse().await.is_err());
    }
}
