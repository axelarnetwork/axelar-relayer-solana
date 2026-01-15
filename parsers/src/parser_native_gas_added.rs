use crate::common::check_discriminators_and_address;
use crate::error::TransactionParsingError;
use crate::instruction_index::InstructionIndex;
use crate::message_matching_key::MessageMatchingKey;
use crate::parser::Parser;
use anchor_lang::AnchorDeserialize;
use async_trait::async_trait;
use relayer_core::gmp_api::gmp_types::{Amount, CommonEventFields, Event, EventMetadata};
use solana_axelar_gas_service::events::GasAddedEvent;
use solana_sdk::pubkey::Pubkey;
use solana_transaction_status::UiCompiledInstruction;
use tracing::debug;
use uuid::Uuid;

pub struct ParserNativeGasAdded {
    signature: String,
    parsed: Option<GasAddedEvent>,
    instruction: UiCompiledInstruction,
    expected_contract_address: Pubkey,
    accounts: Vec<String>,
    timestamp: String,
}

impl ParserNativeGasAdded {
    pub(crate) async fn new(
        signature: String,
        instruction: UiCompiledInstruction,
        expected_contract_address: Pubkey,
        accounts: Vec<String>,
        timestamp: String,
    ) -> Result<Self, TransactionParsingError> {
        Ok(Self {
            signature,
            parsed: None,
            instruction,
            expected_contract_address,
            accounts,
            timestamp,
        })
    }

    fn try_extract_with_config(
        instruction: &UiCompiledInstruction,
        expected_contract_address: Pubkey,
        accounts: &[String],
    ) -> Result<GasAddedEvent, TransactionParsingError> {
        let payload =
            check_discriminators_and_address(instruction, expected_contract_address, accounts)?;
        match GasAddedEvent::deserialize(&mut payload.as_slice()) {
            Ok(event) => {
                debug!("Native Gas Added event={:?}", event);
                Ok(event)
            }
            Err(_) => Err(TransactionParsingError::InvalidInstructionData(
                "invalid native gas added event".to_string(),
            )),
        }
    }
}

#[async_trait]
impl Parser for ParserNativeGasAdded {
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
            "MessageMatchingKey is not available for NativeGasAddedEvent".to_string(),
        ))
    }

    async fn event(&self, _message_id: Option<String>) -> Result<Event, TransactionParsingError> {
        let parsed = self
            .parsed
            .clone()
            .ok_or_else(|| TransactionParsingError::Message("Missing parsed".to_string()))?;

        let message_id = self
            .message_id()
            .await?
            .ok_or_else(|| TransactionParsingError::Message("Missing message_id".to_string()))?;

        Ok(Event::GasCredit {
            common: CommonEventFields {
                r#type: "GAS_CREDIT".to_owned(),
                event_id: format!("{}-gas", Uuid::new_v4()),
                meta: Some(EventMetadata {
                    tx_id: Some(self.signature.to_string()),
                    from_address: None,
                    finalized: None,
                    source_context: None,
                    timestamp: self.timestamp.clone(),
                }),
            },
            message_id,
            refund_address: parsed.refund_address.to_string(),
            payment: Amount {
                token_id: None,
                amount: parsed.amount.to_string(),
            },
        })
    }

    async fn message_id(&self) -> Result<Option<String>, TransactionParsingError> {
        if let Some(parsed) = self.parsed.clone() {
            // NOTE: These are emitted by the Programs as 1-indexed to mirror axelarscan and solscan
            let index = InstructionIndex::deserialize(parsed.message_id)?;
            Ok(Some(index.serialize()))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use solana_transaction_status::UiInstruction;

    use super::*;
    use crate::parser_native_gas_added::ParserNativeGasAdded;
    use crate::test_utils::fixtures::transaction_fixtures;
    #[tokio::test]
    async fn test_parser() {
        let txs = transaction_fixtures();

        let tx = txs[4].clone();
        let compiled_ix: UiCompiledInstruction = match tx.ixs[0].instructions[0].clone() {
            UiInstruction::Compiled(ix) => ix,
            _ => panic!("expected a compiled instruction"),
        };

        let mut parser = ParserNativeGasAdded::new(
            tx.signature.to_string(),
            compiled_ix,
            Pubkey::from_str("CJ9f8WFdm3q38pmg426xQf7uum7RqvrmS9R58usHwNX7").unwrap(),
            tx.account_keys,
            tx.timestamp.unwrap_or_default().to_rfc3339(),
        )
        .await
        .unwrap();
        let sig = tx.signature.clone().to_string();
        parser.parse().await.unwrap();
        let event = parser.event(None).await.unwrap();
        match event {
            Event::GasCredit { ref common, .. } => {
                let expected_event = Event::GasCredit {
                    common: CommonEventFields {
                        r#type: "GAS_CREDIT".to_owned(),
                        event_id: common.event_id.clone(),
                        meta: Some(EventMetadata {
                            tx_id: Some(sig.to_string()),
                            from_address: None,
                            finalized: None,
                            source_context: None,
                            timestamp: parser.timestamp.clone(),
                        }),
                    },
                    message_id: { parser.parsed.as_ref().unwrap().message_id.to_string() },
                    refund_address: parser.parsed.as_ref().unwrap().refund_address.to_string(),
                    payment: Amount {
                        token_id: None,
                        amount: parser.parsed.as_ref().unwrap().amount.to_string(),
                    },
                };
                assert_eq!(event, expected_event);
            }
            _ => panic!("Expected GasCredit event"),
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
        let mut parser = ParserNativeGasAdded::new(
            tx.signature.to_string(),
            compiled_ix,
            Pubkey::from_str("CJ9f8WFdm3q38pmg426xQf7uum7RqvrmS9R58usHwNX7").unwrap(),
            tx.account_keys,
            tx.timestamp.unwrap_or_default().to_rfc3339(),
        )
        .await
        .unwrap();

        assert!(parser.parse().await.is_err());
    }
}
