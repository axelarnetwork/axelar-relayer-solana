use crate::error::TransactionParsingError;
use crate::transaction_parser::common::check_discriminators_and_address;
use crate::transaction_parser::discriminators::{CPI_EVENT_DISC, LOG_SIGNERS_ROTATED_EVENT_DISC};
use crate::transaction_parser::message_matching_key::MessageMatchingKey;
use crate::transaction_parser::parser::{Parser, ParserConfig};
use async_trait::async_trait;
use borsh::BorshDeserialize;
use relayer_core::gmp_api::gmp_types::{
    CommonEventFields, Event, EventMetadata, SignersRotatedEventMetadata,
};
use solana_sdk::pubkey::Pubkey;
use solana_transaction_status::UiCompiledInstruction;
use tracing::debug;

#[derive(BorshDeserialize, Clone, Debug)]
pub struct LogSignersRotatedMessage {
    pub signers_hash: String,
    pub epoch: u64,
}

pub struct ParserLogSignersRotated {
    signature: String,
    parsed: Option<LogSignersRotatedMessage>,
    instruction: UiCompiledInstruction,
    config: ParserConfig,
    index: u64,
    accounts: Vec<String>,
}

impl ParserLogSignersRotated {
    pub(crate) async fn new(
        signature: String,
        instruction: UiCompiledInstruction,
        index: u64,
        expected_contract_address: Pubkey,
        accounts: Vec<String>,
    ) -> Result<Self, TransactionParsingError> {
        Ok(Self {
            signature,
            parsed: None,
            instruction,
            config: ParserConfig {
                event_cpi_discriminator: CPI_EVENT_DISC,
                event_type_discriminator: LOG_SIGNERS_ROTATED_EVENT_DISC,
                expected_contract_address,
            },
            index,
            accounts,
        })
    }

    fn try_extract_with_config(
        instruction: &UiCompiledInstruction,
        config: ParserConfig,
        accounts: &[String],
    ) -> Result<LogSignersRotatedMessage, TransactionParsingError> {
        let payload = check_discriminators_and_address(instruction, config, accounts)?;
        match LogSignersRotatedMessage::try_from_slice(payload.into_iter().as_slice()) {
            Ok(event) => {
                debug!("Log Signers Rotated event={:?}", event);
                Ok(event)
            }
            Err(_) => Err(TransactionParsingError::InvalidInstructionData(
                "invalid log signers rotated event".to_string(),
            )),
        }
    }
}

#[async_trait]
impl Parser for ParserLogSignersRotated {
    async fn parse(&mut self) -> Result<bool, TransactionParsingError> {
        if self.parsed.is_none() {
            self.parsed = Some(Self::try_extract_with_config(
                &self.instruction,
                self.config,
                &self.accounts,
            )?);
        }
        Ok(self.parsed.is_some())
    }

    async fn is_match(&mut self) -> Result<bool, TransactionParsingError> {
        match Self::try_extract_with_config(&self.instruction, self.config, &self.accounts) {
            Ok(parsed) => {
                self.parsed = Some(parsed);
                Ok(true)
            }
            Err(_) => Ok(false),
        }
    }

    async fn key(&self) -> Result<MessageMatchingKey, TransactionParsingError> {
        Err(TransactionParsingError::Message(
            "MessageMatchingKey is not available for LogSignersRotatedEvent".to_string(),
        ))
    }

    async fn event(&self, _: Option<String>) -> Result<Event, TransactionParsingError> {
        let parsed = self
            .parsed
            .clone()
            .ok_or_else(|| TransactionParsingError::Message("Missing parsed".to_string()))?;

        Ok(Event::SignersRotated {
            common: CommonEventFields {
                r#type: "SIGNERS_ROTATED".to_owned(),
                event_id: format!("{}-signers-rotated", self.signature.clone()),
                meta: Some(SignersRotatedEventMetadata {
                    common_meta: EventMetadata {
                        tx_id: Some(self.signature.clone()),
                        from_address: None,
                        finalized: None,
                        source_context: None,
                        timestamp: chrono::Utc::now()
                            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                    },
                    signers_hash: Some(parsed.signers_hash.clone()),
                    epoch: Some(parsed.epoch),
                }),
            },
            message_id: format!("{}-{}", self.signature, self.index),
        })
    }

    async fn message_id(&self) -> Result<Option<String>, TransactionParsingError> {
        Ok(Some(format!("{}-{}", self.signature, self.index)))
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use solana_transaction_status::UiInstruction;

    use super::*;
    use crate::test_utils::fixtures::transaction_fixtures;
    use crate::transaction_parser::parser_signers_rotated::ParserLogSignersRotated;
    #[tokio::test]
    async fn test_parser() {
        let txs = transaction_fixtures();

        let tx = txs[2].clone();
        let compiled_ix: UiCompiledInstruction = match tx.ixs[0].instructions[0].clone() {
            UiInstruction::Compiled(ix) => ix,
            _ => panic!("expected a compiled instruction"),
        };

        let mut parser = ParserLogSignersRotated::new(
            tx.signature.to_string(),
            compiled_ix,
            1,
            Pubkey::from_str("7RdSDLUUy37Wqc6s9ebgo52AwhGiw4XbJWZJgidQ1fJc").unwrap(),
            tx.account_keys,
        )
        .await
        .unwrap();
        assert!(parser.is_match().await.unwrap());
        let sig = tx.signature.clone().to_string();
        parser.parse().await.unwrap();
        let event = parser.event(None).await.unwrap();
        match event {
            Event::SignersRotated { .. } => {
                let expected_event = Event::SignersRotated {
                    common: CommonEventFields {
                        r#type: "SIGNERS_ROTATED".to_owned(),
                        event_id: format!("{}-signers-rotated", sig),
                        meta: Some(SignersRotatedEventMetadata {
                            common_meta: EventMetadata {
                                tx_id: Some(sig.to_string()),
                                from_address: None,
                                finalized: None,
                                source_context: None,
                                timestamp: chrono::Utc::now()
                                    .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                            },
                            signers_hash: Some(
                                parser.parsed.as_ref().unwrap().signers_hash.clone(),
                            ),
                            epoch: Some(parser.parsed.as_ref().unwrap().epoch),
                        }),
                    },
                    message_id: format!(
                        "{}-{}",
                        parser.parsed.as_ref().unwrap().signers_hash,
                        parser.parsed.as_ref().unwrap().epoch
                    ),
                };
                assert_eq!(event, expected_event);
            }
            _ => panic!("Expected GasRefunded event"),
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
        let mut parser = ParserLogSignersRotated::new(
            tx.signature.to_string(),
            compiled_ix,
            1,
            Pubkey::from_str("7RdSDLUUy37Wqc6s9ebgo52AwhGiw4XbJWZJgidQ1fJc").unwrap(),
            tx.account_keys,
        )
        .await
        .unwrap();

        assert!(!parser.is_match().await.unwrap());
    }
}
