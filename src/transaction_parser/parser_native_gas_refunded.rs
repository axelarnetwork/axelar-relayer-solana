use crate::error::TransactionParsingError;
use crate::transaction_parser::common::check_discriminators_and_address;
use crate::transaction_parser::discriminators::{CPI_EVENT_DISC, NATIVE_GAS_REFUNDED_EVENT_DISC};
use crate::transaction_parser::instruction_index::InstructionIndex;
use crate::transaction_parser::message_matching_key::MessageMatchingKey;
use crate::transaction_parser::parser::{Parser, ParserConfig};
use async_trait::async_trait;
use borsh::BorshDeserialize;
use relayer_core::gmp_api::gmp_types::{Amount, CommonEventFields, Event, EventMetadata};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use solana_transaction_status::UiCompiledInstruction;
use tracing::debug;

#[derive(BorshDeserialize, Clone, Debug)]
pub struct NativeGasRefundedEvent {
    /// Solana transaction signature
    pub tx_hash: [u8; 64],
    /// The Gas service config PDA
    pub _config_pda: Pubkey,
    /// The log index
    pub log_index: String,
    /// The receiver of the refund
    pub receiver: Pubkey,
    /// amount of SOL
    pub fees: u64,
}

pub struct ParserNativeGasRefunded {
    signature: String,
    parsed: Option<NativeGasRefundedEvent>,
    instruction: UiCompiledInstruction,
    config: ParserConfig,
    cost_units: u64,
    accounts: Vec<String>,
}

impl ParserNativeGasRefunded {
    pub(crate) async fn new(
        signature: String,
        instruction: UiCompiledInstruction,
        expected_contract_address: Pubkey,
        cost_units: u64,
        accounts: Vec<String>,
    ) -> Result<Self, TransactionParsingError> {
        Ok(Self {
            signature,
            parsed: None,
            instruction,
            config: ParserConfig {
                event_cpi_discriminator: CPI_EVENT_DISC,
                event_type_discriminator: NATIVE_GAS_REFUNDED_EVENT_DISC,
                expected_contract_address,
            },
            cost_units,
            accounts,
        })
    }

    fn try_extract_with_config(
        instruction: &UiCompiledInstruction,
        config: ParserConfig,
        accounts: &[String],
    ) -> Result<NativeGasRefundedEvent, TransactionParsingError> {
        let payload = check_discriminators_and_address(instruction, config, accounts)?;
        match NativeGasRefundedEvent::try_from_slice(payload.into_iter().as_slice()) {
            Ok(event) => {
                debug!("Native Gas Refunded event={:?}", event);
                Ok(event)
            }
            Err(_) => Err(TransactionParsingError::InvalidInstructionData(
                "invalid native gas refunded event".to_string(),
            )),
        }
    }
}

#[async_trait]
impl Parser for ParserNativeGasRefunded {
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
            "MessageMatchingKey is not available for NativeGasRefundedEvent".to_string(),
        ))
    }

    async fn event(&self, _message_id: Option<String>) -> Result<Event, TransactionParsingError> {
        let parsed = self
            .parsed
            .clone()
            .ok_or_else(|| TransactionParsingError::Message("Missing parsed".to_string()))?;

        let message_id = match self.message_id().await? {
            Some(id) => id,
            None => {
                return Err(TransactionParsingError::Message(
                    "Missing message id".to_string(),
                ))
            }
        };

        Ok(Event::GasRefunded {
            common: CommonEventFields {
                r#type: "GAS_REFUNDED".to_owned(),
                event_id: format!("{}-refund", self.signature.clone()),
                meta: Some(EventMetadata {
                    tx_id: Some(self.signature.to_string()),
                    from_address: None,
                    finalized: None,
                    source_context: None,
                    timestamp: chrono::Utc::now()
                        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                }),
            },
            message_id,
            recipient_address: parsed.receiver.to_string(),
            refunded_amount: Amount {
                token_id: None,
                amount: parsed.fees.to_string(),
            },
            cost: Amount {
                amount: self.cost_units.to_string(),
                token_id: None,
            },
        })
    }

    async fn message_id(&self) -> Result<Option<String>, TransactionParsingError> {
        if let Some(parsed) = self.parsed.clone() {
            // Deserialize and then reserialize to ensure that formatting is correct
            let index = InstructionIndex::deserialize(parsed.log_index)?;
            Ok(Some(format!(
                "{}-{}",
                Signature::from(parsed.tx_hash),
                index.serialize()
            )))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use solana_sdk::signature::Signature;
    use solana_transaction_status::UiInstruction;

    use super::*;
    use crate::test_utils::fixtures::transaction_fixtures;
    use crate::transaction_parser::parser_native_gas_refunded::ParserNativeGasRefunded;
    #[tokio::test]
    async fn test_parser() {
        let txs = transaction_fixtures();

        let tx = txs[2].clone();
        let compiled_ix: UiCompiledInstruction = match tx.ixs[0].instructions[0].clone() {
            UiInstruction::Compiled(ix) => ix,
            _ => panic!("expected a compiled instruction"),
        };

        let mut parser = ParserNativeGasRefunded::new(
            tx.signature.to_string(),
            compiled_ix,
            Pubkey::from_str("7RdSDLUUy37Wqc6s9ebgo52AwhGiw4XbJWZJgidQ1fJc").unwrap(),
            tx.cost_units,
            tx.account_keys,
        )
        .await
        .unwrap();
        assert!(parser.is_match().await.unwrap());
        let sig = tx.signature.clone().to_string();
        parser.parse().await.unwrap();
        let event = parser.event(None).await.unwrap();
        match event {
            Event::GasRefunded { .. } => {
                let expected_event = Event::GasRefunded {
                    common: CommonEventFields {
                        r#type: "GAS_REFUNDED".to_owned(),
                        event_id: format!("{}-refund", sig),
                        meta: Some(EventMetadata {
                            tx_id: Some(sig.to_string()),
                            from_address: None,
                            finalized: None,
                            source_context: None,
                            timestamp: chrono::Utc::now()
                                .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                        }),
                    },
                    message_id: format!(
                        "{}-{}",
                        Signature::from(parser.parsed.as_ref().unwrap().tx_hash),
                        parser.parsed.as_ref().unwrap().log_index
                    ),
                    recipient_address: parser.parsed.as_ref().unwrap().receiver.to_string(),
                    refunded_amount: Amount {
                        token_id: None,
                        amount: parser.parsed.as_ref().unwrap().fees.to_string(),
                    },
                    cost: Amount {
                        amount: tx.cost_units.to_string(),
                        token_id: None,
                    },
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
        let mut parser = ParserNativeGasRefunded::new(
            tx.signature.to_string(),
            compiled_ix,
            Pubkey::from_str("7RdSDLUUy37Wqc6s9ebgo52AwhGiw4XbJWZJgidQ1fJc").unwrap(),
            tx.cost_units,
            tx.account_keys,
        )
        .await
        .unwrap();

        assert!(!parser.is_match().await.unwrap());
    }
}
