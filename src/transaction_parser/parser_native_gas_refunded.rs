use crate::error::TransactionParsingError;
use crate::transaction_parser::discriminators::{CPI_EVENT_DISC, NATIVE_GAS_REFUNDED_EVENT_DISC};
use crate::transaction_parser::message_matching_key::MessageMatchingKey;
use crate::transaction_parser::parser::{Parser, ParserConfig};
use async_trait::async_trait;
use borsh::BorshDeserialize;
use relayer_core::gmp_api::gmp_types::{Amount, CommonEventFields, Event, EventMetadata};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use solana_transaction_status::UiCompiledInstruction;
use tracing::{debug, warn};

#[derive(BorshDeserialize, Clone, Debug)]
pub struct NativeGasRefundedEvent {
    /// Solana transaction signature
    pub tx_hash: [u8; 64],
    /// The Gas service config PDA
    pub _config_pda: Pubkey,
    /// The log index
    pub log_index: u64,
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
}

impl ParserNativeGasRefunded {
    pub(crate) async fn new(
        signature: String,
        instruction: UiCompiledInstruction,
        cost_units: u64,
    ) -> Result<Self, TransactionParsingError> {
        Ok(Self {
            signature,
            parsed: None,
            instruction,
            config: ParserConfig {
                event_cpi_discriminator: CPI_EVENT_DISC,
                event_type_discriminator: NATIVE_GAS_REFUNDED_EVENT_DISC,
            },
            cost_units,
        })
    }

    fn try_extract_with_config(
        instruction: &UiCompiledInstruction,
        config: ParserConfig,
    ) -> Option<NativeGasRefundedEvent> {
        let bytes = match bs58::decode(&instruction.data).into_vec() {
            Ok(bytes) => bytes,
            Err(e) => {
                warn!("failed to decode bytes: {:?}", e);
                return None;
            }
        };
        if bytes.len() < 16 {
            return None;
        }

        if bytes.get(0..8) != Some(&config.event_cpi_discriminator) {
            debug!(
                "expected event cpi discriminator, got {:?}",
                bytes.get(0..8)
            );
            return None;
        }
        if bytes.get(8..16) != Some(&config.event_type_discriminator) {
            debug!(
                "expected event type discriminator, got {:?}",
                bytes.get(8..16)
            );
            return None;
        }

        let payload = bytes.get(16..)?;
        match NativeGasRefundedEvent::try_from_slice(payload) {
            Ok(event) => {
                debug!("Native Gas Refunded event={:?}", event);
                Some(event)
            }
            Err(_) => None,
        }
    }
}

#[async_trait]
impl Parser for ParserNativeGasRefunded {
    async fn parse(&mut self) -> Result<bool, TransactionParsingError> {
        if self.parsed.is_none() {
            self.parsed = Self::try_extract_with_config(&self.instruction, self.config);
        }
        Ok(self.parsed.is_some())
    }

    async fn is_match(&self) -> Result<bool, TransactionParsingError> {
        Ok(Self::try_extract_with_config(&self.instruction, self.config).is_some())
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
            Ok(Some(format!(
                "{}-{}",
                Signature::from(parsed.tx_hash),
                parsed.log_index
            )))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
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

        let mut parser =
            ParserNativeGasRefunded::new(tx.signature.to_string(), compiled_ix, tx.cost_units)
                .await
                .unwrap();
        assert!(parser.is_match().await.unwrap());
        let sig = Signature::from([
            21, 54, 228, 195, 127, 81, 55, 72, 35, 80, 62, 172, 38, 18, 236, 72, 70, 136, 65, 235,
            179, 121, 62, 7, 175, 238, 141, 146, 203, 196, 221, 250, 129, 97, 11, 134, 188, 216,
            168, 98, 173, 10, 168, 209, 5, 5, 112, 212, 153, 179, 78, 88, 255, 232, 53, 75, 24,
            116, 95, 191, 172, 206, 21, 0,
        ])
        .to_string();
        parser.parse().await.unwrap();
        let event = parser.event(None).await.unwrap();
        match event {
            Event::GasRefunded {
                common,
                message_id,
                recipient_address,
                refunded_amount,
                cost,
            } => {
                assert_eq!(common.r#type, "GAS_REFUNDED");
                assert_eq!(common.event_id, format!("{}-refund", sig));
                assert_eq!(
                    message_id,
                    format!(
                        "{}-{}",
                        Signature::from(parser.parsed.as_ref().unwrap().tx_hash),
                        parser.parsed.unwrap().log_index
                    )
                );
                assert_eq!(
                    recipient_address,
                    "483jTxdFmFGRnzgx9nBoQM2Zao5mZxKvFgHzTb4Ytn1L"
                );
                assert_eq!(refunded_amount.token_id, None);
                assert_eq!(refunded_amount.amount, "500");
                assert_eq!(cost.amount, "13085");

                let meta = &common.meta.as_ref().unwrap();
                assert_eq!(meta.tx_id.as_deref(), Some(sig.as_str()));
            }
            _ => panic!("Expected GasRefunded event"),
        }
    }

    #[tokio::test]
    async fn test_no_match() {
        let txs = transaction_fixtures();

        let tx = txs[0].clone();
        let compiled_ix: UiCompiledInstruction = match tx.ixs[1].instructions[0].clone() {
            UiInstruction::Compiled(ix) => ix,
            _ => panic!("expected a compiled instruction"),
        };
        let parser =
            ParserNativeGasRefunded::new(tx.signature.to_string(), compiled_ix, tx.cost_units)
                .await
                .unwrap();

        assert!(!parser.is_match().await.unwrap());
    }
}
