use crate::error::TransactionParsingError;
use crate::transaction_parser::discriminators::{CPI_EVENT_DISC, NATIVE_GAS_PAID_EVENT_DISC};
use crate::transaction_parser::message_matching_key::MessageMatchingKey;
use crate::transaction_parser::parser::{Parser, ParserConfig};
use async_trait::async_trait;
use borsh::BorshDeserialize;
use relayer_core::gmp_api::gmp_types::{Amount, CommonEventFields, Event, EventMetadata};
use solana_sdk::pubkey::Pubkey;
use solana_transaction_status::UiCompiledInstruction;
use tracing::{debug, warn};

#[derive(BorshDeserialize, Clone, Debug)]
pub struct NativeGasPaidForContractCallEvent {
    /// The Gas service config PDA
    pub _config_pda: Pubkey,
    /// Destination chain on the Axelar network
    pub destination_chain: String,
    /// Destination address on the Axelar network
    pub destination_address: String,
    /// The payload hash for the event we're paying for
    pub payload_hash: [u8; 32],
    /// The refund address
    pub refund_address: Pubkey,
    /// The amount of SOL to send
    pub gas_fee_amount: u64,
}

pub struct ParserNativeGasPaid {
    signature: String,
    parsed: Option<NativeGasPaidForContractCallEvent>,
    instruction: UiCompiledInstruction,
    config: ParserConfig,
}

impl ParserNativeGasPaid {
    pub(crate) async fn new(
        signature: String,
        instruction: UiCompiledInstruction,
    ) -> Result<Self, TransactionParsingError> {
        Ok(Self {
            signature,
            parsed: None,
            instruction,
            config: ParserConfig {
                event_cpi_discriminator: CPI_EVENT_DISC,
                event_type_discriminator: NATIVE_GAS_PAID_EVENT_DISC,
            },
        })
    }

    fn try_extract_with_config(
        instruction: &UiCompiledInstruction,
        config: ParserConfig,
    ) -> Option<NativeGasPaidForContractCallEvent> {
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
        match NativeGasPaidForContractCallEvent::try_from_slice(payload) {
            Ok(event) => {
                debug!("Native Gas Paid vent={:?}", event);
                Some(event)
            }
            Err(_) => None,
        }
    }
}

#[async_trait]
impl Parser for ParserNativeGasPaid {
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
        let parsed = self
            .parsed
            .clone()
            .ok_or_else(|| TransactionParsingError::Message("Missing parsed".to_string()))?;

        Ok(MessageMatchingKey {
            destination_chain: parsed.destination_chain,
            destination_address: parsed.destination_address,
            payload_hash: parsed.payload_hash,
        })
    }

    async fn event(&self, message_id: Option<String>) -> Result<Event, TransactionParsingError> {
        let parsed = self
            .parsed
            .clone()
            .ok_or_else(|| TransactionParsingError::Message("Missing parsed".to_string()))?;

        let message_id = message_id
            .ok_or_else(|| TransactionParsingError::Message("Missing message_id".to_string()))?;

        Ok(Event::GasCredit {
            common: CommonEventFields {
                r#type: "GAS_CREDIT".to_owned(),
                event_id: format!("{}-gas", self.signature),
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
            refund_address: parsed.refund_address.to_string(),
            payment: Amount {
                token_id: None,
                amount: parsed.gas_fee_amount.to_string(),
            },
        })
    }

    async fn message_id(&self) -> Result<Option<String>, TransactionParsingError> {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use solana_sdk::signature::Signature;
    use solana_transaction_status::UiInstruction;

    use super::*;
    use crate::test_utils::fixtures::transaction_fixtures;
    use crate::transaction_parser::parser_native_gas_paid::ParserNativeGasPaid;
    #[tokio::test]
    async fn test_parser() {
        let txs = transaction_fixtures();

        let tx = txs[0].clone();
        let compiled_ix: UiCompiledInstruction = match tx.ixs[0].instructions[0].clone() {
            UiInstruction::Compiled(ix) => ix,
            _ => panic!("expected a compiled instruction"),
        };

        let mut parser = ParserNativeGasPaid::new(tx.signature.to_string(), compiled_ix)
            .await
            .unwrap();
        assert!(parser.is_match().await.unwrap());
        let sig = Signature::from([
            13, 146, 133, 135, 22, 161, 247, 83, 30, 136, 203, 15, 188, 23, 239, 196, 10, 144, 112,
            176, 21, 102, 85, 185, 180, 186, 52, 99, 159, 235, 208, 16, 199, 133, 34, 135, 175,
            241, 214, 163, 19, 215, 71, 100, 19, 209, 117, 32, 171, 132, 220, 207, 185, 110, 237,
            62, 187, 9, 143, 40, 213, 85, 104, 13,
        ])
        .to_string();
        parser.parse().await.unwrap();
        let event = parser.event(Some(format!("{}-1", sig))).await.unwrap();
        match event {
            Event::GasCredit {
                common,
                message_id,
                refund_address,
                payment,
            } => {
                assert_eq!(common.r#type, "GAS_CREDIT");
                assert_eq!(common.event_id, format!("{}-gas", sig));
                assert_eq!(message_id, format!("{}-1", sig));
                assert_eq!(
                    refund_address,
                    "483jTxdFmFGRnzgx9nBoQM2Zao5mZxKvFgHzTb4Ytn1L"
                );
                assert_eq!(payment.token_id, None);
                assert_eq!(payment.amount, "1000");

                let meta = &common.meta.as_ref().unwrap();
                assert_eq!(meta.tx_id.as_deref(), Some(sig.as_str()));
            }
            _ => panic!("Expected GasCredit event"),
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
        let parser = ParserNativeGasPaid::new(tx.signature.to_string(), compiled_ix)
            .await
            .unwrap();

        assert!(!parser.is_match().await.unwrap());
    }
}
