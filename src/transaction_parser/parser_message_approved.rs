use crate::error::TransactionParsingError;
use crate::transaction_parser::discriminators::{CPI_EVENT_DISC, MESSAGE_APPROVED_EVENT_DISC};
use crate::transaction_parser::message_matching_key::MessageMatchingKey;
use crate::transaction_parser::parser::{Parser, ParserConfig};
use async_trait::async_trait;
use borsh::BorshDeserialize;
use bs58::encode;
use relayer_core::gmp_api::gmp_types::{
    Amount, CommonEventFields, Event, EventMetadata, GatewayV2Message, MessageApprovedEventMetadata,
};
use solana_sdk::pubkey::Pubkey;
use solana_transaction_status::UiCompiledInstruction;
use tracing::{debug, warn};

#[derive(BorshDeserialize, Clone, Debug)]
pub struct MessageApprovedEvent {
    pub command_id: [u8; 32],
    pub destination_address: Pubkey,
    pub payload_hash: [u8; 32],
    pub source_chain: String,
    pub message_id: String,
    pub source_address: String,
    pub destination_chain: String,
}

pub struct ParserMessageApproved {
    signature: String,
    parsed: Option<MessageApprovedEvent>,
    instruction: UiCompiledInstruction,
    config: ParserConfig,
}

impl ParserMessageApproved {
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
                event_type_discriminator: MESSAGE_APPROVED_EVENT_DISC,
            },
        })
    }

    fn try_extract_with_config(
        instruction: &UiCompiledInstruction,
        config: ParserConfig,
    ) -> Option<MessageApprovedEvent> {
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
        match MessageApprovedEvent::try_from_slice(payload) {
            Ok(event) => {
                debug!("Message Approved event={:?}", event);
                Some(event)
            }
            Err(_) => None,
        }
    }
}

#[async_trait]
impl Parser for ParserMessageApproved {
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
        let key = MessageMatchingKey {
            destination_chain: parsed.destination_chain.clone(),
            destination_address: parsed.destination_address.to_string(),
            payload_hash: parsed.payload_hash,
        };

        Ok(key)
    }

    async fn event(&self, _: Option<String>) -> Result<Event, TransactionParsingError> {
        let parsed = self
            .parsed
            .clone()
            .ok_or_else(|| TransactionParsingError::Message("Missing parsed".to_string()))?;

        Ok(Event::MessageApproved {
            common: CommonEventFields {
                r#type: "MESSAGE_APPROVED".to_owned(),
                event_id: self.signature.clone(),
                meta: Some(MessageApprovedEventMetadata {
                    common_meta: EventMetadata {
                        tx_id: Some(self.signature.clone()),
                        from_address: None,
                        finalized: None,
                        source_context: None,
                        timestamp: chrono::Utc::now()
                            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                    },
                    command_id: Some(encode(parsed.command_id).into_string()),
                }),
            },
            message: GatewayV2Message {
                message_id: parsed.message_id.clone(),
                source_chain: parsed.source_chain.clone(),
                source_address: parsed.source_address.clone(),
                destination_address: parsed.destination_address.to_string(),
                // shold this be hex encoded?
                payload_hash: hex::encode(parsed.payload_hash),
            },
            cost: Amount {
                token_id: None,
                amount: "0".to_string(),
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
    use crate::transaction_parser::parser_message_approved::ParserMessageApproved;
    #[tokio::test]
    async fn test_parser() {
        let txs = transaction_fixtures();

        let tx = txs[1].clone();
        let compiled_ix: UiCompiledInstruction = match tx.ixs[0].instructions[1].clone() {
            UiInstruction::Compiled(ix) => ix,
            _ => panic!("expected a compiled instruction"),
        };

        let mut parser = ParserMessageApproved::new(tx.signature.to_string(), compiled_ix)
            .await
            .unwrap();
        assert!(parser.is_match().await.unwrap());
        let sig = Signature::from([
            40, 230, 73, 108, 137, 197, 160, 39, 204, 198, 21, 197, 67, 88, 19, 21, 106, 217, 129,
            223, 19, 151, 242, 199, 144, 241, 23, 113, 90, 15, 12, 205, 134, 111, 95, 166, 31, 93,
            42, 71, 167, 183, 101, 94, 161, 165, 85, 205, 76, 215, 192, 176, 200, 225, 48, 206,
            147, 33, 28, 240, 150, 167, 28, 117,
        ])
        .to_string();
        parser.parse().await.unwrap();
        let event = parser.event(Some(format!("{}-1", sig))).await.unwrap();
        match event {
            Event::MessageApproved { ref common, .. } => {
                let expected_event = Event::MessageApproved {
                    common: CommonEventFields {
                        r#type: "MESSAGE_APPROVED".to_owned(),
                        event_id: sig.clone(),
                        meta: Some(MessageApprovedEventMetadata {
                            common_meta: EventMetadata {
                                tx_id: Some(sig.clone()),
                                from_address: None,
                                finalized: None,
                                source_context: None,
                                timestamp: common
                                    .meta
                                    .as_ref()
                                    .unwrap()
                                    .common_meta
                                    .timestamp
                                    .clone(),
                            },
                            command_id: Some(
                                encode(parser.parsed.as_ref().unwrap().command_id).into_string(),
                            ),
                        }),
                    },
                    message: GatewayV2Message {
                        message_id: parser.parsed.as_ref().unwrap().message_id.clone(),
                        source_chain: parser.parsed.as_ref().unwrap().source_chain.clone(),
                        source_address: parser.parsed.as_ref().unwrap().source_address.clone(),
                        destination_address: parser
                            .parsed
                            .as_ref()
                            .unwrap()
                            .destination_address
                            .to_string(),
                        payload_hash: hex::encode(parser.parsed.as_ref().unwrap().payload_hash),
                    },
                    cost: Amount {
                        token_id: None,
                        amount: "0".to_string(),
                    },
                };
                assert_eq!(event, expected_event);
            }
            _ => panic!("Expected MessageApproved event"),
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
        let parser = ParserMessageApproved::new(tx.signature.to_string(), compiled_ix)
            .await
            .unwrap();

        assert!(!parser.is_match().await.unwrap());
    }
}
