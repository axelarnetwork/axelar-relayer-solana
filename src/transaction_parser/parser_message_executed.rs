use crate::error::TransactionParsingError;
use crate::transaction_parser::discriminators::{CPI_EVENT_DISC, MESSAGE_EXECUTED_EVENT_DISC};
use crate::transaction_parser::message_matching_key::MessageMatchingKey;
use crate::transaction_parser::parser::{Parser, ParserConfig};
use async_trait::async_trait;
use borsh::BorshDeserialize;
use bs58::encode;
use relayer_core::gmp_api::gmp_types::{
    Amount, CommonEventFields, Event, EventMetadata, MessageExecutedEventMetadata,
    MessageExecutionStatus,
};
use solana_sdk::pubkey::Pubkey;
use solana_transaction_status::UiCompiledInstruction;
use tracing::{debug, warn};

#[derive(BorshDeserialize, Clone, Debug)]
pub struct MessageExecutedEvent {
    pub command_id: [u8; 32],
    pub _destination_address: Pubkey,
    pub _payload_hash: [u8; 32],
    pub source_chain: String,
    pub message_id: String,
    pub _source_address: String,
    pub _destination_chain: String,
}

pub struct ParserMessageExecuted {
    signature: String,
    parsed: Option<MessageExecutedEvent>,
    instruction: UiCompiledInstruction,
    config: ParserConfig,
}

impl ParserMessageExecuted {
    pub(crate) async fn new(
        signature: String,
        instruction: UiCompiledInstruction,
        expected_contract_address: Pubkey,
    ) -> Result<Self, TransactionParsingError> {
        Ok(Self {
            signature,
            parsed: None,
            instruction,
            config: ParserConfig {
                event_cpi_discriminator: CPI_EVENT_DISC,
                event_type_discriminator: MESSAGE_EXECUTED_EVENT_DISC,
                expected_contract_address,
            },
        })
    }

    fn try_extract_with_config(
        instruction: &UiCompiledInstruction,
        config: ParserConfig,
    ) -> Option<MessageExecutedEvent> {
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
        match MessageExecutedEvent::try_from_slice(payload) {
            Ok(event) => {
                debug!("Message Executed event={:?}", event);
                Some(event)
            }
            Err(_) => None,
        }
    }
}

#[async_trait]
impl Parser for ParserMessageExecuted {
    async fn parse(&mut self) -> Result<bool, TransactionParsingError> {
        if self.parsed.is_none() {
            self.parsed = Self::try_extract_with_config(&self.instruction, self.config);
        }
        Ok(self.parsed.is_some())
    }

    async fn is_match(&mut self) -> Result<bool, TransactionParsingError> {
        match Self::try_extract_with_config(&self.instruction, self.config) {
            Some(parsed) => {
                self.parsed = Some(parsed);
                Ok(true)
            }
            None => Ok(false),
        }
    }

    async fn key(&self) -> Result<MessageMatchingKey, TransactionParsingError> {
        Err(TransactionParsingError::Message(
            "MessageMatchingKey is not available for MessageExecutedEvent".to_string(),
        ))
    }

    async fn event(&self, _: Option<String>) -> Result<Event, TransactionParsingError> {
        let parsed = self
            .parsed
            .clone()
            .ok_or_else(|| TransactionParsingError::Message("Missing parsed".to_string()))?;

        Ok(Event::MessageExecuted {
            common: CommonEventFields {
                r#type: "MESSAGE_EXECUTED".to_owned(),
                event_id: self.signature.clone(),
                meta: Some(MessageExecutedEventMetadata {
                    common_meta: EventMetadata {
                        tx_id: Some(self.signature.clone()),
                        from_address: None,
                        finalized: None,
                        source_context: None,
                        timestamp: chrono::Utc::now()
                            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                    },
                    command_id: Some(encode(parsed.command_id).into_string()),
                    child_message_ids: None,
                    revert_reason: None,
                }),
            },
            message_id: parsed.message_id.clone(),
            source_chain: parsed.source_chain,
            status: MessageExecutionStatus::SUCCESSFUL,
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
    use std::str::FromStr;

    use solana_transaction_status::UiInstruction;

    use super::*;
    use crate::test_utils::fixtures::transaction_fixtures;
    use crate::transaction_parser::parser_message_executed::ParserMessageExecuted;
    #[tokio::test]
    async fn test_parser() {
        let txs = transaction_fixtures();

        let tx = txs[3].clone();
        let compiled_ix: UiCompiledInstruction = match tx.ixs[0].instructions[0].clone() {
            UiInstruction::Compiled(ix) => ix,
            _ => panic!("expected a compiled instruction"),
        };

        let mut parser = ParserMessageExecuted::new(
            tx.signature.to_string(),
            compiled_ix,
            Pubkey::from_str("7RdSDLUUy37Wqc6s9ebgo52AwhGiw4XbJWZJgidQ1fJc").unwrap(),
        )
        .await
        .unwrap();
        assert!(parser.is_match().await.unwrap());
        let sig = tx.signature.clone().to_string();
        parser.parse().await.unwrap();
        let event = parser.event(Some(format!("{}-1", sig))).await.unwrap();
        match event {
            Event::MessageExecuted { ref common, .. } => {
                let expected_event = Event::MessageExecuted {
                    common: CommonEventFields {
                        r#type: "MESSAGE_EXECUTED".to_owned(),
                        event_id: sig.clone(),
                        meta: Some(MessageExecutedEventMetadata {
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
                            child_message_ids: None,
                            revert_reason: None,
                        }),
                    },
                    message_id: parser.parsed.as_ref().unwrap().message_id.clone(),
                    source_chain: parser.parsed.as_ref().unwrap().source_chain.clone(),
                    status: MessageExecutionStatus::SUCCESSFUL,
                    cost: Amount {
                        token_id: None,
                        amount: "0".to_string(),
                    },
                };
                assert_eq!(event, expected_event);
            }
            _ => panic!("Expected MessageExecuted event"),
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
        let mut parser = ParserMessageExecuted::new(
            tx.signature.to_string(),
            compiled_ix,
            Pubkey::from_str("7RdSDLUUy37Wqc6s9ebgo52AwhGiw4XbJWZJgidQ1fJc").unwrap(),
        )
        .await
        .unwrap();

        assert!(!parser.is_match().await.unwrap());
    }
}
