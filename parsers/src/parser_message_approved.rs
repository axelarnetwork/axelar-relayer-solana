use crate::common::check_discriminators_and_address;
use crate::error::TransactionParsingError;
use crate::message_matching_key::MessageMatchingKey;
use crate::parser::Parser;
use crate::redis::{CostCacheRef, TransactionType};
use anchor_lang::AnchorDeserialize;
use async_trait::async_trait;
use base64::prelude::BASE64_STANDARD;
use base64::Engine as _;
use bs58::encode;
use relayer_core::gmp_api::gmp_types::{
    Amount, CommonEventFields, Event, EventMetadata, GatewayV2Message, MessageApprovedEventMetadata,
};
use solana_axelar_gateway::events::MessageApprovedEvent;
use solana_sdk::pubkey::Pubkey;
use solana_transaction_status::UiCompiledInstruction;
use tracing::debug;
use uuid::Uuid;

pub struct ParserMessageApproved {
    signature: String,
    parsed: Option<MessageApprovedEvent>,
    instruction: UiCompiledInstruction,
    expected_contract_address: Pubkey,
    accounts: Vec<String>,
    timestamp: String,
    cost_cache: CostCacheRef,
}

impl ParserMessageApproved {
    pub(crate) fn new(
        signature: String,
        instruction: UiCompiledInstruction,
        expected_contract_address: Pubkey,
        accounts: Vec<String>,
        timestamp: String,
        cost_cache: CostCacheRef,
    ) -> Result<Self, TransactionParsingError> {
        Ok(Self {
            signature,
            parsed: None,
            instruction,
            expected_contract_address,
            accounts,
            timestamp,
            cost_cache,
        })
    }

    fn try_extract_with_config(
        instruction: &UiCompiledInstruction,
        expected_contract_address: Pubkey,
        accounts: &[String],
    ) -> Result<MessageApprovedEvent, TransactionParsingError> {
        let payload =
            check_discriminators_and_address(instruction, expected_contract_address, accounts)?;
        debug!(
            "MessageApprovedEvent payload length: {}, first 32 bytes: {:?}",
            payload.len(),
            payload.get(0..32.min(payload.len()))
        );
        match MessageApprovedEvent::deserialize(&mut payload.as_slice()) {
            Ok(event) => {
                debug!("Message Approved event={:?}", event);
                Ok(event)
            }
            Err(e) => {
                debug!(
                    "Failed to deserialize MessageApprovedEvent: {:?}, payload length: {}",
                    e,
                    payload.len()
                );
                Err(TransactionParsingError::InvalidInstructionData(format!(
                    "invalid message approved event: {:?}",
                    e
                )))
            }
        }
    }
}

#[async_trait]
impl Parser for ParserMessageApproved {
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
                event_id: Uuid::new_v4().to_string(),
                meta: Some(MessageApprovedEventMetadata {
                    common_meta: EventMetadata {
                        tx_id: Some(self.signature.clone()),
                        from_address: None,
                        finalized: None,
                        source_context: None,
                        timestamp: self.timestamp.clone(),
                    },
                    command_id: Some(encode(parsed.command_id).into_string()),
                }),
            },
            message: GatewayV2Message {
                message_id: parsed.cc_id.clone(),
                source_chain: parsed.source_chain.clone(),
                source_address: parsed.source_address.clone(),
                destination_address: parsed.destination_address.to_string(),
                payload_hash: BASE64_STANDARD.encode(parsed.payload_hash),
            },
            cost: Amount {
                token_id: None,
                amount: self
                    .cost_cache
                    .get_cost_by_message_id(parsed.cc_id.clone(), TransactionType::Approve)
                    .await
                    .map_err(|e| TransactionParsingError::CostCacheError(e.to_string()))?
                    .to_string(),
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
    use std::sync::Arc;

    use solana_transaction_status::UiInstruction;

    use super::*;
    use crate::parser_message_approved::ParserMessageApproved;
    use crate::redis::MockCostCacheTrait;
    use crate::test_utils::fixtures::transaction_fixtures;
    #[tokio::test]
    async fn test_parser() {
        let txs = transaction_fixtures();

        let tx = txs[1].clone();
        let compiled_ix: UiCompiledInstruction = match tx.ixs[0].instructions[1].clone() {
            UiInstruction::Compiled(ix) => ix,
            _ => panic!("expected a compiled instruction"),
        };

        let mut mock = MockCostCacheTrait::new();
        mock.expect_get_cost_by_message_id().returning(|_, _| Ok(0));

        let mut parser = ParserMessageApproved::new(
            tx.signature.to_string(),
            compiled_ix,
            Pubkey::from_str("gtwT4uGVTYSPnTGv6rSpMheyFyczUicxVWKqdtxNGw9").unwrap(),
            tx.account_keys,
            tx.timestamp.unwrap_or_default().to_rfc3339(),
            Arc::new(mock),
        )
        .unwrap();
        assert!(parser.parse().await.unwrap());
        let sig = tx.signature.clone().to_string();
        parser.parse().await.unwrap();
        let event = parser.event(Some(format!("{}-1", sig))).await.unwrap();
        match event {
            Event::MessageApproved {
                ref common,
                ref cost,
                ..
            } => {
                let expected_event = Event::MessageApproved {
                    common: CommonEventFields {
                        r#type: "MESSAGE_APPROVED".to_owned(),
                        event_id: common.event_id.clone(),
                        meta: Some(MessageApprovedEventMetadata {
                            common_meta: EventMetadata {
                                tx_id: Some(sig.clone()),
                                from_address: None,
                                finalized: None,
                                source_context: None,
                                timestamp: parser.timestamp.clone(),
                            },
                            command_id: Some(
                                encode(parser.parsed.as_ref().unwrap().command_id).into_string(),
                            ),
                        }),
                    },
                    message: GatewayV2Message {
                        message_id: parser.parsed.as_ref().unwrap().cc_id.clone(),
                        source_chain: parser.parsed.as_ref().unwrap().source_chain.clone(),
                        source_address: parser.parsed.as_ref().unwrap().source_address.clone(),
                        destination_address: parser
                            .parsed
                            .as_ref()
                            .unwrap()
                            .destination_address
                            .to_string(),
                        payload_hash: BASE64_STANDARD
                            .encode(parser.parsed.as_ref().unwrap().payload_hash),
                    },
                    cost: Amount {
                        token_id: None,
                        amount: cost.amount.clone(),
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
        let mut parser = ParserMessageApproved::new(
            tx.signature.to_string(),
            compiled_ix,
            Pubkey::from_str("gtwT4uGVTYSPnTGv6rSpMheyFyczUicxVWKqdtxNGw9").unwrap(),
            tx.account_keys,
            tx.timestamp.unwrap_or_default().to_rfc3339(),
            Arc::new(MockCostCacheTrait::new()),
        )
        .unwrap();

        assert!(parser.parse().await.is_err());
    }
}
