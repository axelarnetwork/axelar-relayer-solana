use crate::common::check_discriminators_and_address;
use crate::error::TransactionParsingError;
use crate::message_matching_key::MessageMatchingKey;
use crate::parser::Parser;
use crate::redis::{CostCacheRef, TransactionType};
use anchor_lang::AnchorDeserialize;
use async_trait::async_trait;
use bs58::encode;
use relayer_core::gmp_api::gmp_types::{
    Amount, CommonEventFields, Event, EventMetadata, MessageExecutedEventMetadata,
    MessageExecutionStatus,
};
use solana_axelar_gateway::events::MessageExecutedEvent;
use solana_sdk::pubkey::Pubkey;
use solana_transaction_status::UiCompiledInstruction;
use tracing::debug;
use uuid::Uuid;

pub struct ParserMessageExecuted {
    signature: String,
    parsed: Option<MessageExecutedEvent>,
    instruction: UiCompiledInstruction,
    expected_contract_address: Pubkey,
    accounts: Vec<String>,
    timestamp: String,
    cost_cache: CostCacheRef,
}

impl ParserMessageExecuted {
    pub(crate) async fn new(
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
    ) -> Result<MessageExecutedEvent, TransactionParsingError> {
        let payload =
            check_discriminators_and_address(instruction, expected_contract_address, accounts)?;
        match MessageExecutedEvent::deserialize(&mut payload.as_slice()) {
            Ok(event) => {
                debug!("Message Executed event={:?}", event);
                Ok(event)
            }
            Err(_) => Err(TransactionParsingError::InvalidInstructionData(
                "invalid message executed event".to_string(),
            )),
        }
    }
}

#[async_trait]
impl Parser for ParserMessageExecuted {
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
                event_id: Uuid::new_v4().to_string(),
                meta: Some(MessageExecutedEventMetadata {
                    common_meta: EventMetadata {
                        tx_id: Some(self.signature.clone()),
                        from_address: Some(parsed.source_address.clone()),
                        finalized: None,
                        source_context: None,
                        timestamp: self.timestamp.clone(),
                    },
                    command_id: Some(encode(parsed.command_id).into_string()),
                    child_message_ids: None,
                    revert_reason: None,
                }),
            },
            message_id: parsed.cc_id.clone(),
            source_chain: parsed.source_chain,
            status: MessageExecutionStatus::SUCCESSFUL,
            cost: Amount {
                token_id: None,
                amount: self
                    .cost_cache
                    .get_cost_by_message_id(parsed.cc_id.clone(), TransactionType::Execute)
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
    use crate::parser_message_executed::ParserMessageExecuted;
    use crate::redis::MockCostCacheTrait;
    use crate::test_utils::fixtures::transaction_fixtures;
    #[tokio::test]
    async fn test_parser() {
        let txs = transaction_fixtures();

        let tx = txs[3].clone();
        let compiled_ix: UiCompiledInstruction = match tx.ixs[0].instructions[0].clone() {
            UiInstruction::Compiled(ix) => ix,
            _ => panic!("expected a compiled instruction"),
        };

        let mut mock = MockCostCacheTrait::new();
        mock.expect_get_cost_by_message_id().returning(|_, _| Ok(0));

        let mut parser = ParserMessageExecuted::new(
            tx.signature.to_string(),
            compiled_ix,
            Pubkey::from_str("gtwT4uGVTYSPnTGv6rSpMheyFyczUicxVWKqdtxNGw9").unwrap(),
            tx.account_keys,
            tx.timestamp.unwrap_or_default().to_rfc3339(),
            Arc::new(mock),
        )
        .await
        .unwrap();
        assert!(parser.parse().await.unwrap());
        let sig = tx.signature.clone().to_string();
        parser.parse().await.unwrap();
        let event = parser.event(Some(format!("{}-1", sig))).await.unwrap();
        match event {
            Event::MessageExecuted {
                ref common,
                ref cost,
                ..
            } => {
                let expected_event = Event::MessageExecuted {
                    common: CommonEventFields {
                        r#type: "MESSAGE_EXECUTED".to_owned(),
                        event_id: common.event_id.clone(),
                        meta: Some(MessageExecutedEventMetadata {
                            common_meta: EventMetadata {
                                tx_id: Some(sig.clone()),
                                from_address: Some(
                                    parser.parsed.as_ref().unwrap().source_address.clone(),
                                ),
                                finalized: None,
                                source_context: None,
                                timestamp: parser.timestamp.clone(),
                            },
                            command_id: Some(
                                encode(parser.parsed.as_ref().unwrap().command_id).into_string(),
                            ),
                            child_message_ids: None,
                            revert_reason: None,
                        }),
                    },
                    message_id: parser.parsed.as_ref().unwrap().cc_id.clone(),
                    source_chain: parser.parsed.as_ref().unwrap().source_chain.clone(),
                    status: MessageExecutionStatus::SUCCESSFUL,
                    cost: Amount {
                        token_id: None,
                        amount: cost.amount.clone(),
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
            Pubkey::from_str("gtwT4uGVTYSPnTGv6rSpMheyFyczUicxVWKqdtxNGw9").unwrap(),
            tx.account_keys,
            tx.timestamp.unwrap_or_default().to_rfc3339(),
            Arc::new(MockCostCacheTrait::new()),
        )
        .await
        .unwrap();

        assert!(parser.parse().await.is_err());
    }
}
