use super::message_matching_key::MessageMatchingKey;

use crate::instruction_index::InstructionIndex;
use crate::parser_call_contract::ParserCallContract;
use crate::parser_its_interchain_token_deployment_started::ParserInterchainTokenDeploymentStarted;
use crate::parser_its_interchain_transfer::ParserInterchainTransfer;
use crate::parser_its_link_token_started::ParserLinkTokenStarted;
use crate::parser_its_token_metadata_registered::ParserTokenMetadataRegistered;
use crate::parser_message_approved::ParserMessageApproved;
use crate::parser_message_executed::ParserMessageExecuted;
use crate::parser_native_gas_added::ParserNativeGasAdded;
use crate::parser_native_gas_paid::ParserNativeGasPaid;
use crate::parser_native_gas_refunded::ParserNativeGasRefunded;
use crate::parser_signers_rotated::ParserSignersRotated;
use crate::types::SolanaTransaction;
use crate::{error::TransactionParsingError, redis::CostCacheRef};
use anchor_lang::Discriminator;
use async_trait::async_trait;
use relayer_core::gmp_api::gmp_types::Event;
use relayer_core::utils::ThreadSafe;
use solana_axelar_gas_service::events::{GasAddedEvent, GasPaidEvent, GasRefundedEvent};
use solana_axelar_gateway::events::{
    CallContractEvent, MessageApprovedEvent, MessageExecutedEvent, VerifierSetRotatedEvent,
};
use solana_axelar_its::events::{
    InterchainTokenDeploymentStarted, InterchainTransferSent, LinkTokenStarted,
    TokenMetadataRegistered,
};
use solana_sdk::pubkey::Pubkey;
use solana_transaction_status::UiInstruction;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info, warn};

pub const CPI_EVENT_DISC: &[u8] = &[228, 69, 165, 46, 81, 203, 154, 29];

#[async_trait]
pub trait Parser: ThreadSafe {
    async fn parse(&mut self) -> Result<bool, crate::error::TransactionParsingError>;
    async fn key(&self) -> Result<MessageMatchingKey, crate::error::TransactionParsingError>;
    async fn event(
        &self,
        message_id: Option<String>,
    ) -> Result<Event, crate::error::TransactionParsingError>;
    async fn message_id(&self) -> Result<Option<String>, crate::error::TransactionParsingError>;
}

#[derive(Clone)]
pub struct TransactionParser {
    chain_name: String,
    gas_service_address: Pubkey,
    gateway_address: Pubkey,
    its_address: Pubkey,
    cost_cache: CostCacheRef,
}

#[cfg_attr(any(test, feature = "mocks"), mockall::automock)]
#[async_trait]
pub trait TransactionParserTrait: ThreadSafe {
    async fn parse_transaction(
        &self,
        transaction: String,
    ) -> Result<Vec<Event>, TransactionParsingError>;
}

#[async_trait]
impl TransactionParserTrait for TransactionParser {
    async fn parse_transaction(
        &self,
        transaction: String,
    ) -> Result<Vec<Event>, TransactionParsingError> {
        let mut events: Vec<Event> = Vec::new();
        let mut parsers: Vec<Box<dyn Parser + Send + Sync>> = Vec::new();
        let mut its_parsers: Vec<Box<dyn Parser + Send + Sync>> = Vec::new();
        let mut call_contract: Vec<Box<dyn Parser + Send + Sync>> = Vec::new();
        let mut gas_credit_map: HashMap<MessageMatchingKey, Box<dyn Parser + Send + Sync>> =
            HashMap::new();

        let transaction = serde_json::from_str::<SolanaTransaction>(&transaction)
            .map_err(|e| TransactionParsingError::InvalidTransaction(e.to_string()))?;

        self.create_parsers(
            transaction.clone(),
            &mut parsers,
            &mut its_parsers,
            &mut call_contract,
            &mut gas_credit_map,
            self.chain_name.clone(),
        )
        .await?;

        info!(
            "Parsing results: transaction signature={} parsers={}, call_contract={}, gas_credit_map={}",
            transaction.signature,
            parsers.len(),
            call_contract.len(),
            gas_credit_map.len()
        );

        if (parsers.len() + its_parsers.len() + call_contract.len() + gas_credit_map.len()) == 0 {
            debug!(
                "Transaction did not produce any parsers: transaction signature={}",
                transaction.signature
            );
        }

        for cc in call_contract.iter() {
            let cc_key = cc.key().await?;
            events.push(cc.event(None).await?);
            if let Some(parser) = gas_credit_map.remove(&cc_key) {
                let message_id = cc.message_id().await?.ok_or_else(|| {
                    TransactionParsingError::Message("Missing message_id".to_string())
                })?;

                let event = parser.event(Some(message_id)).await?;
                events.push(event);
            }
        }

        for (i, its_parser) in its_parsers.iter().enumerate() {
            match call_contract.get(i) {
                Some(contract_parser) => {
                    let message_id = contract_parser.message_id().await?;
                    events.push(its_parser.event(message_id).await?);
                }
                None => {
                    return Err(TransactionParsingError::ITSWithoutPair(format!(
                        "No matching call_contract for ITS index {i}"
                    )));
                }
            }
        }

        for parser in parsers {
            let event = parser.event(None).await?;
            events.push(event);
        }

        Ok(events)
    }
}

impl TransactionParser {
    pub fn new(
        chain_name: String,
        gas_service_address: Pubkey,
        gateway_address: Pubkey,
        its_address: Pubkey,
        cost_cache: CostCacheRef,
    ) -> Self {
        Self {
            chain_name,
            gas_service_address,
            gateway_address,
            its_address,
            cost_cache,
        }
    }

    async fn create_parsers(
        &self,
        transaction: SolanaTransaction,
        parsers: &mut Vec<Box<dyn Parser + Send + Sync>>,
        its_parsers: &mut Vec<Box<dyn Parser + Send + Sync>>,
        call_contract: &mut Vec<Box<dyn Parser + Send + Sync>>,
        gas_credit_map: &mut HashMap<MessageMatchingKey, Box<dyn Parser + Send + Sync>>,
        chain_name: String,
    ) -> Result<(), TransactionParsingError> {
        for group in transaction.ixs.iter() {
            for (inner_index, inst) in group.instructions.iter().enumerate() {
                if let UiInstruction::Compiled(ci) = inst {
                    let bytes = bs58::decode(&ci.data).into_vec().map_err(|e| {
                        warn!("invalid instruction data: {:?}", e);
                        TransactionParsingError::InvalidAccountAddress(e.to_string())
                    })?;

                    if bytes.get(0..8) != Some(CPI_EVENT_DISC) {
                        debug!(
                            "expected event cpi discriminator, transaction signature={}",
                            transaction.signature
                        );
                        continue;
                    }

                    let index = InstructionIndex::new(
                        transaction.signature.to_string(),
                        group.index.checked_add(1).ok_or(
                            TransactionParsingError::IndexOverflow(
                                "Outer index overflow".to_string(),
                            ),
                        )?,
                        inner_index
                            .checked_add(1)
                            .ok_or(TransactionParsingError::IndexOverflow(
                                "Inner index overflow".to_string(),
                            ))? as u8,
                    );

                    let event_type_discriminator = match bytes.get(8..16) {
                        Some(event_type_discriminator) => event_type_discriminator,
                        None => {
                            warn!(
                                "event type discriminator is out of bounds, transaction signature={}",
                                transaction.signature
                            );
                            continue;
                        }
                    };

                    match event_type_discriminator {
                        GasPaidEvent::DISCRIMINATOR => {
                            let mut parser = ParserNativeGasPaid::new(
                                transaction.signature.to_string(),
                                ci.clone(),
                                self.gas_service_address,
                                transaction.account_keys.clone(),
                                transaction.timestamp.unwrap_or_default().to_rfc3339(),
                            )?;
                            info!(
                                "ParserNativeGasPaid matched, transaction signature={}",
                                transaction.signature
                            );
                            parser.parse().await?;
                            let key = parser.key().await?;
                            gas_credit_map.insert(key, Box::new(parser));
                        }
                        GasAddedEvent::DISCRIMINATOR => {
                            let mut parser = ParserNativeGasAdded::new(
                                transaction.signature.to_string(),
                                ci.clone(),
                                self.gas_service_address,
                                transaction.account_keys.clone(),
                                transaction.timestamp.unwrap_or_default().to_rfc3339(),
                            )?;
                            info!(
                                "ParserNativeGasAdded matched, transaction signature={}",
                                transaction.signature
                            );
                            parser.parse().await?;
                            parsers.push(Box::new(parser));
                        }
                        GasRefundedEvent::DISCRIMINATOR => {
                            let mut parser = ParserNativeGasRefunded::new(
                                transaction.signature.to_string(),
                                ci.clone(),
                                self.gas_service_address,
                                transaction.cost_units,
                                transaction.account_keys.clone(),
                                transaction.timestamp.unwrap_or_default().to_rfc3339(),
                            )?;
                            info!(
                                "ParserNativeGasRefunded matched, transaction signature={}",
                                transaction.signature
                            );
                            parser.parse().await?;
                            parsers.push(Box::new(parser));
                        }
                        CallContractEvent::DISCRIMINATOR => {
                            let mut parser = ParserCallContract::new(
                                transaction.signature.to_string(),
                                ci.clone(),
                                transaction.account_keys.clone(),
                                chain_name.clone(),
                                index,
                                self.gateway_address,
                                transaction.timestamp.unwrap_or_default().to_rfc3339(),
                            )?;
                            info!(
                                "ParserCallContract matched, transaction signature={}",
                                transaction.signature
                            );
                            parser.parse().await?;
                            call_contract.push(Box::new(parser));
                        }
                        MessageApprovedEvent::DISCRIMINATOR => {
                            let mut parser = ParserMessageApproved::new(
                                transaction.signature.to_string(),
                                ci.clone(),
                                self.gateway_address,
                                transaction.account_keys.clone(),
                                transaction.timestamp.unwrap_or_default().to_rfc3339(),
                                Arc::clone(&self.cost_cache),
                            )?;
                            info!(
                                "ParserMessageApproved matched, transaction signature={}",
                                transaction.signature
                            );
                            parser.parse().await?;
                            parsers.push(Box::new(parser));
                        }
                        MessageExecutedEvent::DISCRIMINATOR => {
                            let mut parser = ParserMessageExecuted::new(
                                transaction.signature.to_string(),
                                ci.clone(),
                                self.gateway_address,
                                transaction.account_keys.clone(),
                                transaction.timestamp.unwrap_or_default().to_rfc3339(),
                                Arc::clone(&self.cost_cache),
                            )?;
                            info!(
                                "ParserMessageExecuted matched, transaction signature={}",
                                transaction.signature
                            );
                            parser.parse().await?;
                            parsers.push(Box::new(parser));
                        }
                        VerifierSetRotatedEvent::DISCRIMINATOR => {
                            let mut parser = ParserSignersRotated::new(
                                transaction.signature.to_string(),
                                ci.clone(),
                                index,
                                self.gateway_address,
                                transaction.account_keys.clone(),
                                transaction.timestamp.unwrap_or_default().to_rfc3339(),
                            )?;
                            info!(
                                "ParserSignersRotated matched, transaction signature={}",
                                transaction.signature
                            );
                            parser.parse().await?;
                            parsers.push(Box::new(parser));
                        }
                        InterchainTransferSent::DISCRIMINATOR => {
                            let mut parser = ParserInterchainTransfer::new(
                                transaction.signature.to_string(),
                                ci.clone(),
                                self.its_address,
                                transaction.account_keys.clone(),
                                transaction.timestamp.unwrap_or_default().to_rfc3339(),
                            )?;
                            info!(
                                "ParserInterchainTransfer matched, transaction signature={}",
                                transaction.signature
                            );
                            parser.parse().await?;
                            its_parsers.push(Box::new(parser));
                        }
                        InterchainTokenDeploymentStarted::DISCRIMINATOR => {
                            let mut parser = ParserInterchainTokenDeploymentStarted::new(
                                transaction.signature.to_string(),
                                ci.clone(),
                                self.its_address,
                                transaction.account_keys.clone(),
                                transaction.timestamp.unwrap_or_default().to_rfc3339(),
                            )?;
                            info!(
                                    "ParserInterchainTokenDeploymentStarted matched, transaction signature={}",
                                    transaction.signature
                                );
                            parser.parse().await?;
                            its_parsers.push(Box::new(parser));
                        }
                        LinkTokenStarted::DISCRIMINATOR => {
                            let mut parser = ParserLinkTokenStarted::new(
                                transaction.signature.to_string(),
                                ci.clone(),
                                self.its_address,
                                transaction.account_keys.clone(),
                                transaction.timestamp.unwrap_or_default().to_rfc3339(),
                            )?;
                            info!(
                                "ParserLinkTokenStarted matched, transaction signature={}",
                                transaction.signature
                            );
                            parser.parse().await?;
                            its_parsers.push(Box::new(parser));
                        }
                        TokenMetadataRegistered::DISCRIMINATOR => {
                            let mut parser = ParserTokenMetadataRegistered::new(
                                transaction.signature.to_string(),
                                ci.clone(),
                                self.its_address,
                                transaction.account_keys.clone(),
                                transaction.timestamp.unwrap_or_default().to_rfc3339(),
                            )?;
                            info!(
                                "ParserTokenMetadataRegistered matched, transaction signature={}",
                                transaction.signature
                            );
                            parser.parse().await?;
                            its_parsers.push(Box::new(parser));
                        }
                        _ => {
                            debug!(
                                "Unknown event type discriminator: {:?}",
                                event_type_discriminator
                            );
                            continue;
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use std::sync::Arc;

    use super::*;
    use crate::redis::MockCostCacheTrait;
    use crate::test_utils::fixtures::transaction_fixtures;

    #[tokio::test]
    async fn test_parser_converted_and_message_id_set() {
        let txs = transaction_fixtures();
        let mock = Arc::new(MockCostCacheTrait::new());
        let parser = TransactionParser::new(
            "solana".to_string(),
            Pubkey::from_str("CJ9f8WFdm3q38pmg426xQf7uum7RqvrmS9R58usHwNX7").unwrap(),
            Pubkey::from_str("gtwT4uGVTYSPnTGv6rSpMheyFyczUicxVWKqdtxNGw9").unwrap(),
            Pubkey::from_str("gtwT4uGVTYSPnTGv6rSpMheyFyczUicxVWKqdtxNGw9").unwrap(),
            mock,
        );
        let events = parser
            .parse_transaction(serde_json::to_string(&txs[0]).unwrap())
            .await
            .unwrap();
        assert_eq!(events.len(), 2);

        let sig = txs[0].signature.clone().to_string();

        match events[0].clone() {
            Event::Call {
                message,
                destination_chain,
                ..
            } => {
                assert_eq!(destination_chain, "solana-5");
                assert_eq!(message.message_id, format!("{}-2.1", sig));
            }
            _ => panic!("Expected CallContract event"),
        }

        match events[1].clone() {
            Event::GasCredit {
                message_id,
                payment,
                ..
            } => {
                assert_eq!(message_id, format!("{}-2.1", sig));
                assert_eq!(payment.amount, "1000");
            }
            _ => panic!("Expected GasCredit event"),
        }
    }

    #[tokio::test]
    async fn test_message_executed() {
        let txs = transaction_fixtures();
        let cost_units = txs[3].cost_units;

        let mut mock = MockCostCacheTrait::new();
        mock.expect_get_cost_by_message_id()
            .returning(move |_, _| Ok(cost_units));

        let parser = TransactionParser::new(
            "solana".to_string(),
            Pubkey::from_str("CJ9f8WFdm3q38pmg426xQf7uum7RqvrmS9R58usHwNX7").unwrap(),
            Pubkey::from_str("gtwT4uGVTYSPnTGv6rSpMheyFyczUicxVWKqdtxNGw9").unwrap(),
            Pubkey::from_str("gtwT4uGVTYSPnTGv6rSpMheyFyczUicxVWKqdtxNGw9").unwrap(),
            Arc::new(mock),
        );

        let events = parser
            .parse_transaction(serde_json::to_string(&txs[3]).unwrap())
            .await
            .unwrap();

        assert_eq!(events.len(), 1);

        match events[0].clone() {
            Event::MessageExecuted { cost, .. } => {
                assert_eq!(cost.amount, txs[3].cost_units.to_string());
                assert!(cost.token_id.is_none());
            }
            _ => panic!("Expected MessageExecuted event"),
        }
    }

    #[tokio::test]
    async fn test_message_approved() {
        let txs = transaction_fixtures();
        let cost_units = txs[1].cost_units;

        let mut mock = MockCostCacheTrait::new();
        mock.expect_get_cost_by_message_id()
            .returning(move |_, _| Ok(cost_units));

        let parser = TransactionParser::new(
            "solana".to_string(),
            Pubkey::from_str("CJ9f8WFdm3q38pmg426xQf7uum7RqvrmS9R58usHwNX7").unwrap(),
            Pubkey::from_str("gtwT4uGVTYSPnTGv6rSpMheyFyczUicxVWKqdtxNGw9").unwrap(),
            Pubkey::from_str("gtwT4uGVTYSPnTGv6rSpMheyFyczUicxVWKqdtxNGw9").unwrap(),
            Arc::new(mock),
        );

        let events = parser
            .parse_transaction(serde_json::to_string(&txs[1]).unwrap())
            .await
            .unwrap();
        assert_eq!(events.len(), 1);

        match events[0].clone() {
            Event::MessageApproved { cost, .. } => {
                assert_eq!(cost.amount, txs[1].cost_units.to_string());
            }
            _ => panic!("Expected MessageApproved event"),
        }
    }

    #[tokio::test]
    async fn test_gas_refunded() {
        let txs = transaction_fixtures();
        let parser = TransactionParser::new(
            "solana".to_string(),
            Pubkey::from_str("CJ9f8WFdm3q38pmg426xQf7uum7RqvrmS9R58usHwNX7").unwrap(),
            Pubkey::from_str("gtwT4uGVTYSPnTGv6rSpMheyFyczUicxVWKqdtxNGw9").unwrap(),
            Pubkey::from_str("gtwT4uGVTYSPnTGv6rSpMheyFyczUicxVWKqdtxNGw9").unwrap(),
            Arc::new(MockCostCacheTrait::new()),
        );
        let events = parser
            .parse_transaction(serde_json::to_string(&txs[2]).unwrap())
            .await
            .unwrap();
        assert_eq!(events.len(), 1);

        match events[0].clone() {
            Event::GasRefunded { cost, .. } => {
                assert_eq!(cost.amount, txs[2].cost_units.to_string());
                assert!(cost.token_id.is_none());
            }
            _ => panic!("Expected GasRefunded event"),
        }
    }

    #[tokio::test]
    async fn test_gas_added() {
        let txs = transaction_fixtures();
        let parser = TransactionParser::new(
            "solana".to_string(),
            Pubkey::from_str("CJ9f8WFdm3q38pmg426xQf7uum7RqvrmS9R58usHwNX7").unwrap(),
            Pubkey::from_str("gtwT4uGVTYSPnTGv6rSpMheyFyczUicxVWKqdtxNGw9").unwrap(),
            Pubkey::from_str("gtwT4uGVTYSPnTGv6rSpMheyFyczUicxVWKqdtxNGw9").unwrap(),
            Arc::new(MockCostCacheTrait::new()),
        );
        let events = parser
            .parse_transaction(serde_json::to_string(&txs[4]).unwrap())
            .await
            .unwrap();
        assert_eq!(events.len(), 1);

        match events[0].clone() {
            Event::GasCredit { .. } => {}
            _ => panic!("Expected GasCredit event"),
        }
    }

    #[tokio::test]
    async fn test_its_interchain_transfer() {
        let txs = transaction_fixtures();
        let parser = TransactionParser::new(
            "solana".to_string(),
            Pubkey::from_str("CJ9f8WFdm3q38pmg426xQf7uum7RqvrmS9R58usHwNX7").unwrap(),
            Pubkey::from_str("gtwT4uGVTYSPnTGv6rSpMheyFyczUicxVWKqdtxNGw9").unwrap(),
            Pubkey::from_str("gtwT4uGVTYSPnTGv6rSpMheyFyczUicxVWKqdtxNGw9").unwrap(),
            Arc::new(MockCostCacheTrait::new()),
        );
        let events = parser
            .parse_transaction(serde_json::to_string(&txs[7]).unwrap())
            .await
            .unwrap();
        assert_eq!(events.len(), 2);

        match events[1].clone() {
            Event::ITSInterchainTransfer { message_id, .. } => match events[0].clone() {
                Event::Call { message, .. } => {
                    assert_eq!(message_id, message.message_id);
                }
                _ => panic!("Expected Call event"),
            },
            _ => panic!("Expected ITSInterchainTransfer event"),
        }
    }

    #[tokio::test]
    async fn test_its_link_token_started() {
        let txs = transaction_fixtures();
        let parser = TransactionParser::new(
            "solana".to_string(),
            Pubkey::from_str("CJ9f8WFdm3q38pmg426xQf7uum7RqvrmS9R58usHwNX7").unwrap(),
            Pubkey::from_str("gtwT4uGVTYSPnTGv6rSpMheyFyczUicxVWKqdtxNGw9").unwrap(),
            Pubkey::from_str("gtwT4uGVTYSPnTGv6rSpMheyFyczUicxVWKqdtxNGw9").unwrap(),
            Arc::new(MockCostCacheTrait::new()),
        );
        let events = parser
            .parse_transaction(serde_json::to_string(&txs[8]).unwrap())
            .await
            .unwrap();
        assert_eq!(events.len(), 2);

        match events[0].clone() {
            Event::Call { message, .. } => match events[1].clone() {
                Event::ITSLinkTokenStarted { message_id, .. } => {
                    assert_eq!(message_id, message.message_id);
                }
                _ => panic!("Expected ITSLinkTokenStarted event"),
            },
            _ => panic!("Expected Call event"),
        }
    }

    #[tokio::test]
    async fn test_its_interchain_token_deployment_started() {
        let txs = transaction_fixtures();
        let parser = TransactionParser::new(
            "solana".to_string(),
            Pubkey::from_str("CJ9f8WFdm3q38pmg426xQf7uum7RqvrmS9R58usHwNX7").unwrap(),
            Pubkey::from_str("gtwT4uGVTYSPnTGv6rSpMheyFyczUicxVWKqdtxNGw9").unwrap(),
            Pubkey::from_str("gtwT4uGVTYSPnTGv6rSpMheyFyczUicxVWKqdtxNGw9").unwrap(),
            Arc::new(MockCostCacheTrait::new()),
        );
        let events = parser
            .parse_transaction(serde_json::to_string(&txs[9]).unwrap())
            .await
            .unwrap();
        assert_eq!(events.len(), 2);

        match events[0].clone() {
            Event::Call { message, .. } => match events[1].clone() {
                Event::ITSInterchainTokenDeploymentStarted { message_id, .. } => {
                    assert_eq!(message_id, message.message_id);
                }
                _ => panic!("Expected ITSInterchainTokenDeploymentStarted event"),
            },
            _ => panic!("Expected Call event"),
        }
    }

    #[tokio::test]
    async fn test_its_token_metadata_registered() {
        let txs = transaction_fixtures();
        let parser = TransactionParser::new(
            "solana".to_string(),
            Pubkey::from_str("CJ9f8WFdm3q38pmg426xQf7uum7RqvrmS9R58usHwNX7").unwrap(),
            Pubkey::from_str("gtwT4uGVTYSPnTGv6rSpMheyFyczUicxVWKqdtxNGw9").unwrap(),
            Pubkey::from_str("gtwT4uGVTYSPnTGv6rSpMheyFyczUicxVWKqdtxNGw9").unwrap(),
            Arc::new(MockCostCacheTrait::new()),
        );
        let events = parser
            .parse_transaction(serde_json::to_string(&txs[10]).unwrap())
            .await
            .unwrap();
        assert_eq!(events.len(), 2);

        match events[0].clone() {
            Event::Call { message, .. } => match events[1].clone() {
                Event::ITSTokenMetadataRegistered { message_id, .. } => {
                    assert_eq!(message_id, message.message_id);
                }
                _ => panic!("Expected ITSTokenMetadataRegistered event"),
            },
            _ => panic!("Expected Call event"),
        }
    }
}
