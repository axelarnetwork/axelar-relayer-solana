use super::message_matching_key::MessageMatchingKey;
use crate::transaction_parser::parser_call_contract::ParserCallContract;
use crate::transaction_parser::parser_message_approved::ParserMessageApproved;
use crate::transaction_parser::parser_message_executed::ParserMessageExecuted;
use crate::transaction_parser::parser_native_gas_added::ParserNativeGasAdded;
use crate::transaction_parser::parser_native_gas_paid::ParserNativeGasPaid;
use crate::transaction_parser::parser_native_gas_refunded::ParserNativeGasRefunded;
use crate::transaction_parser::parser_signers_rotated::ParserLogSignersRotated;
use crate::types::SolanaTransaction;
use crate::{
    error::TransactionParsingError,
    transaction_parser::parser_execute_insufficient_gas::ParserExecuteInsufficientGas,
};
use async_trait::async_trait;
use relayer_core::gmp_api::gmp_types::Event;
use solana_transaction_status::UiInstruction;
use std::collections::HashMap;
use tracing::{info, warn};

#[derive(Clone, Copy, Debug)]
pub struct ParserConfig {
    pub event_cpi_discriminator: [u8; 8],
    pub event_type_discriminator: [u8; 8],
}

#[async_trait]
pub trait Parser {
    async fn parse(&mut self) -> Result<bool, crate::error::TransactionParsingError>;
    async fn is_match(&self) -> Result<bool, crate::error::TransactionParsingError>;
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
}

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait TransactionParserTrait: Send + Sync {
    async fn parse_transaction(
        &self,
        transaction: SolanaTransaction,
    ) -> Result<Vec<Event>, TransactionParsingError>;
}

#[async_trait]
impl TransactionParserTrait for TransactionParser {
    async fn parse_transaction(
        &self,
        transaction: SolanaTransaction,
    ) -> Result<Vec<Event>, TransactionParsingError> {
        let mut events: Vec<Event> = Vec::new();
        let mut parsers: Vec<Box<dyn Parser + Send + Sync>> = Vec::new();
        let mut call_contract: Vec<Box<dyn Parser + Send + Sync>> = Vec::new();
        let mut gas_credit_map: HashMap<MessageMatchingKey, Box<dyn Parser + Send + Sync>> =
            HashMap::new();

        let transaction_id = transaction.signature;
        let (message_approved_count, message_executed_count) = self
            .create_parsers(
                transaction.clone(),
                &mut parsers,
                &mut call_contract,
                &mut gas_credit_map,
                self.chain_name.clone(),
            )
            .await?;

        info!(
            "Parsing results: transaction_id={} parsers={}, call_contract={}, gas_credit_map={}",
            transaction_id,
            parsers.len(),
            call_contract.len(),
            gas_credit_map.len()
        );

        if (parsers.len() + call_contract.len() + gas_credit_map.len()) == 0 {
            warn!(
                "Transaction did not produce any parsers: transaction_id={}",
                transaction_id
            );
        }

        for cc in call_contract {
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

        for parser in parsers {
            let event = parser.event(None).await?;
            events.push(event);
        }

        let mut parsed_events: Vec<Event> = Vec::new();

        for event in events {
            let event = match event {
                Event::MessageApproved {
                    common,
                    message,
                    mut cost,
                } => {
                    let cost_units = transaction.clone().cost_units;
                    if cost_units == 0 {
                        return Err(TransactionParsingError::Generic(
                            "Cost units for approved not found".to_string(),
                        ));
                    }
                    cost.amount = (cost_units.checked_div(message_approved_count))
                        .unwrap_or(0)
                        .to_string();
                    Event::MessageApproved {
                        common,
                        message,
                        cost,
                    }
                }
                Event::MessageExecuted {
                    common,
                    message_id,
                    source_chain,
                    status,
                    mut cost,
                } => {
                    let cost_units = transaction.clone().cost_units;
                    if cost_units == 0 {
                        return Err(TransactionParsingError::Generic(
                            "Cost units for executed not found".to_string(),
                        ));
                    }
                    cost.amount = (cost_units.checked_div(message_executed_count))
                        .unwrap_or(0)
                        .to_string();
                    Event::MessageExecuted {
                        common,
                        message_id,
                        source_chain,
                        status,
                        cost,
                    }
                }
                other => other,
            };
            parsed_events.push(event);
        }

        Ok(parsed_events)
    }
}

impl TransactionParser {
    pub fn new(chain_name: String) -> Self {
        Self { chain_name }
    }

    async fn create_parsers(
        &self,
        transaction: SolanaTransaction,
        parsers: &mut Vec<Box<dyn Parser + Send + Sync>>,
        call_contract: &mut Vec<Box<dyn Parser + Send + Sync>>,
        gas_credit_map: &mut HashMap<MessageMatchingKey, Box<dyn Parser + Send + Sync>>,
        chain_name: String,
    ) -> Result<(u64, u64), TransactionParsingError> {
        let mut index = 0;
        let mut message_approved_count = 0u64;
        let mut message_executed_count = 0u64;

        for group in transaction.ixs.iter() {
            for inst in group.instructions.iter() {
                if let UiInstruction::Compiled(ci) = inst {
                    // Should we check from which account the event was emitted?
                    let mut parser =
                        ParserNativeGasPaid::new(transaction.signature.to_string(), ci.clone())
                            .await?;
                    if parser.is_match().await? {
                        info!(
                            "ParserNativeGasPaid matched, transaction_id={}",
                            transaction.signature
                        );
                        parser.parse().await?;
                        let key = parser.key().await?;
                        gas_credit_map.insert(key, Box::new(parser));
                    }

                    let mut parser =
                        ParserNativeGasAdded::new(transaction.signature.to_string(), ci.clone())
                            .await?;
                    if parser.is_match().await? {
                        info!(
                            "ParserNativeGasAdded matched, transaction_id={}",
                            transaction.signature
                        );
                        parser.parse().await?;
                        parsers.push(Box::new(parser));
                    }
                    let mut parser = ParserNativeGasRefunded::new(
                        transaction.signature.to_string(),
                        ci.clone(),
                        transaction.cost_units,
                    )
                    .await?;
                    if parser.is_match().await? {
                        info!(
                            "ParserNativeGasRefunded matched, transaction_id={}",
                            transaction.signature
                        );
                        parser.parse().await?;
                        parsers.push(Box::new(parser));
                    }
                    let mut parser = ParserCallContract::new(
                        transaction.signature.to_string(),
                        ci.clone(),
                        chain_name.clone(),
                        index,
                    )
                    .await?;
                    if parser.is_match().await? {
                        info!(
                            "ParserCallContract matched, transaction_id={}",
                            transaction.signature
                        );
                        parser.parse().await?;
                        call_contract.push(Box::new(parser));
                    }
                    let mut parser =
                        ParserMessageApproved::new(transaction.signature.to_string(), ci.clone())
                            .await?;
                    if parser.is_match().await? {
                        info!(
                            "ParserMessageApproved matched, transaction_id={}",
                            transaction.signature
                        );
                        parser.parse().await?;
                        parsers.push(Box::new(parser));
                        message_approved_count += 1;
                    }
                    let mut parser =
                        ParserMessageExecuted::new(transaction.signature.to_string(), ci.clone())
                            .await?;
                    if parser.is_match().await? {
                        info!(
                            "ParserMessageExecuted matched, transaction_id={}",
                            transaction.signature
                        );
                        parser.parse().await?;
                        parsers.push(Box::new(parser));
                        message_executed_count += 1;
                    }
                    let mut parser = ParserExecuteInsufficientGas::new(
                        transaction.signature.to_string(),
                        ci.clone(),
                    )
                    .await?;
                    if parser.is_match().await? {
                        info!(
                            "ParserExecuteInsufficientGas matched, transaction_id={}",
                            transaction.signature
                        );
                        parser.parse().await?;
                        parsers.push(Box::new(parser));
                    }
                    let mut parser = ParserLogSignersRotated::new(
                        transaction.signature.to_string(),
                        ci.clone(),
                        index,
                    )
                    .await?;
                    if parser.is_match().await? {
                        info!(
                            "ParserLogSignersRotated matched, transaction_id={}",
                            transaction.signature
                        );
                        parser.parse().await?;
                        parsers.push(Box::new(parser));
                    }
                    index += 1; // index is the position of the instruction in the transaction including the inner ones
                }
            }
        }

        Ok((message_approved_count, message_executed_count))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::fixtures::transaction_fixtures;

    #[tokio::test]
    async fn test_parser_converted_and_message_id_set() {
        let txs = transaction_fixtures();
        let parser = TransactionParser::new("solana".to_string());
        let events = parser.parse_transaction(txs[0].clone()).await.unwrap();
        assert_eq!(events.len(), 2);

        let sig = txs[0].signature.clone().to_string();

        match events[0].clone() {
            Event::Call {
                message,
                destination_chain,
                ..
            } => {
                assert_eq!(destination_chain, "ethereum");
                assert_eq!(message.message_id, format!("{}-1", sig));
            }
            _ => panic!("Expected CallContract event"),
        }

        match events[1].clone() {
            Event::GasCredit {
                message_id,
                payment,
                ..
            } => {
                assert_eq!(message_id, format!("{}-1", sig));
                assert_eq!(payment.amount, "1000");
            }
            _ => panic!("Expected GasCredit event"),
        }
    }

    #[tokio::test]
    async fn test_message_executed() {
        let txs = transaction_fixtures();
        let parser = TransactionParser::new("solana".to_string());
        let events = parser.parse_transaction(txs[3].clone()).await.unwrap();

        assert_eq!(events.len(), 1);

        match events[0].clone() {
            Event::MessageExecuted { cost, .. } => {
                assert_eq!(cost.amount, "26930");
                assert!(cost.token_id.is_none());
            }
            _ => panic!("Expected MessageExecuted event"),
        }
    }

    #[tokio::test]
    async fn test_multiple_message_executed() {
        let txs = transaction_fixtures();
        let parser = TransactionParser::new("solana".to_string());
        let events = parser.parse_transaction(txs[6].clone()).await.unwrap();

        assert_eq!(events.len(), 2);

        match events[0].clone() {
            Event::MessageExecuted { cost, .. } => {
                assert_eq!(cost.amount, "13465");
                assert!(cost.token_id.is_none());
            }
            _ => panic!("Expected MessageExecuted event"),
        }
        match events[1].clone() {
            Event::MessageExecuted { cost, .. } => {
                assert_eq!(cost.amount, "13465");
                assert!(cost.token_id.is_none());
            }
            _ => panic!("Expected MessageExecuted event"),
        }
    }

    #[tokio::test]
    async fn test_message_approved() {
        let txs = transaction_fixtures();
        let parser = TransactionParser::new("solana".to_string());
        let events = parser.parse_transaction(txs[1].clone()).await.unwrap();
        assert_eq!(events.len(), 1);

        match events[0].clone() {
            Event::MessageApproved { cost, .. } => {
                assert_eq!(cost.amount, "38208");
                assert!(cost.token_id.is_none());
            }
            _ => panic!("Expected MessageApproved event"),
        }
    }

    #[tokio::test]
    async fn test_multiple_message_approved() {
        let txs = transaction_fixtures();
        let parser = TransactionParser::new("solana".to_string());
        let events = parser.parse_transaction(txs[5].clone()).await.unwrap();
        assert_eq!(events.len(), 2);

        match events[0].clone() {
            Event::MessageApproved { cost, .. } => {
                assert_eq!(cost.amount, "19104");
                assert!(cost.token_id.is_none());
            }
            _ => panic!("Expected MessageApproved event"),
        }

        match events[1].clone() {
            Event::MessageApproved { cost, .. } => {
                assert_eq!(cost.amount, "19104");
                assert!(cost.token_id.is_none());
            }
            _ => panic!("Expected MessageApproved event"),
        }
    }

    #[tokio::test]
    async fn test_gas_refunded() {
        let txs = transaction_fixtures();
        let parser = TransactionParser::new("solana".to_string());
        let events = parser.parse_transaction(txs[2].clone()).await.unwrap();
        assert_eq!(events.len(), 1);

        match events[0].clone() {
            Event::GasRefunded { cost, .. } => {
                assert_eq!(cost.amount, "13085");
                assert!(cost.token_id.is_none());
            }
            _ => panic!("Expected GasRefunded event"),
        }
    }

    #[tokio::test]
    async fn test_gas_added() {
        let txs = transaction_fixtures();
        let parser = TransactionParser::new("solana".to_string());
        let events = parser.parse_transaction(txs[4].clone()).await.unwrap();
        assert_eq!(events.len(), 1);

        match events[0].clone() {
            Event::GasCredit { message_id, .. } => {
                assert_eq!(message_id, "3oViqY1trepjh1wYWnwGH2JxuQXz2h4ro18GwvNEDdTpLhiZVTFPXSvgAre3yzcXUouNuDSNkpNfSsUEpg23Snu5-0");
            }
            _ => panic!("Expected GasCredit event"),
        }
    }
}
