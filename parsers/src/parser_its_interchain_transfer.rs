use std::collections::HashMap;

use crate::common::check_discriminators_and_address;
use crate::error::TransactionParsingError;
use crate::message_matching_key::MessageMatchingKey;
use crate::parser::Parser;
use anchor_lang::AnchorDeserialize;
use async_trait::async_trait;
use base64::prelude::BASE64_STANDARD;
use base64::Engine as _;
use relayer_core::gmp_api::gmp_types::{Amount, CommonEventFields, Event, EventMetadata};
use solana_axelar_its::events::InterchainTransferSent;
use solana_sdk::pubkey::Pubkey;
use solana_transaction_status::UiCompiledInstruction;
use tracing::debug;
use uuid::Uuid;

pub struct ParserInterchainTransfer {
    signature: String,
    parsed: Option<InterchainTransferSent>,
    instruction: UiCompiledInstruction,
    expected_contract_address: Pubkey,
    accounts: Vec<String>,
    timestamp: String,
}

impl ParserInterchainTransfer {
    pub(crate) fn new(
        signature: String,
        instruction: UiCompiledInstruction,
        expected_contract_address: Pubkey,
        accounts: Vec<String>,
        timestamp: String,
    ) -> Result<Self, TransactionParsingError> {
        Ok(Self {
            signature,
            parsed: None,
            instruction,
            expected_contract_address,
            accounts,
            timestamp,
        })
    }

    fn try_extract_with_config(
        instruction: &UiCompiledInstruction,
        expected_contract_address: Pubkey,
        accounts: &[String],
    ) -> Result<InterchainTransferSent, TransactionParsingError> {
        let payload =
            check_discriminators_and_address(instruction, expected_contract_address, accounts)?;
        match InterchainTransferSent::deserialize(&mut payload.as_slice()) {
            Ok(event) => {
                debug!("Interchain Transfer event={:?}", event);
                Ok(event)
            }
            Err(_) => Err(TransactionParsingError::InvalidInstructionData(
                "invalid interchain transfer event".to_string(),
            )),
        }
    }
}

#[async_trait]
impl Parser for ParserInterchainTransfer {
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
            "MessageMatchingKey is not available for InterchainTransfer".to_string(),
        ))
    }

    async fn event(&self, message_id: Option<String>) -> Result<Event, TransactionParsingError> {
        let parsed = self
            .parsed
            .clone()
            .ok_or_else(|| TransactionParsingError::Message("Missing parsed".to_string()))?;

        Ok(Event::ITSInterchainTransfer {
            common: CommonEventFields {
                r#type: "ITS/INTERCHAIN_TRANSFER".to_owned(),
                event_id: format!("{}-its-interchain-transfer", Uuid::new_v4()),
                meta: Some(EventMetadata {
                    tx_id: Some(self.signature.clone()),
                    from_address: None,
                    finalized: None,
                    source_context: Some(HashMap::from([(
                        "source_token_account".to_owned(),
                        parsed.source_token_account.to_string(),
                    )])),
                    timestamp: self.timestamp.clone(),
                }),
            },
            source_address: parsed.source_address.to_string(),
            destination_chain: parsed.destination_chain.clone(),
            destination_address: BASE64_STANDARD.encode(parsed.destination_address),
            data_hash: match parsed.data_hash {
                Some(data_hash) => BASE64_STANDARD.encode(data_hash),
                None => "".to_string(),
            },
            message_id: message_id.ok_or_else(|| {
                TransactionParsingError::Message("Missing message_id".to_string())
            })?,
            token_spent: Amount {
                token_id: Some(hex::encode(parsed.token_id)),
                amount: parsed.amount.to_string(),
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
    use crate::parser_its_interchain_transfer::ParserInterchainTransfer;
    use crate::test_utils::fixtures::transaction_fixtures;
    #[tokio::test]
    async fn test_parser() {
        let txs = transaction_fixtures();

        let tx = txs[7].clone();
        let compiled_ix: UiCompiledInstruction = match tx.ixs[0].instructions[0].clone() {
            UiInstruction::Compiled(ix) => ix,
            _ => panic!("expected a compiled instruction"),
        };

        let mut parser = ParserInterchainTransfer::new(
            tx.signature.to_string(),
            compiled_ix,
            Pubkey::from_str("gtwT4uGVTYSPnTGv6rSpMheyFyczUicxVWKqdtxNGw9").unwrap(),
            tx.account_keys,
            tx.timestamp.unwrap_or_default().to_rfc3339(),
        )
        .unwrap();
        assert!(parser.parse().await.unwrap());
        let sig = tx.signature.clone().to_string();
        parser.parse().await.unwrap();
        let event = parser.event(Some(format!("{}-1", sig))).await.unwrap();
        match event {
            Event::ITSInterchainTransfer { ref common, .. } => {
                let expected_event = Event::ITSInterchainTransfer {
                    common: CommonEventFields {
                        r#type: "ITS/INTERCHAIN_TRANSFER".to_owned(),
                        event_id: common.event_id.clone(),
                        meta: Some(EventMetadata {
                            tx_id: Some(sig.to_string()),
                            from_address: None,
                            finalized: None,
                            source_context: Some(HashMap::from([(
                                "source_token_account".to_owned(),
                                parser
                                    .parsed
                                    .as_ref()
                                    .unwrap()
                                    .source_token_account
                                    .to_string(),
                            )])),
                            timestamp: parser.timestamp.clone(),
                        }),
                    },
                    source_address: parser.parsed.as_ref().unwrap().source_address.to_string(),
                    destination_chain: parser.parsed.as_ref().unwrap().destination_chain.clone(),
                    destination_address: BASE64_STANDARD
                        .encode(parser.parsed.as_ref().unwrap().destination_address.clone()),
                    data_hash: match parser.parsed.as_ref().unwrap().data_hash {
                        Some(data_hash) => BASE64_STANDARD.encode(data_hash),
                        None => "".to_string(),
                    },
                    message_id: format!("{}-1", sig),
                    token_spent: Amount {
                        token_id: Some(hex::encode(parser.parsed.as_ref().unwrap().token_id)),
                        amount: parser.parsed.as_ref().unwrap().amount.to_string(),
                    },
                };
                assert_eq!(event, expected_event);
            }
            _ => panic!("Expected ITSInterchainTransfer event"),
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

        let mut parser = ParserInterchainTransfer::new(
            tx.signature.to_string(),
            compiled_ix,
            Pubkey::from_str("gtwT4uGVTYSPnTGv6rSpMheyFyczUicxVWKqdtxNGw9").unwrap(),
            tx.account_keys,
            tx.timestamp.unwrap_or_default().to_rfc3339(),
        )
        .unwrap();
        assert!(parser.parse().await.is_err());
    }
}
