use std::collections::HashMap;

use crate::error::TransactionParsingError;
use crate::transaction_parser::common::check_discriminators_and_address;
use crate::transaction_parser::discriminators::{
    CPI_EVENT_DISC, ITS_INTERCHAIN_TRANSFER_EVENT_DISC,
};
use crate::transaction_parser::message_matching_key::MessageMatchingKey;
use crate::transaction_parser::parser::{Parser, ParserConfig};
use async_trait::async_trait;
use borsh::BorshDeserialize;
use relayer_core::gmp_api::gmp_types::{Amount, CommonEventFields, Event, EventMetadata};
use solana_sdk::pubkey::Pubkey;
use solana_transaction_status::UiCompiledInstruction;
use tracing::debug;

#[derive(BorshDeserialize, Clone, Debug)]
pub struct InterchainTransfer {
    pub token_id: [u8; 32],
    pub source_address: Pubkey,
    pub source_token_account: Pubkey,
    pub destination_chain: String,
    pub destination_address: Vec<u8>,
    pub amount: u64,
    pub data_hash: [u8; 32],
}

pub struct ParserInterchainTransfer {
    signature: String,
    parsed: Option<InterchainTransfer>,
    instruction: UiCompiledInstruction,
    config: ParserConfig,
    accounts: Vec<String>,
}

impl ParserInterchainTransfer {
    pub(crate) async fn new(
        signature: String,
        instruction: UiCompiledInstruction,
        expected_contract_address: Pubkey,
        accounts: Vec<String>,
    ) -> Result<Self, TransactionParsingError> {
        Ok(Self {
            signature,
            parsed: None,
            instruction,
            config: ParserConfig {
                event_cpi_discriminator: CPI_EVENT_DISC,
                event_type_discriminator: ITS_INTERCHAIN_TRANSFER_EVENT_DISC,
                expected_contract_address,
            },
            accounts,
        })
    }

    fn try_extract_with_config(
        instruction: &UiCompiledInstruction,
        config: ParserConfig,
        accounts: &[String],
    ) -> Result<InterchainTransfer, TransactionParsingError> {
        let payload = check_discriminators_and_address(instruction, config, accounts)?;
        match InterchainTransfer::try_from_slice(payload.into_iter().as_slice()) {
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
                event_id: format!("{}-its-interchain-transfer", self.signature.clone()),
                meta: Some(EventMetadata {
                    tx_id: Some(self.signature.clone()),
                    from_address: None,
                    finalized: None,
                    source_context: Some(HashMap::from([(
                        "source_token_account".to_owned(),
                        parsed.source_token_account.to_string(),
                    )])),
                    timestamp: chrono::Utc::now()
                        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                }),
            },
            source_address: parsed.source_address.to_string(),
            destination_chain: parsed.destination_chain.clone(),
            destination_address: hex::encode(parsed.destination_address),
            data_hash: hex::encode(parsed.data_hash),
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
    use crate::test_utils::fixtures::transaction_fixtures;
    use crate::transaction_parser::parser_its_interchain_transfer::ParserInterchainTransfer;
    #[tokio::test]
    async fn test_parser() {
        let txs = transaction_fixtures();

        let tx = txs[7].clone();
        let compiled_ix: UiCompiledInstruction = match tx.ixs[1].instructions[0].clone() {
            UiInstruction::Compiled(ix) => ix,
            _ => panic!("expected a compiled instruction"),
        };

        let mut parser = ParserInterchainTransfer::new(
            tx.signature.to_string(),
            compiled_ix,
            Pubkey::from_str("7RdSDLUUy37Wqc6s9ebgo52AwhGiw4XbJWZJgidQ1fJc").unwrap(),
            tx.account_keys,
        )
        .await
        .unwrap();
        assert!(parser.is_match().await.unwrap());
        let sig = tx.signature.clone().to_string();
        parser.parse().await.unwrap();
        let event = parser.event(Some(format!("{}-1", sig))).await.unwrap();
        match event {
            Event::ITSInterchainTransfer { .. } => {
                let expected_event = Event::ITSInterchainTransfer {
                    common: CommonEventFields {
                        r#type: "ITS/INTERCHAIN_TRANSFER".to_owned(),
                        event_id: format!("{}-its-interchain-transfer", sig),
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
                            timestamp: chrono::Utc::now()
                                .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                        }),
                    },
                    source_address: parser.parsed.as_ref().unwrap().source_address.to_string(),
                    destination_chain: parser.parsed.as_ref().unwrap().destination_chain.clone(),
                    destination_address: hex::encode(
                        parser.parsed.as_ref().unwrap().destination_address.clone(),
                    ),
                    data_hash: hex::encode(parser.parsed.as_ref().unwrap().data_hash),
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
            Pubkey::from_str("7RdSDLUUy37Wqc6s9ebgo52AwhGiw4XbJWZJgidQ1fJc").unwrap(),
            tx.account_keys,
        )
        .await
        .unwrap();
        assert!(!parser.is_match().await.unwrap());
    }
}
