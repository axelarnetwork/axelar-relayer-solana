use crate::common::check_discriminators_and_address;
use crate::error::TransactionParsingError;
use crate::message_matching_key::MessageMatchingKey;
use crate::parser::Parser;
use anchor_lang::AnchorDeserialize;
use async_trait::async_trait;
use base64::prelude::BASE64_STANDARD;
use base64::Engine as _;
use relayer_core::gmp_api::gmp_types::{CommonEventFields, Event, EventMetadata, TokenManagerType};
use solana_axelar_its::events::LinkTokenStarted;
use solana_sdk::pubkey::Pubkey;
use solana_transaction_status::UiCompiledInstruction;
use tracing::debug;
use uuid::Uuid;

pub struct ParserLinkTokenStarted {
    signature: String,
    parsed: Option<LinkTokenStarted>,
    instruction: UiCompiledInstruction,
    expected_contract_address: Pubkey,
    accounts: Vec<String>,
    timestamp: String,
}

impl ParserLinkTokenStarted {
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
    ) -> Result<LinkTokenStarted, TransactionParsingError> {
        let payload =
            check_discriminators_and_address(instruction, expected_contract_address, accounts)?;
        match LinkTokenStarted::deserialize(&mut payload.as_slice()) {
            Ok(event) => {
                debug!("Link Token Started event={:?}", event);
                Ok(event)
            }
            Err(_) => Err(TransactionParsingError::InvalidInstructionData(
                "invalid link token started event".to_string(),
            )),
        }
    }
}

#[async_trait]
impl Parser for ParserLinkTokenStarted {
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
            "MessageMatchingKey is not available for LinkTokenStarted".to_string(),
        ))
    }

    async fn event(&self, message_id: Option<String>) -> Result<Event, TransactionParsingError> {
        let parsed = self
            .parsed
            .clone()
            .ok_or_else(|| TransactionParsingError::Message("Missing parsed".to_string()))?;

        Ok(Event::ITSLinkTokenStarted {
            common: CommonEventFields {
                r#type: "ITS/LINK_TOKEN_STARTED".to_owned(),
                event_id: format!("{}-its-link-token-started", Uuid::new_v4()),
                meta: Some(EventMetadata {
                    tx_id: Some(self.signature.clone()),
                    from_address: None,
                    finalized: None,
                    source_context: None,
                    timestamp: self.timestamp.clone(),
                }),
            },
            destination_chain: parsed.destination_chain.clone(),
            message_id: message_id.ok_or_else(|| {
                TransactionParsingError::Message("Missing message_id".to_string())
            })?,
            token_id: hex::encode(parsed.token_id),
            source_token_address: BASE64_STANDARD.encode(parsed.source_token_address),
            destination_token_address: BASE64_STANDARD.encode(parsed.destination_token_address),
            token_manager_type: u8_to_token_manager_type(parsed.token_manager_type)?,
            //params: parsed.params, // TBD if we need this
        })
    }

    async fn message_id(&self) -> Result<Option<String>, TransactionParsingError> {
        Ok(None)
    }
}

pub fn u8_to_token_manager_type(value: u8) -> Result<TokenManagerType, TransactionParsingError> {
    match value {
        0 => Ok(TokenManagerType::NativeInterchainToken),
        1 => Ok(TokenManagerType::MintBurnFrom),
        2 => Ok(TokenManagerType::LockUnlock),
        3 => Ok(TokenManagerType::LockUnlockFee),
        4 => Ok(TokenManagerType::MintBurn),
        _ => Err(TransactionParsingError::Message(
            "Invalid token manager type".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use solana_transaction_status::UiInstruction;

    use super::*;
    use crate::parser_its_link_token_started::ParserLinkTokenStarted;
    use crate::test_utils::fixtures::transaction_fixtures;
    #[tokio::test]
    async fn test_parser() {
        let txs = transaction_fixtures();

        let tx = txs[8].clone();
        let compiled_ix: UiCompiledInstruction = match tx.ixs[1].instructions[0].clone() {
            UiInstruction::Compiled(ix) => ix,
            _ => panic!("expected a compiled instruction"),
        };

        let mut parser = ParserLinkTokenStarted::new(
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
            Event::ITSLinkTokenStarted { ref common, .. } => {
                let expected_event = Event::ITSLinkTokenStarted {
                    common: CommonEventFields {
                        r#type: "ITS/LINK_TOKEN_STARTED".to_owned(),
                        event_id: common.event_id.clone(),
                        meta: Some(EventMetadata {
                            tx_id: Some(sig.to_string()),
                            from_address: None,
                            finalized: None,
                            source_context: None,
                            timestamp: parser.timestamp.clone(),
                        }),
                    },
                    message_id: format!("{}-1", sig),
                    token_id: hex::encode(parser.parsed.as_ref().unwrap().token_id),
                    source_token_address: BASE64_STANDARD.encode(
                        parser
                            .parsed
                            .as_ref()
                            .unwrap()
                            .source_token_address
                            .to_bytes(),
                    ),
                    destination_token_address: BASE64_STANDARD.encode(
                        parser
                            .parsed
                            .as_ref()
                            .unwrap()
                            .destination_token_address
                            .clone(),
                    ),
                    token_manager_type: u8_to_token_manager_type(
                        parser.parsed.as_ref().unwrap().token_manager_type,
                    )
                    .unwrap(),
                    destination_chain: parser.parsed.as_ref().unwrap().destination_chain.clone(),
                };
                assert_eq!(event, expected_event);
            }
            _ => panic!("Expected ITSLinkTokenStarted event"),
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

        let mut parser = ParserLinkTokenStarted::new(
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
