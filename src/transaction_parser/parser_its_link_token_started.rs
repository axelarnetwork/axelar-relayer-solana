use crate::error::TransactionParsingError;
use crate::transaction_parser::common::check_discriminators_and_address;
use crate::transaction_parser::discriminators::{
    CPI_EVENT_DISC, ITS_LINK_TOKEN_STARTED_EVENT_DISC,
};
use crate::transaction_parser::message_matching_key::MessageMatchingKey;
use crate::transaction_parser::parser::{Parser, ParserConfig};
use async_trait::async_trait;
use borsh::BorshDeserialize;
use relayer_core::gmp_api::gmp_types::{CommonEventFields, Event, EventMetadata, TokenManagerType};
use solana_sdk::pubkey::Pubkey;
use solana_transaction_status::UiCompiledInstruction;
use tracing::debug;

#[derive(BorshDeserialize, Clone, Debug)]
pub struct LinkTokenStarted {
    pub token_id: [u8; 32],
    pub destination_chain: String,
    pub source_token_address: Pubkey,
    pub destination_token_address: Vec<u8>,
    pub token_manager_type: u8,
    pub _params: Vec<u8>,
}

pub struct ParserLinkTokenStarted {
    signature: String,
    parsed: Option<LinkTokenStarted>,
    instruction: UiCompiledInstruction,
    config: ParserConfig,
    accounts: Vec<String>,
}

impl ParserLinkTokenStarted {
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
                event_type_discriminator: ITS_LINK_TOKEN_STARTED_EVENT_DISC,
                expected_contract_address,
            },
            accounts,
        })
    }

    fn try_extract_with_config(
        instruction: &UiCompiledInstruction,
        config: ParserConfig,
        accounts: &[String],
    ) -> Result<LinkTokenStarted, TransactionParsingError> {
        let payload = check_discriminators_and_address(instruction, config, accounts)?;
        match LinkTokenStarted::try_from_slice(payload.into_iter().as_slice()) {
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
                event_id: format!("{}-its-link-token-started", self.signature.clone()),
                meta: Some(EventMetadata {
                    tx_id: Some(self.signature.clone()),
                    from_address: None,
                    finalized: None,
                    source_context: None,
                    timestamp: chrono::Utc::now()
                        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                }),
            },
            destination_chain: parsed.destination_chain.clone(),
            message_id: message_id.ok_or_else(|| {
                TransactionParsingError::Message("Missing message_id".to_string())
            })?,
            token_id: hex::encode(parsed.token_id),
            source_token_address: hex::encode(parsed.source_token_address),
            destination_token_address: hex::encode(parsed.destination_token_address),
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
    use crate::test_utils::fixtures::transaction_fixtures;
    use crate::transaction_parser::parser_its_link_token_started::ParserLinkTokenStarted;
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
            Event::ITSLinkTokenStarted { .. } => {
                let expected_event = Event::ITSLinkTokenStarted {
                    common: CommonEventFields {
                        r#type: "ITS/LINK_TOKEN_STARTED".to_owned(),
                        event_id: format!("{}-its-link-token-started", sig),
                        meta: Some(EventMetadata {
                            tx_id: Some(sig.to_string()),
                            from_address: None,
                            finalized: None,
                            source_context: None,
                            timestamp: chrono::Utc::now()
                                .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                        }),
                    },
                    message_id: format!("{}-1", sig),
                    token_id: hex::encode(parser.parsed.as_ref().unwrap().token_id),
                    source_token_address: hex::encode(
                        parser.parsed.as_ref().unwrap().source_token_address,
                    ),
                    destination_token_address: hex::encode(
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
            Pubkey::from_str("7RdSDLUUy37Wqc6s9ebgo52AwhGiw4XbJWZJgidQ1fJc").unwrap(),
            tx.account_keys,
        )
        .await
        .unwrap();
        assert!(!parser.is_match().await.unwrap());
    }
}
