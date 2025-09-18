use std::collections::HashMap;

use crate::error::TransactionParsingError;
use crate::transaction_parser::common::check_discriminators_and_address;
use crate::transaction_parser::discriminators::{
    CPI_EVENT_DISC, ITS_INTERCHAIN_TOKEN_DEPLOYMENT_STARTED_EVENT_DISC,
};
use crate::transaction_parser::message_matching_key::MessageMatchingKey;
use crate::transaction_parser::parser::{Parser, ParserConfig};
use async_trait::async_trait;
use borsh::BorshDeserialize;
use relayer_core::gmp_api::gmp_types::{
    CommonEventFields, Event, EventMetadata, InterchainTokenDefinition,
};
use solana_sdk::pubkey::Pubkey;
use solana_transaction_status::UiCompiledInstruction;
use tracing::debug;

#[derive(BorshDeserialize, Clone, Debug)]
pub struct InterchainTokenDeploymentStarted {
    pub token_id: [u8; 32],
    pub token_name: String,
    pub token_symbol: String,
    pub token_decimals: u8,
    pub minter: Vec<u8>,
    pub destination_chain: String,
}

pub struct ParserInterchainTokenDeploymentStarted {
    signature: String,
    parsed: Option<InterchainTokenDeploymentStarted>,
    instruction: UiCompiledInstruction,
    config: ParserConfig,
}

impl ParserInterchainTokenDeploymentStarted {
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
                event_type_discriminator: ITS_INTERCHAIN_TOKEN_DEPLOYMENT_STARTED_EVENT_DISC,
                expected_contract_address,
            },
        })
    }

    fn try_extract_with_config(
        instruction: &UiCompiledInstruction,
        config: ParserConfig,
    ) -> Option<InterchainTokenDeploymentStarted> {
        let payload = check_discriminators_and_address(instruction, config)?;
        match InterchainTokenDeploymentStarted::try_from_slice(payload.into_iter().as_slice()) {
            Ok(event) => {
                debug!("Interchain Token Deployment Started event={:?}", event);
                Some(event)
            }
            Err(_) => None,
        }
    }
}

#[async_trait]
impl Parser for ParserInterchainTokenDeploymentStarted {
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
            "MessageMatchingKey is not available for InterchainTokenDeploymentStarted".to_string(),
        ))
    }

    async fn event(&self, message_id: Option<String>) -> Result<Event, TransactionParsingError> {
        let parsed = self
            .parsed
            .clone()
            .ok_or_else(|| TransactionParsingError::Message("Missing parsed".to_string()))?;

        Ok(Event::ITSInterchainTokenDeploymentStarted {
            common: CommonEventFields {
                r#type: "ITS/INTERCHAIN_TOKEN_DEPLOYMENT_STARTED".to_owned(),
                event_id: format!(
                    "{}-its-interchain-token-deployment-started",
                    self.signature.clone()
                ),
                meta: Some(EventMetadata {
                    tx_id: Some(self.signature.clone()),
                    from_address: Some(hex::encode(parsed.minter)),
                    finalized: None,
                    source_context: Some(HashMap::from([(
                        "token_id".to_owned(),
                        hex::encode(parsed.token_id),
                    )])),
                    timestamp: chrono::Utc::now()
                        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                }),
            },
            destination_chain: parsed.destination_chain.clone(),
            message_id: message_id.ok_or_else(|| {
                TransactionParsingError::Message("Missing message_id".to_string())
            })?,
            token: InterchainTokenDefinition {
                id: hex::encode(parsed.token_id),
                name: parsed.token_name.clone(),
                symbol: parsed.token_symbol.clone(),
                decimals: parsed.token_decimals,
            },
        })
    }

    async fn message_id(&self) -> Result<Option<String>, TransactionParsingError> {
        Ok(None)
    }
}
