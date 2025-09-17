use crate::error::TransactionParsingError;
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
use tracing::{debug, warn};

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
}

impl ParserLinkTokenStarted {
    pub(crate) async fn new(
        signature: String,
        instruction: UiCompiledInstruction,
    ) -> Result<Self, TransactionParsingError> {
        Ok(Self {
            signature,
            parsed: None,
            instruction,
            config: ParserConfig {
                event_cpi_discriminator: CPI_EVENT_DISC,
                event_type_discriminator: ITS_LINK_TOKEN_STARTED_EVENT_DISC,
            },
        })
    }

    fn try_extract_with_config(
        instruction: &UiCompiledInstruction,
        config: ParserConfig,
    ) -> Option<LinkTokenStarted> {
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
        match LinkTokenStarted::try_from_slice(payload) {
            Ok(event) => {
                debug!("Link Token Started event={:?}", event);
                Some(event)
            }
            Err(_) => None,
        }
    }
}

#[async_trait]
impl Parser for ParserLinkTokenStarted {
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
