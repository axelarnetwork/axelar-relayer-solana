use crate::error::TransactionParsingError;
use crate::transaction_parser::discriminators::{
    CPI_EVENT_DISC, ITS_TOKEN_METADATA_REGISTERED_EVENT_DISC,
};
use crate::transaction_parser::message_matching_key::MessageMatchingKey;
use crate::transaction_parser::parser::{Parser, ParserConfig};
use async_trait::async_trait;
use borsh::BorshDeserialize;
use relayer_core::gmp_api::gmp_types::{CommonEventFields, Event, EventMetadata};
use solana_sdk::pubkey::Pubkey;
use solana_transaction_status::UiCompiledInstruction;
use tracing::{debug, warn};

#[derive(BorshDeserialize, Clone, Debug)]
pub struct TokenMetadataRegistered {
    pub token_address: Pubkey,
    pub decimals: u8,
}

pub struct ParserTokenMetadataRegistered {
    signature: String,
    parsed: Option<TokenMetadataRegistered>,
    instruction: UiCompiledInstruction,
    config: ParserConfig,
}

impl ParserTokenMetadataRegistered {
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
                event_type_discriminator: ITS_TOKEN_METADATA_REGISTERED_EVENT_DISC,
            },
        })
    }

    fn try_extract_with_config(
        instruction: &UiCompiledInstruction,
        config: ParserConfig,
    ) -> Option<TokenMetadataRegistered> {
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
        match TokenMetadataRegistered::try_from_slice(payload) {
            Ok(event) => {
                debug!("Token Metadata Registered event={:?}", event);
                Some(event)
            }
            Err(_) => None,
        }
    }
}

#[async_trait]
impl Parser for ParserTokenMetadataRegistered {
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
            "MessageMatchingKey is not available for TokenMetadataRegistered".to_string(),
        ))
    }

    async fn event(&self, message_id: Option<String>) -> Result<Event, TransactionParsingError> {
        let parsed = self
            .parsed
            .clone()
            .ok_or_else(|| TransactionParsingError::Message("Missing parsed".to_string()))?;

        Ok(Event::ITSTokenMetadataRegistered {
            common: CommonEventFields {
                r#type: "ITS/TOKEN_METADATA_REGISTERED".to_owned(),
                event_id: format!("{}-its-metadata", self.signature.clone()),
                meta: Some(EventMetadata {
                    tx_id: Some(self.signature.clone()),
                    from_address: None,
                    finalized: None,
                    source_context: None,
                    timestamp: chrono::Utc::now()
                        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                }),
            },
            message_id: message_id.ok_or_else(|| {
                TransactionParsingError::Message("Missing message_id".to_string())
            })?,
            address: parsed.token_address.to_string(),
            decimals: parsed.decimals,
        })
    }

    async fn message_id(&self) -> Result<Option<String>, TransactionParsingError> {
        Ok(None)
    }
}
