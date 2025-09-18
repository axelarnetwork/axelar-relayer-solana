use crate::error::TransactionParsingError;
use crate::transaction_parser::common::check_discriminators_and_address;
use crate::transaction_parser::discriminators::{CPI_EVENT_DISC, LOG_SIGNERS_ROTATED_EVENT_DISC};
use crate::transaction_parser::message_matching_key::MessageMatchingKey;
use crate::transaction_parser::parser::{Parser, ParserConfig};
use async_trait::async_trait;
use borsh::BorshDeserialize;
use relayer_core::gmp_api::gmp_types::{
    CommonEventFields, Event, EventMetadata, SignersRotatedEventMetadata,
};
use solana_sdk::pubkey::Pubkey;
use solana_transaction_status::UiCompiledInstruction;
use tracing::debug;

#[derive(BorshDeserialize, Clone, Debug)]
pub struct LogSignersRotatedMessage {
    pub signers_hash: String,
    pub epoch: u64,
}

pub struct ParserLogSignersRotated {
    signature: String,
    parsed: Option<LogSignersRotatedMessage>,
    instruction: UiCompiledInstruction,
    config: ParserConfig,
    index: u64,
}

impl ParserLogSignersRotated {
    pub(crate) async fn new(
        signature: String,
        instruction: UiCompiledInstruction,
        index: u64,
        expected_contract_address: Pubkey,
    ) -> Result<Self, TransactionParsingError> {
        Ok(Self {
            signature,
            parsed: None,
            instruction,
            config: ParserConfig {
                event_cpi_discriminator: CPI_EVENT_DISC,
                event_type_discriminator: LOG_SIGNERS_ROTATED_EVENT_DISC,
                expected_contract_address,
            },
            index,
        })
    }

    fn try_extract_with_config(
        instruction: &UiCompiledInstruction,
        config: ParserConfig,
    ) -> Option<LogSignersRotatedMessage> {
        let payload = check_discriminators_and_address(instruction, config)?;
        match LogSignersRotatedMessage::try_from_slice(payload.into_iter().as_slice()) {
            Ok(event) => {
                debug!("Signers Rotated event={:?}", event);
                Some(event)
            }
            Err(_) => None,
        }
    }
}

#[async_trait]
impl Parser for ParserLogSignersRotated {
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
            "MessageMatchingKey is not available for LogSignersRotatedEvent".to_string(),
        ))
    }

    async fn event(&self, _: Option<String>) -> Result<Event, TransactionParsingError> {
        let parsed = self
            .parsed
            .clone()
            .ok_or_else(|| TransactionParsingError::Message("Missing parsed".to_string()))?;

        Ok(Event::SignersRotated {
            common: CommonEventFields {
                r#type: "SIGNERS_ROTATED".to_owned(),
                event_id: self.signature.clone(),
                meta: Some(SignersRotatedEventMetadata {
                    common_meta: EventMetadata {
                        tx_id: Some(self.signature.clone()),
                        from_address: None,
                        finalized: None,
                        source_context: None,
                        timestamp: chrono::Utc::now()
                            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                    },
                    signers_hash: Some(parsed.signers_hash.clone()),
                    epoch: Some(parsed.epoch),
                }),
            },
            message_id: self.signature.clone(),
        })
    }

    async fn message_id(&self) -> Result<Option<String>, TransactionParsingError> {
        Ok(Some(format!("{}-{}", self.signature, self.index)))
    }
}
