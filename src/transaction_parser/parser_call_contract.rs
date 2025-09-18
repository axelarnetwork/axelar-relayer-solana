use crate::error::TransactionParsingError;
use crate::transaction_parser::common::check_discriminators_and_address;
use crate::transaction_parser::discriminators::{CALL_CONTRACT_EVENT_DISC, CPI_EVENT_DISC};
use crate::transaction_parser::message_matching_key::MessageMatchingKey;
use crate::transaction_parser::parser::{Parser, ParserConfig};
use async_trait::async_trait;
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use borsh::BorshDeserialize;
use relayer_core::gmp_api::gmp_types::{CommonEventFields, Event, EventMetadata, GatewayV2Message};
use solana_sdk::pubkey::Pubkey;
use solana_transaction_status::UiCompiledInstruction;
use std::collections::HashMap;
use tracing::debug;

#[derive(BorshDeserialize, Clone, Debug)]
pub struct CallContractEvent {
    /// Sender's public key.
    pub sender_key: Pubkey,
    /// Payload hash, 32 bytes.
    pub payload_hash: [u8; 32],
    /// Destination chain as a `String`.
    pub destination_chain: String,
    /// Destination contract address as a `String`.
    pub destination_contract_address: String,
    /// Payload data as a `Vec<u8>`.
    pub payload: Vec<u8>,
}

pub struct ParserCallContract {
    signature: String,
    parsed: Option<CallContractEvent>,
    instruction: UiCompiledInstruction,
    config: ParserConfig,
    chain_name: String,
    index: u64,
}

impl ParserCallContract {
    pub(crate) async fn new(
        signature: String,
        instruction: UiCompiledInstruction,
        chain_name: String,
        index: u64,
        expected_contract_address: Pubkey,
    ) -> Result<Self, TransactionParsingError> {
        Ok(Self {
            signature,
            parsed: None,
            instruction,
            config: ParserConfig {
                event_cpi_discriminator: CPI_EVENT_DISC,
                event_type_discriminator: CALL_CONTRACT_EVENT_DISC,
                expected_contract_address,
            },
            chain_name,
            index,
        })
    }

    fn try_extract_with_config(
        instruction: &UiCompiledInstruction,
        config: ParserConfig,
    ) -> Option<CallContractEvent> {
        let payload = check_discriminators_and_address(instruction, config)?;
        match CallContractEvent::try_from_slice(payload.into_iter().as_slice()) {
            Ok(event) => {
                debug!("Call Contract event={:?}", event);
                Some(event)
            }
            Err(_) => None,
        }
    }
}

#[async_trait]
impl Parser for ParserCallContract {
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
        let parsed = self
            .parsed
            .clone()
            .ok_or_else(|| TransactionParsingError::Message("Missing parsed".to_string()))?;

        Ok(MessageMatchingKey {
            destination_chain: parsed.destination_chain,
            destination_address: parsed.destination_contract_address,
            payload_hash: parsed.payload_hash,
        })
    }

    async fn event(&self, _message_id: Option<String>) -> Result<Event, TransactionParsingError> {
        let parsed = self
            .parsed
            .clone()
            .ok_or_else(|| TransactionParsingError::Message("Missing parsed".to_string()))?;

        let message_id = self
            .message_id()
            .await?
            .ok_or_else(|| TransactionParsingError::Message("Missing message_id".to_string()))?;

        let source_context = HashMap::from([
            ("source_address".to_owned(), parsed.sender_key.to_string()),
            (
                "destination_address".to_owned(),
                parsed.destination_contract_address.to_string(),
            ),
            (
                "destination_chain".to_owned(),
                parsed.destination_chain.clone(),
            ),
        ]);

        Ok(Event::Call {
            common: CommonEventFields {
                r#type: "CALL".to_owned(),
                event_id: format!("{}-call", self.signature.clone()),
                meta: Some(EventMetadata {
                    tx_id: Some(self.signature.clone()),
                    from_address: None,
                    finalized: None,
                    source_context: Some(source_context),
                    timestamp: chrono::Utc::now()
                        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                }),
            },
            message: GatewayV2Message {
                message_id,
                source_chain: self.chain_name.to_string(),
                source_address: parsed.sender_key.to_string(),
                destination_address: parsed.destination_contract_address.to_string(),
                payload_hash: BASE64_STANDARD.encode(parsed.payload_hash),
            },
            destination_chain: parsed.destination_chain.clone(),
            payload: BASE64_STANDARD.encode(parsed.payload),
        })
    }

    async fn message_id(&self) -> Result<Option<String>, TransactionParsingError> {
        Ok(Some(format!("{}-{}", self.signature, self.index)))
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use solana_transaction_status::UiInstruction;

    use super::*;
    use crate::test_utils::fixtures::transaction_fixtures;
    use crate::transaction_parser::parser_call_contract::ParserCallContract;
    #[tokio::test]
    async fn test_parser() {
        let txs = transaction_fixtures();

        let tx = txs[0].clone();
        let compiled_ix: UiCompiledInstruction = match tx.ixs[1].instructions[0].clone() {
            UiInstruction::Compiled(ix) => ix,
            _ => panic!("expected a compiled instruction"),
        };

        let mut parser = ParserCallContract::new(
            tx.signature.to_string(),
            compiled_ix,
            "solana".to_string(),
            1,
            Pubkey::from_str("7RdSDLUUy37Wqc6s9ebgo52AwhGiw4XbJWZJgidQ1fJc").unwrap(),
        )
        .await
        .unwrap();
        assert!(parser.is_match().await.unwrap());
        let sig = tx.signature.clone().to_string();
        assert_eq!(
            parser.message_id().await.unwrap().unwrap(),
            format!("{}-1", sig)
        );
        parser.parse().await.unwrap();
        let event = parser.event(None).await.unwrap();
        match event {
            Event::Call { .. } => {
                let expected_event = Event::Call {
                    common: CommonEventFields {
                        r#type: "CALL".to_owned(),
                        event_id: format!("{}-call", sig),
                        meta: Some(EventMetadata {
                            tx_id: Some(sig.to_string()),
                            from_address: None,
                            finalized: None,
                            source_context: Some(HashMap::from([
                                (
                                    "source_address".to_owned(),
                                    parser.parsed.as_ref().unwrap().sender_key.to_string(),
                                ),
                                (
                                    "destination_address".to_owned(),
                                    parser
                                        .parsed
                                        .as_ref()
                                        .unwrap()
                                        .destination_contract_address
                                        .to_string(),
                                ),
                                (
                                    "destination_chain".to_owned(),
                                    parser
                                        .parsed
                                        .as_ref()
                                        .unwrap()
                                        .destination_chain
                                        .to_string(),
                                ),
                            ])),
                            timestamp: chrono::Utc::now()
                                .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                        }),
                    },
                    message: GatewayV2Message {
                        message_id: format!("{}-1", sig),
                        source_chain: "solana".to_string(),
                        source_address: "483jTxdFmFGRnzgx9nBoQM2Zao5mZxKvFgHzTb4Ytn1L".to_string(),
                        destination_address: "0x7RdSDLUUy37Wqc6s9ebgo52AwhGiw4XbJWZJgidQ1fdd"
                            .to_string(),
                        payload_hash: "dPgf4WfZm0y0HW0MzagieMrunz4vJdXlo5Nv89zsYNA=".to_string(),
                    },
                    destination_chain: "ethereum".to_string(),
                    payload: "AQIDBAU=".to_string(),
                };
                assert_eq!(event, expected_event);
            }
            _ => panic!("Expected CallContract event"),
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
        let mut parser = ParserCallContract::new(
            tx.signature.to_string(),
            compiled_ix,
            "solana".to_string(),
            1,
            Pubkey::from_str("7RdSDLUUy37Wqc6s9ebgo52AwhGiw4XbJWZJgidQ1fJc").unwrap(),
        )
        .await
        .unwrap();
        assert!(!parser.is_match().await.unwrap());
    }
}
