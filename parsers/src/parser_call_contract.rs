use crate::common::check_discriminators_and_address;
use crate::error::TransactionParsingError;
use crate::instruction_index::InstructionIndex;
use crate::message_matching_key::MessageMatchingKey;
use crate::parser::Parser;
use anchor_lang::AnchorDeserialize;
use async_trait::async_trait;
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use relayer_core::gmp_api::gmp_types::{CommonEventFields, Event, EventMetadata, GatewayV2Message};
use solana_axelar_gateway::events::CallContractEvent;
use solana_sdk::pubkey::Pubkey;
use solana_transaction_status::UiCompiledInstruction;
use std::collections::HashMap;
use tracing::debug;
use uuid::Uuid;

pub struct ParserCallContract {
    signature: String,
    parsed: Option<CallContractEvent>,
    instruction: UiCompiledInstruction,
    expected_contract_address: Pubkey,
    accounts: Vec<String>,
    chain_name: String,
    index: InstructionIndex,
    timestamp: String,
}

impl ParserCallContract {
    pub(crate) async fn new(
        signature: String,
        instruction: UiCompiledInstruction,
        accounts: Vec<String>,
        chain_name: String,
        index: InstructionIndex,
        expected_contract_address: Pubkey,
        timestamp: String,
    ) -> Result<Self, TransactionParsingError> {
        Ok(Self {
            signature,
            parsed: None,
            instruction,
            expected_contract_address,
            accounts,
            chain_name,
            index,
            timestamp,
        })
    }

    fn try_extract_with_config(
        instruction: &UiCompiledInstruction,
        expected_contract_address: Pubkey,
        accounts: &[String],
    ) -> Result<CallContractEvent, TransactionParsingError> {
        let payload =
            check_discriminators_and_address(instruction, expected_contract_address, accounts)?;
        match CallContractEvent::deserialize(&mut payload.as_slice()) {
            Ok(event) => {
                debug!("Call Contract event={:?}", event);
                Ok(event)
            }
            Err(_) => Err(TransactionParsingError::InvalidInstructionData(
                "invalid call contract event".to_string(),
            )),
        }
    }
}

#[async_trait]
impl Parser for ParserCallContract {
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
            ("source_address".to_owned(), parsed.sender.to_string()),
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
                event_id: format!("{}-call", Uuid::new_v4()),
                meta: Some(EventMetadata {
                    tx_id: Some(self.signature.clone()),
                    from_address: None,
                    finalized: None,
                    source_context: Some(source_context),
                    timestamp: self.timestamp.clone(),
                }),
            },
            message: GatewayV2Message {
                message_id,
                source_chain: self.chain_name.to_string(),
                source_address: parsed.sender.to_string(),
                destination_address: parsed.destination_contract_address.to_string(),
                payload_hash: BASE64_STANDARD.encode(parsed.payload_hash),
            },
            destination_chain: parsed.destination_chain.clone(),
            payload: BASE64_STANDARD.encode(parsed.payload),
        })
    }

    async fn message_id(&self) -> Result<Option<String>, TransactionParsingError> {
        Ok(Some(self.index.serialize()))
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use solana_transaction_status::UiInstruction;

    use super::*;
    use crate::parser_call_contract::ParserCallContract;
    use crate::test_utils::fixtures::transaction_fixtures;
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
            tx.account_keys,
            "solana".to_string(),
            InstructionIndex::new(tx.signature.to_string(), 1, 2),
            Pubkey::from_str("8YsLGnLV2KoyxdksgiAi3gh1WvhMrznA2toKWqyz91bR").unwrap(),
            tx.timestamp.unwrap_or_default().to_rfc3339(),
        )
        .await
        .unwrap();
        let sig = tx.signature.clone().to_string();
        assert_eq!(
            parser.message_id().await.unwrap().unwrap(),
            format!("{}-1.2", sig)
        );
        parser.parse().await.unwrap();
        let event = parser.event(None).await.unwrap();
        match event {
            Event::Call { ref common, .. } => {
                let expected_event = Event::Call {
                    common: CommonEventFields {
                        r#type: "CALL".to_owned(),
                        event_id: common.event_id.clone(),
                        meta: Some(EventMetadata {
                            tx_id: Some(sig.to_string()),
                            from_address: None,
                            finalized: None,
                            source_context: Some(HashMap::from([
                                (
                                    "source_address".to_owned(),
                                    parser.parsed.as_ref().unwrap().sender.to_string(),
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
                            timestamp: parser.timestamp.clone(),
                        }),
                    },
                    message: GatewayV2Message {
                        message_id: format!("{}-1.2", sig),
                        source_chain: "solana".to_string(),
                        source_address: parser.parsed.as_ref().unwrap().sender.to_string(),
                        destination_address: parser
                            .parsed
                            .as_ref()
                            .unwrap()
                            .destination_contract_address
                            .to_string(),
                        payload_hash: BASE64_STANDARD
                            .encode(parser.parsed.as_ref().unwrap().payload_hash),
                    },
                    destination_chain: parser
                        .parsed
                        .as_ref()
                        .unwrap()
                        .destination_chain
                        .to_string(),
                    payload: BASE64_STANDARD
                        .encode(parser.parsed.as_ref().unwrap().payload.clone()),
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
            tx.account_keys,
            "solana".to_string(),
            InstructionIndex::new(tx.signature.to_string(), 1, 2),
            Pubkey::from_str("8YsLGnLV2KoyxdksgiAi3gh1WvhMrznA2toKWqyz91bR").unwrap(),
            tx.timestamp.unwrap_or_default().to_rfc3339(),
        )
        .await
        .unwrap();

        assert!(parser.parse().await.is_err());
    }
}
