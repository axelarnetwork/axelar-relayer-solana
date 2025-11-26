use crate::types::SolanaTransaction;
use async_trait::async_trait;
use relayer_core::error::IngestorError;
use relayer_core::gmp_api::gmp_types::{
    ConstructProofTask, Event, ReactToWasmEventTask, RetryTask, VerifyTask,
};
use relayer_core::ingestor::IngestorTrait;
use relayer_core::models::gmp_events::EventModel;
use relayer_core::subscriber::ChainTransaction;
use relayer_core::utils::ThreadSafe;
use tracing::{info, warn};

use crate::solana_transaction::{EventSummary, UpdateEvents};
use solana_transaction_parser::parser::TransactionParserTrait;

#[derive(Clone)]
pub struct SolanaIngestor<TP: TransactionParserTrait, STM: UpdateEvents> {
    solana_parser: TP,
    solana_transaction_model: STM,
}

impl<TP: TransactionParserTrait, STM: UpdateEvents> SolanaIngestor<TP, STM> {
    pub fn new(solana_parser: TP, solana_transaction_model: STM) -> Self {
        Self {
            solana_parser,
            solana_transaction_model,
        }
    }
}

#[async_trait]
impl<TP, STM> IngestorTrait for SolanaIngestor<TP, STM>
where
    TP: TransactionParserTrait + ThreadSafe,
    STM: UpdateEvents,
{
    async fn handle_verify(&self, task: VerifyTask) -> Result<(), IngestorError> {
        warn!("handle_verify: {:?}", task);

        Err(IngestorError::GenericError(
            "Still not implemented".to_string(),
        ))
    }

    async fn handle_transaction(
        &self,
        chain_transaction: ChainTransaction,
    ) -> Result<Vec<Event>, IngestorError> {
        let transaction: SolanaTransaction = serde_json::from_str(&chain_transaction)
            .map_err(|e| IngestorError::GenericError(e.to_string()))?;

        let events = self
            .solana_parser
            .parse_transaction(
                serde_json::to_string(&transaction)
                    .map_err(|e| IngestorError::GenericError(e.to_string()))?,
            )
            .await
            .map_err(|e| IngestorError::GenericError(e.to_string()))?;

        let event_models: Vec<EventModel> =
            events.clone().into_iter().map(EventModel::from).collect();

        let event_summaries: Vec<EventSummary> =
            event_models.iter().map(EventSummary::from).collect();

        info!("Created {} event summaries", event_summaries.len());

        if !event_summaries.is_empty() {
            self.solana_transaction_model
                .update_events(transaction.clone().signature.to_string(), event_summaries)
                .await
                .map_err(|e| IngestorError::GenericError(e.to_string()))?;

            info!("Updated transaction with event summaries");
        }

        Ok(events)
    }

    async fn handle_wasm_event(&self, task: ReactToWasmEventTask) -> Result<(), IngestorError> {
        warn!("handle_wasm_event: {:?}", task);

        Err(IngestorError::GenericError(
            "Still not implemented".to_string(),
        ))
    }

    async fn handle_construct_proof(&self, task: ConstructProofTask) -> Result<(), IngestorError> {
        warn!("handle_construct_proof: {:?}", task);

        Err(IngestorError::GenericError(
            "Still not implemented".to_string(),
        ))
    }

    async fn handle_retriable_task(&self, task: RetryTask) -> Result<(), IngestorError> {
        warn!("handle_retriable_task: {:?}", task);

        Err(IngestorError::GenericError(
            "Still not implemented".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::solana_transaction::MockUpdateEvents;
    use crate::types::SolanaTransaction;
    use relayer_core::gmp_api::gmp_types::Event;
    use solana_sdk::signature::Signature;
    use solana_transaction_parser::gmp_types::{CommonEventFields, GatewayV2Message};
    use solana_transaction_parser::parser::MockTransactionParserTrait;

    #[tokio::test]
    async fn test_handle_transaction_invalid_chain_transaction_returns_error() {
        let mock_parser = MockTransactionParserTrait::new();
        let mock_transaction_model = MockUpdateEvents::new();

        let ingestor = SolanaIngestor::new(mock_parser, mock_transaction_model);

        let invalid_chain_transaction = "invalid json".to_string();

        let result = ingestor.handle_transaction(invalid_chain_transaction).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_handle_transaction_empty_event_summaries_does_not_call_transaction_model() {
        let mut mock_parser = MockTransactionParserTrait::new();
        let mut mock_transaction_model = MockUpdateEvents::new();

        mock_parser
            .expect_parse_transaction()
            .times(1)
            .returning(|_| Ok(vec![]));

        mock_transaction_model.expect_update_events().times(0);

        let ingestor = SolanaIngestor::new(mock_parser, mock_transaction_model);

        let signature = Signature::new_unique();
        let solana_tx = SolanaTransaction {
            signature,
            timestamp: None,
            slot: 12345,
            logs: vec!["log1".to_string(), "log2".to_string()],
            ixs: vec![],
            account_keys: vec![],
            cost_units: 5000,
        };
        let chain_transaction = serde_json::to_string(&solana_tx).unwrap();

        let result = ingestor.handle_transaction(chain_transaction).await;

        assert!(result.is_ok());
        let events = result.unwrap();
        assert!(events.is_empty());
    }

    #[tokio::test]
    async fn test_handle_transaction_returns_events_from_parser() {
        let mut mock_parser = MockTransactionParserTrait::new();
        let mut mock_transaction_model = MockUpdateEvents::new();
        let mock_events = vec![Event::Call {
            common: CommonEventFields {
                r#type: "CALL".to_string(),
                event_id: "test_event_1".to_string(),
                meta: None,
            },
            message: GatewayV2Message {
                message_id: "test_msg_1".to_string(),
                source_chain: "test_chain".to_string(),
                source_address: "test_source".to_string(),
                destination_address: "test_destination".to_string(),
                payload_hash: "test_payload_hash".to_string(),
            },
            destination_chain: "test_destination_chain".to_string(),
            payload: "test_payload".to_string(),
        }];

        let mock_event_summary = EventSummary {
            event_id: "test_event_1".to_string(),
            message_id: Some("test_msg_1".to_string()),
            event_type: "CALL".to_string(),
        };

        mock_parser
            .expect_parse_transaction()
            .times(1)
            .returning(move |_| {
                let events = mock_events.clone();
                Ok(events)
            });

        mock_transaction_model
            .expect_update_events()
            .times(1)
            .withf(move |signature, events| {
                signature == signature && events[0] == mock_event_summary
            })
            .returning(|_, _| Box::pin(async { Ok(()) }));

        let ingestor = SolanaIngestor::new(mock_parser, mock_transaction_model);

        let signature = Signature::new_unique();
        let solana_tx = SolanaTransaction {
            signature,
            timestamp: None,
            slot: 12345,
            logs: vec!["test log".to_string()],
            ixs: vec![],
            account_keys: vec![],
            cost_units: 5000,
        };
        let chain_transaction = serde_json::to_string(&solana_tx).unwrap();

        let result = ingestor.handle_transaction(chain_transaction).await;

        assert!(result.is_ok());
        let returned_events = result.unwrap();
        assert!(returned_events.len() == 1);
    }
}
