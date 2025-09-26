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

use crate::parser::TransactionParserTrait;
use crate::solana_transaction::{EventSummary, UpdateEvents};

#[derive(Clone)]
pub struct SolanaIngestor<TP: TransactionParserTrait + Sync, STM: UpdateEvents + ThreadSafe> {
    solana_parser: TP,
    solana_transaction_model: STM,
}

impl<TP: TransactionParserTrait + Sync, STM: UpdateEvents + ThreadSafe> SolanaIngestor<TP, STM> {
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
    STM: UpdateEvents + ThreadSafe,
{
    async fn handle_verify(&self, task: VerifyTask) -> Result<(), IngestorError> {
        warn!("handle_verify: {:?}", task);

        Err(IngestorError::GenericError(
            "Still not implemented".to_string(),
        ))
    }

    async fn handle_transaction(
        &self,
        transaction: ChainTransaction,
    ) -> Result<Vec<Event>, IngestorError> {
        let transaction: SolanaTransaction = serde_json::from_str(&transaction)
            .map_err(|e| IngestorError::GenericError(e.to_string()))?;

        let events = self
            .solana_parser
            .parse_transaction(transaction.clone())
            .await
            .map_err(|e| IngestorError::GenericError(e.to_string()))?;

        let event_models: Vec<EventModel> = events
            .iter()
            .map(|event| EventModel::from_event(event.clone()))
            .collect();

        let event_summaries: Vec<EventSummary> = event_models
            .iter()
            .map(|model| EventSummary {
                event_id: model.event_id.clone(),
                message_id: model.message_id.clone(),
                event_type: model.event_type.clone(),
            })
            .collect();

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
