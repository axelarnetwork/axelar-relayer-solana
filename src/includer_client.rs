use async_trait::async_trait;

use axelar_solana_gateway_v2::IncomingMessage;
use bytemuck;
use relayer_core::{error::ClientError, utils::ThreadSafe};
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::CommitmentConfig, pubkey::Pubkey, signature::Signature,
    transaction::Transaction,
};
use std::{sync::Arc, time::Duration};
use tracing::warn;

use crate::error::IncluderClientError;

#[async_trait]
#[cfg_attr(any(test), mockall::automock)]
pub trait IncluderClientTrait: ThreadSafe {
    fn inner(&self) -> &RpcClient;
    async fn send_transaction(
        &self,
        transaction: Transaction,
    ) -> Result<(Signature, Option<u64>), IncluderClientError>;
    async fn incoming_message_already_executed(
        &self,
        incoming_message_pda: &Pubkey,
    ) -> Result<bool, IncluderClientError>;
    async fn get_signature_status(&self, signature: &Signature) -> Result<(), IncluderClientError>;
    async fn get_gas_cost_from_simulation(
        &self,
        transaction: &Transaction,
    ) -> Result<u64, IncluderClientError>;
    async fn get_transaction_cost_from_signature(
        &self,
        signature: &Signature,
    ) -> Result<Option<u64>, IncluderClientError>;
}

#[derive(Clone)]
pub struct IncluderClient {
    client: Arc<RpcClient>,
    max_retries: usize,
    commitment: CommitmentConfig,
}

impl IncluderClient {
    pub fn new(
        url: &str,
        commitment: CommitmentConfig,
        max_retries: usize,
    ) -> Result<Self, ClientError> {
        Ok(Self {
            client: Arc::new(RpcClient::new_with_commitment(url.to_string(), commitment)),
            max_retries,
            commitment,
        })
    }
}

#[async_trait]
impl IncluderClientTrait for IncluderClient {
    fn inner(&self) -> &RpcClient {
        &self.client
    }

    async fn send_transaction(
        &self,
        transaction: Transaction,
    ) -> Result<(Signature, Option<u64>), IncluderClientError> {
        let mut retries = 0;
        let mut delay = Duration::from_millis(500);

        loop {
            let res = self.client.send_and_confirm_transaction(&transaction).await;
            match res {
                Ok(signature) => {
                    let cost = match self.get_transaction_cost_from_signature(&signature).await {
                        Ok(cost) => cost,
                        Err(e) => return Err(IncluderClientError::GenericError(e.to_string())),
                    };
                    return Ok((signature, cost));
                }
                Err(e) => {
                    if e.to_string().contains("Computational budget exceeded") {
                        return Err(IncluderClientError::GasExceededError(e.to_string()));
                    }
                    if e.get_transaction_error().is_some() {
                        return Err(IncluderClientError::TransactionError(e.to_string()));
                    }
                    if retries >= self.max_retries {
                        warn!(
                            "Failed to send transaction after {} retries: {}",
                            retries, e
                        );
                        return Err(IncluderClientError::MaxRetriesExceededError(e.to_string()));
                    }

                    warn!(
                        "Transaction send failed (retry {}/{}): {}. Retrying in {:?}...",
                        retries + 1,
                        self.max_retries,
                        e,
                        delay
                    );

                    tokio::time::sleep(delay).await;
                    retries += 1;
                    delay = delay.mul_f32(2.0);
                }
            }
        }
    }

    async fn incoming_message_already_executed(
        &self,
        incoming_message_pda: &Pubkey,
    ) -> Result<bool, IncluderClientError> {
        let raw_incoming_message = self
            .inner()
            .get_account_data(incoming_message_pda)
            .await
            .map_err(|e| IncluderClientError::GenericError(e.to_string()))?;
        let incoming_message = read(&raw_incoming_message).ok_or_else(|| {
            IncluderClientError::GenericError("Could not read incoming message".to_string())
        })?;
        Ok(incoming_message.status.is_executed())
    }

    async fn get_signature_status(&self, signature: &Signature) -> Result<(), IncluderClientError> {
        let status = self
            .inner()
            .get_signature_status_with_commitment(signature, self.commitment)
            .await
            .map_err(|e| IncluderClientError::GenericError(e.to_string()))?;

        match status {
            Some(Ok(_)) => Ok(()),
            Some(Err(e)) => Err(IncluderClientError::GenericError(e.to_string())),
            None => Err(IncluderClientError::GenericError(
                "Unknown transaction status".into(),
            )),
        }
    }

    async fn get_gas_cost_from_simulation(
        &self,
        transaction: &Transaction,
    ) -> Result<u64, IncluderClientError> {
        let simulation_result = self
            .inner()
            .simulate_transaction(transaction)
            .await
            .map_err(|e| IncluderClientError::GenericError(e.to_string()))?;
        Ok(simulation_result.value.units_consumed.ok_or_else(|| {
            IncluderClientError::GenericError("Units consumed not found".to_string())
        })?)
    }

    async fn get_transaction_cost_from_signature(
        &self,
        signature: &Signature,
    ) -> Result<Option<u64>, IncluderClientError> {
        use solana_rpc_client_api::config::RpcTransactionConfig;
        use solana_transaction_status::UiTransactionEncoding;

        let transaction_info = self
            .inner()
            .get_transaction_with_config(
                signature,
                RpcTransactionConfig {
                    encoding: Some(UiTransactionEncoding::Json),
                    commitment: Some(self.commitment),
                    max_supported_transaction_version: Some(0),
                },
            )
            .await
            .map_err(|e| IncluderClientError::GenericError(e.to_string()))?;

        if let Some(meta) = &transaction_info.transaction.meta {
            Ok(Some(meta.fee))
        } else {
            Ok(None)
        }
    }
}

fn read(data: &[u8]) -> Option<&IncomingMessage> {
    let result: &IncomingMessage = bytemuck::try_from_bytes(data).ok()?;
    Some(result)
}
