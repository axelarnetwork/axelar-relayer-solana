use async_trait::async_trait;

use relayer_core::{error::ClientError, utils::ThreadSafe};
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::CommitmentConfig, signature::Signature, transaction::Transaction,
};
use std::sync::Arc;

#[async_trait]
#[cfg_attr(any(test), mockall::automock)]
pub trait IncluderClientTrait: ThreadSafe {
    fn inner(&self) -> &RpcClient;
    async fn send_transaction(&self, transaction: Transaction) -> Result<Signature, ClientError>;
}

#[derive(Clone)]
pub struct IncluderClient {
    client: Arc<RpcClient>,
    max_retries: usize,
    rpc_url: String,
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
            rpc_url: url.to_string(),
        })
    }
}

#[async_trait]
impl IncluderClientTrait for IncluderClient {
    fn inner(&self) -> &RpcClient {
        &self.client
    }

    async fn send_transaction(&self, transaction: Transaction) -> Result<Signature, ClientError> {
        self.client
            .send_and_confirm_transaction(&transaction)
            .await
            .map_err(|e| ClientError::BadRequest(e.to_string()))
    }
}
