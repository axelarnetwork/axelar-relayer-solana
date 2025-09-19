use std::{future::Future, pin::Pin};

use anyhow::anyhow;

use futures::lock::Mutex;
use futures_util::stream::BoxStream;
use relayer_core::error::ClientError;
use solana_client::{
    rpc_config::{RpcTransactionLogsConfig, RpcTransactionLogsFilter},
    rpc_response::RpcLogsResponse,
};
use solana_pubsub_client::nonblocking::pubsub_client::PubsubClient;
use solana_rpc_client_api::response::Response as RpcResponse;
use solana_sdk::commitment_config::CommitmentConfig;

// Match the nonblocking PubsubClient logs_subscribe return type
// (BoxStream<'a, RpcResponse<RpcLogsResponse>>, UnsubscribeFn)
// UnsubscribeFn = Box<dyn FnOnce() -> BoxFuture<'static, ()> + Send>
pub type LogsSubscription<'a> = BoxStream<'a, RpcResponse<RpcLogsResponse>>;

type UnsubFn =
    Box<dyn FnOnce() -> Pin<Box<dyn core::future::Future<Output = ()> + Send>> + Send + 'static>;

pub trait SolanaStreamClientTrait: Send + Sync {
    fn inner(&self) -> &PubsubClient;

    fn logs_subscriber(
        &self,
        account: String,
    ) -> impl Future<Output = Result<LogsSubscription<'_>, anyhow::Error>>;

    fn unsubscribe(&self) -> impl Future<Output = ()>;
}

pub struct SolanaStreamClient {
    client: PubsubClient,
    commitment: CommitmentConfig,
    unsub: Mutex<Option<UnsubFn>>,
}

impl SolanaStreamClient {
    pub async fn new(url: &str, commitment: CommitmentConfig) -> Result<Self, ClientError> {
        Ok(Self {
            client: PubsubClient::new(url)
                .await
                .map_err(|e| ClientError::ConnectionFailed(e.to_string()))?,
            commitment,
            unsub: Mutex::new(None),
        })
    }

    pub async fn shutdown(self) -> Result<(), ClientError> {
        self.client
            .shutdown()
            .await
            .map_err(|e| ClientError::ConnectionFailed(e.to_string()))
    }
}

impl SolanaStreamClientTrait for SolanaStreamClient {
    fn inner(&self) -> &PubsubClient {
        &self.client
    }

    async fn logs_subscriber(
        &self,
        account: String,
    ) -> Result<LogsSubscription<'_>, anyhow::Error> {
        let (sub, unsub) = self
            .client
            .logs_subscribe(
                RpcTransactionLogsFilter::Mentions(vec![account]),
                RpcTransactionLogsConfig {
                    commitment: Some(self.commitment),
                },
            )
            .await
            .map_err(|e| anyhow!(e.to_string()))?;
        *self.unsub.lock().await = Some(unsub);
        Ok(sub)
    }

    async fn unsubscribe(&self) {
        if let Some(unsub) = self.unsub.lock().await.take() {
            unsub().await; // consume the FnOnce
        }
    }
}
