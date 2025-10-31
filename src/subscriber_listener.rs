use std::{future::Future, str::FromStr, sync::Arc, time::Duration};

use crate::{
    config::SolanaConfig,
    models::solana_transaction::SolanaTransactionModel,
    poll_client::{SolanaRpcClient, SolanaRpcClientTrait},
    stream_client::{LogsSubscription, SolanaStreamClient, SolanaStreamClientTrait},
};

use crate::types::SolanaTransaction;
use crate::utils::upsert_and_publish;
use futures::StreamExt;
use relayer_core::{error::SubscriberError, queue::QueueTrait};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use tokio::{select, sync::Semaphore, time::timeout};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{error, info, warn};

pub trait TransactionListener {
    type Transaction;
    type Account;

    fn subscribe(
        &self,
        account: Self::Account,
    ) -> impl Future<Output = Result<LogsSubscription<'_>, anyhow::Error>>;

    fn unsubscribe(&self) -> impl Future<Output = ()>;
}

pub struct SolanaListener<STR: SolanaStreamClientTrait, SM: SolanaTransactionModel> {
    client: STR,
    transaction_model: Arc<SM>,
    queue: Arc<dyn QueueTrait>,
}

impl<STR: SolanaStreamClientTrait, SM: SolanaTransactionModel> TransactionListener
    for SolanaListener<STR, SM>
{
    type Transaction = SolanaTransaction;
    type Account = Pubkey;

    async fn subscribe(
        &self,
        account: Self::Account,
    ) -> Result<LogsSubscription<'_>, anyhow::Error> {
        self.client.logs_subscriber(account.to_string()).await
    }

    async fn unsubscribe(&self) {
        self.client.unsubscribe().await;
    }
}

impl<STR: SolanaStreamClientTrait, SM: SolanaTransactionModel> SolanaListener<STR, SM> {
    pub async fn new(
        client: STR,
        transaction_model: Arc<SM>,
        queue: Arc<dyn QueueTrait>,
    ) -> Result<Self, SubscriberError> {
        Ok(SolanaListener {
            client,
            transaction_model,
            queue,
        })
    }

    pub async fn run(
        &self,
        gas_service_account: Pubkey,
        gateway_account: Pubkey,
        its_account: Pubkey,
        config: SolanaConfig,
        cancellation_token: CancellationToken,
    ) {
        let solana_config_clone = config.clone();
        let queue_clone = Arc::clone(&self.queue);
        let transaction_model_clone = Arc::clone(&self.transaction_model);
        let cancellation_token_clone = cancellation_token.clone();

        let handle_gas_service = tokio::spawn(async move {
            Self::setup_connection_and_work(
                solana_config_clone,
                gas_service_account,
                &queue_clone,
                &transaction_model_clone,
                "gas_service",
                cancellation_token_clone,
            )
            .await;
        });

        tokio::pin!(handle_gas_service);

        let solana_config_clone = config.clone();
        let queue_clone = Arc::clone(&self.queue);
        let transaction_model_clone = Arc::clone(&self.transaction_model);
        let cancellation_token_clone = cancellation_token.clone();

        let handle_gateway = tokio::spawn(async move {
            Self::setup_connection_and_work(
                solana_config_clone,
                gateway_account,
                &queue_clone,
                &transaction_model_clone,
                "gateway",
                cancellation_token_clone,
            )
            .await;
        });

        let solana_config_clone = config.clone();
        let queue_clone = Arc::clone(&self.queue);
        let transaction_model_clone = Arc::clone(&self.transaction_model);
        let cancellation_token_clone = cancellation_token.clone();

        tokio::pin!(handle_gateway);

        let handle_its = tokio::spawn(async move {
            Self::setup_connection_and_work(
                solana_config_clone,
                its_account,
                &queue_clone,
                &transaction_model_clone,
                "ITS",
                cancellation_token_clone,
            )
            .await;
        });

        tokio::pin!(handle_its);

        tokio::select! {
            _ = &mut handle_gas_service => {
                info!("Gas service stopped");
            },
            _ = &mut handle_gateway => {
                info!("Gateway stopped");
            },
            _ = &mut handle_its => {
                info!("ITS stopped");
            },
        }

        info!("Shut down solana listener");
    }

    async fn setup_connection_and_work(
        solana_config: SolanaConfig,
        account: Pubkey,
        queue: &Arc<dyn QueueTrait>,
        transaction_model: &Arc<SM>,
        stream_name: &str,
        cancellation_token: CancellationToken,
    ) {
        loop {
            // TODO: Connection Pool
            let solana_stream_client = match SolanaStreamClient::new(
                &solana_config.solana_stream_rpc,
                solana_config.solana_commitment,
            )
            .await
            {
                Ok(solana_stream_client) => solana_stream_client,
                Err(e) => {
                    error!(
                        "Error creating solana stream client for {}: {:?}",
                        stream_name, e
                    );
                    break;
                }
            };

            let mut subscriber_stream = match solana_stream_client
                .logs_subscriber(account.to_string())
                .await
            {
                Ok(subscriber) => subscriber,
                Err(e) => {
                    error!("Error creating {} subscriber stream: {:?}", stream_name, e);
                    break;
                }
            };

            let mut should_break = false;
            let tracker = TaskTracker::new();

            select! {
                _ = cancellation_token.cancelled() => {
                    warn!("Cancellation requested; no longer awaiting subscriber_stream.next()");
                    should_break = true;
                },
                _ = Self::work(
                    &mut subscriber_stream,
                stream_name,
                queue,
                transaction_model,
                &solana_config,
                cancellation_token.clone(),
                &tracker,
                ) => {}
            }

            tracker.close();
            tracker.wait().await;

            solana_stream_client.unsubscribe().await;

            drop(subscriber_stream);

            if let Err(e) = solana_stream_client.shutdown().await {
                error!("Error shutting down solana stream client: {:?}", e);
            }

            if should_break {
                break;
            }
        }
    }

    // TODO: Create a general stream work function and separate it from poll work
    async fn work(
        subscriber_stream: &mut LogsSubscription<'_>,
        stream_name: &str,
        queue: &Arc<dyn QueueTrait>,
        transaction_model: &Arc<SM>,
        solana_config: &SolanaConfig,
        cancellation_token: CancellationToken,
        tracker: &TaskTracker,
    ) {
        let semaphore = Arc::new(Semaphore::new(
            solana_config.common_config.num_workers as usize,
        ));
        let num_workers = solana_config.common_config.num_workers as usize;

        // Set pending limit to 10×num_workers, but avoid overflow and enforce a minimum of 1 (Semaphore requires > 0)
        let pending_limit = num_workers.saturating_mul(10).max(1);

        let pending_semaphore = Arc::new(Semaphore::new(pending_limit));

        // Create a single RPC client and reuse it across spawned tasks to leverage
        // the underlying HTTP connection pooling, instead of creating a client per request.
        let solana_rpc_client = match SolanaRpcClient::new(
            &solana_config.solana_poll_rpc,
            solana_config.solana_commitment,
            3,
        ) {
            Ok(client) => Arc::new(client),
            Err(e) => {
                error!(
                    "Error creating solana rpc client for {}: {:?}",
                    stream_name, e
                );
                return;
            }
        };
        loop {
            info!("Waiting for messages from {}...", stream_name);
            // If the stream has not received any messages in 30 seconds, re-establish the connection to avoid silent failures
            select! {
                _ = cancellation_token.cancelled() => {
                    warn!("Cancellation requested; no longer awaiting subscriber_stream.next()");
                    break;
                },
                maybe_response = timeout(Duration::from_secs(30), subscriber_stream.next()) => {
                    match maybe_response {
                        Ok(Some(response)) => {
                            let signature = match Signature::from_str(&response.value.signature) {
                                Ok(signature) => signature,
                                Err(e) => {
                                    error!("Error parsing signature: {:?}", e);
                                    continue;
                                }
                            };

                            let pending_permit = match Arc::clone(&pending_semaphore).try_acquire_owned() {
                                Ok(permit) => permit,
                                Err(_) => {
                                    error!("Too many pending tasks, dropping signature: {}", signature);
                                    continue;
                                }
                            };

                            let transaction_model_clone = Arc::clone(transaction_model);
                            let queue_clone = Arc::clone(queue);
                            let stream_name_clone = stream_name.to_string();
                            let rpc_clone = Arc::clone(&solana_rpc_client);
                            let semaphore_clone = Arc::clone(&semaphore);

                            tracker.spawn(async move {
                                let _pending_permit = pending_permit;
                                let _work_permit = match semaphore_clone.acquire_owned().await {
                                    Ok(permit) => permit,
                                    Err(_) => {
                                        error!("Failed to acquire semaphore permit");
                                        return;
                                    }
                                };
                                let tx = match rpc_clone
                                .get_transaction_by_signature(signature)
                                .await
                            {
                                Ok(tx) => tx,
                                Err(e) => {
                                    error!("Error getting transaction by signature: {:?}", e);
                                    return;
                                }
                            };

                            match upsert_and_publish(&transaction_model_clone, &queue_clone, &tx, stream_name_clone.clone()).await {
                                Ok(inserted) => {
                                    if inserted {
                                        info!(
                                            "Upserted and published transaction by {} stream: {:?}",
                                            stream_name_clone, tx
                                        );
                                    }
                                }
                                Err(e) => {
                                    error!("Error upserting and publishing transaction: {:?}", e);
                                }
                            }
                            });
                        }
                        Ok(None) => {
                            warn!("Stream {} was closed", stream_name);
                            break;
                        }
                        Err(e) => {
                            warn!("Restarting {} stream: {:?}", stream_name, e);
                            break;
                        }
                    }
                }
            }
        }
    }
}
