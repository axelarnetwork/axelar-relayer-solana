use dotenv::dotenv;

use relayer_core::config::config_from_yaml;
use relayer_core::logging::setup_logging;
use relayer_core::queue::Queue;
use solana::{
    config::SolanaConfig,
    models::{solana_subscriber_cursor::PostgresDB, solana_transaction::PgSolanaTransactionModel},
    poll_client::SolanaRpcClient,
    subscriber_poller::SolanaPoller,
};
use solana_sdk::signature::Signature;
use sqlx::PgPool;
use std::{str::FromStr, sync::Arc};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv().ok();
    let network = std::env::var("NETWORK").expect("NETWORK must be set");
    let config: SolanaConfig = config_from_yaml(&format!("config.{}.yaml", network))?;

    let _guard = setup_logging(&config.common_config);

    let events_queue: Arc<dyn relayer_core::queue::QueueTrait> = Queue::new(
        &config.common_config.queue_address,
        "events",
        config.common_config.num_workers,
    )
    .await;
    let postgres_db = PostgresDB::new(&config.common_config.postgres_url).await?;
    let pg_pool = PgPool::connect(&config.common_config.postgres_url).await?;

    let solana_transaction_model = PgSolanaTransactionModel::new(pg_pool.clone());

    let solana_rpc_client: SolanaRpcClient =
        SolanaRpcClient::new(&config.solana_poll_rpc, config.solana_commitment, 3)?;

    let solana_poller = SolanaPoller::new(
        solana_rpc_client,
        "recovery_solana_poller".to_string(),
        Arc::new(solana_transaction_model.clone()),
        Arc::new(postgres_db),
        Arc::clone(&events_queue),
    )
    .await?;

    let signatures = vec![
        "4eA2mB9QG984eivKj67YdVLCcZsy4NaU6Pg4zn4KXNir2Ki6oFJ7daGjM3P7dVP8td9bqY7yUyM7VEpLC8jkvLkb",
    ];

    solana_poller
        .recover_txs(
            signatures
                .into_iter()
                .map(|s| Signature::from_str(s).unwrap_or(Signature::default()))
                .collect(),
        )
        .await?;
    events_queue.close().await;

    Ok(())
}
