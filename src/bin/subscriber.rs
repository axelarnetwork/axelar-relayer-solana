use dotenv::dotenv;

use relayer_core::config::config_from_yaml;
use relayer_core::logging::setup_logging;
use relayer_core::redis::connection_manager;
use relayer_core::{queue::Queue, utils::setup_heartbeat};
use solana::{
    config::SolanaConfig,
    models::{solana_subscriber_cursor::PostgresDB, solana_transaction::PgSolanaTransactionModel},
    poll_client::SolanaRpcClient,
    stream_client::SolanaStreamClient,
    subscriber_listener::SolanaListener,
    subscriber_poller::SolanaPoller,
};
use solana_axelar_gas_service::ID as GAS_SERVICE_ID;
use solana_axelar_gateway::ID as GATEWAY_ID;
use solana_axelar_its::ID as ITS_ID;
use sqlx::PgPool;
use std::sync::Arc;
use tokio::signal::unix::{signal, SignalKind};
use tokio_util::sync::CancellationToken;
use tracing::info;

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

    let mut sigint = signal(SignalKind::interrupt())?;
    let mut sigterm = signal(SignalKind::terminate())?;

    let solana_stream_client =
        SolanaStreamClient::new(&config.solana_stream_rpc, config.solana_commitment).await?;

    let solana_rpc_client: SolanaRpcClient =
        SolanaRpcClient::new(&config.solana_poll_rpc, config.solana_commitment, 3)?;

    let solana_poller = SolanaPoller::new(
        solana_rpc_client,
        "solana_poller".to_string(),
        Arc::new(solana_transaction_model.clone()),
        Arc::new(postgres_db),
        Arc::clone(&events_queue),
    )
    .await?;

    let solana_listener = SolanaListener::new(
        solana_stream_client,
        Arc::new(solana_transaction_model),
        Arc::clone(&events_queue),
    )
    .await?;

    let redis_client = redis::Client::open(config.common_config.redis_server.clone())?;
    let redis_conn = connection_manager(redis_client, None, None, None, None).await?;

    let cancellation_token = CancellationToken::new();
    setup_heartbeat(
        "heartbeat:subscriber".to_owned(),
        redis_conn,
        Some(cancellation_token.clone()),
    );
    let sigint_cloned_token = cancellation_token.clone();
    let sigterm_cloned_token = cancellation_token.clone();
    let subscriber_cloned_token = cancellation_token.clone();
    let listener_cloned_token = cancellation_token.clone();
    let poller_cloned_token = cancellation_token.clone();

    let gas_service_account = GAS_SERVICE_ID;
    let gateway_account = GATEWAY_ID;
    let its_account = ITS_ID;

    let mut handle_poller = tokio::spawn(async move {
        solana_poller
            .run(
                gas_service_account,
                gateway_account,
                its_account,
                poller_cloned_token,
            )
            .await;
    });

    let mut handle_listener = tokio::spawn(async move {
        solana_listener
            .run(
                gas_service_account,
                gateway_account,
                its_account,
                config,
                listener_cloned_token,
            )
            .await;
    });

    tokio::select! {
        _ = sigint.recv()  => {
            sigint_cloned_token.cancel();
        },
        _ = sigterm.recv() => {
            sigterm_cloned_token.cancel();
        },
        _ = &mut handle_poller => {
            info!("Poller stopped");
            subscriber_cloned_token.cancel();
        },
        _ = &mut handle_listener => {
            info!("Listener stopped");
            subscriber_cloned_token.cancel();
        }
    }

    events_queue.close().await;

    if !handle_poller.is_finished() {
        let _ = handle_poller.await;
    }
    if !handle_listener.is_finished() {
        let _ = handle_listener.await;
    }

    Ok(())
}
