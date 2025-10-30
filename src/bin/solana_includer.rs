use dotenv::dotenv;
use relayer_core::config::config_from_yaml;
use relayer_core::logging::setup_logging;
use relayer_core::redis::connection_manager;
use relayer_core::utils::setup_heartbeat;
use relayer_core::{database::PostgresDB, gmp_api, payload_cache::PayloadCache, queue::Queue};
use solana::config::SolanaConfig;
use solana::includer::SolanaIncluder;
use solana::redis::RedisConnection;
use sqlx::PgPool;
use std::sync::Arc;
use tokio::signal::unix::{signal, SignalKind};
use tokio_util::sync::CancellationToken;
use tracing::log::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv().ok();
    let network = std::env::var("NETWORK").expect("NETWORK must be set");
    let config: SolanaConfig = config_from_yaml(&format!("config.{network}.yaml"))?;

    let (_sentry_guard, otel_guard) = setup_logging(&config.common_config);

    let tasks_queue = Queue::new(
        &config.common_config.queue_address,
        "includer_tasks",
        config.common_config.num_workers,
    )
    .await;
    let construct_proof_queue = Queue::new(
        &config.common_config.queue_address,
        "construct_proof",
        config.common_config.num_workers,
    )
    .await;
    let redis_client = redis::Client::open(config.common_config.redis_server.clone())?;
    let redis_conn_manager = connection_manager(redis_client.clone(), None, None, None).await?;
    let redis_conn = RedisConnection::new(redis_conn_manager.clone());

    let postgres_db = PostgresDB::new(&config.common_config.postgres_url).await?;
    let payload_cache_for_includer = PayloadCache::new(postgres_db);

    let pg_pool = PgPool::connect(&config.common_config.postgres_url).await?;
    let gmp_api = gmp_api::construct_gmp_api(pg_pool.clone(), &config.common_config, true)?;

    let solana_includer = SolanaIncluder::<
        relayer_core::gmp_api::GmpApiDbAuditDecorator<
            relayer_core::gmp_api::GmpApi,
            relayer_core::models::gmp_tasks::PgGMPTasks,
            relayer_core::models::gmp_events::PgGMPEvents,
        >,
        RedisConnection,
    >::create_includer(
        config,
        gmp_api,
        redis_conn.clone(),
        payload_cache_for_includer,
        Arc::clone(&construct_proof_queue),
    )
    .await
    .map_err(|e| anyhow::anyhow!("Failed to create includer: {}", e))?;
    let mut sigint = signal(SignalKind::interrupt())?;
    let mut sigterm = signal(SignalKind::terminate())?;

    let token = CancellationToken::new();
    setup_heartbeat(
        "heartbeat:includer".to_owned(),
        redis_conn_manager,
        Some(token.clone()),
    );
    let sigint_cloned_token = token.clone();
    let sigterm_cloned_token = token.clone();
    let includer_cloned_token = token.clone();

    let handle = tokio::spawn({
        let tasks = Arc::clone(&tasks_queue);
        let token_clone = token.clone();
        async move { solana_includer.run(tasks, token_clone).await }
    });

    tokio::pin!(handle);

    tokio::select! {
        _ = sigint.recv()  => {
            sigint_cloned_token.cancel();
        },
        _ = sigterm.recv() => {
            sigterm_cloned_token.cancel();
        },
        _ = &mut handle => {
            info!("Includer stopped");
            includer_cloned_token.cancel();
        }
    }
    tasks_queue.close().await;
    Arc::clone(&construct_proof_queue).close().await;
    let _ = handle.await;

    otel_guard
        .force_flush()
        .expect("Failed to flush OTEL messages");

    Ok(())
}
