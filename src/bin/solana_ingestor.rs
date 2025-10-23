use dotenv::dotenv;
use relayer_core::config::config_from_yaml;
use relayer_core::logging::setup_logging;
use relayer_core::logging_ctx_cache::RedisLoggingCtxCache;
use relayer_core::queue::{Queue, QueueTrait};
use relayer_core::redis::connection_manager;
use relayer_core::{gmp_api, ingestor};
use solana::config::SolanaConfig;
use solana::ingestor::SolanaIngestor;
use solana::solana_transaction::PgSolanaTransactionModel;
use solana_sdk::pubkey::Pubkey;
use solana_transaction_parser::parser::TransactionParser;
use sqlx::PgPool;
use std::str::FromStr;
use std::sync::Arc;
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv().ok();
    let network = std::env::var("NETWORK").expect("NETWORK must be set");
    let config: SolanaConfig = config_from_yaml(&format!("config.{}.yaml", network))?;

    let _guard = setup_logging(&config.common_config);

    let tasks_queue: Arc<dyn QueueTrait> = Queue::new(
        &config.common_config.queue_address,
        "ingestor_tasks",
        config.common_config.num_workers,
    )
    .await;
    let events_queue: Arc<dyn QueueTrait> = Queue::new(
        &config.common_config.queue_address,
        "events",
        config.common_config.num_workers,
    )
    .await;

    let pg_pool = PgPool::connect(&config.common_config.postgres_url).await?;

    let gmp_api = gmp_api::construct_gmp_api(pg_pool.clone(), &config.common_config, true)?;

    let mut our_addresses = vec![];
    // for wallet in config.wallets {
    //     our_addresses.push(Pubkey::from_str(&wallet.public_key)?);
    // }
    let gateway = Pubkey::from_str(&config.solana_gateway)?;
    let gas_service = Pubkey::from_str(&config.solana_gas_service)?;
    our_addresses.push(gateway);
    our_addresses.push(gas_service);

    let parser = TransactionParser::new(
        config.common_config.chain_name,
        Pubkey::from_str(&config.solana_gas_service)?,
        Pubkey::from_str(&config.solana_gateway)?,
        Pubkey::from_str(&config.solana_its)?,
    );

    let redis_client = redis::Client::open(config.common_config.redis_server.clone())?;
    let redis_conn = connection_manager(redis_client, None, None, None).await?;

    let solana_transaction_model = PgSolanaTransactionModel::new(pg_pool.clone());
    let solana_ingestor = SolanaIngestor::new(parser, solana_transaction_model, redis_conn.clone());

    let logging_ctx_cache = RedisLoggingCtxCache::new(redis_conn.clone());

    ingestor::run_ingestor(
        &tasks_queue,
        &events_queue,
        gmp_api,
        redis_conn,
        Arc::new(logging_ctx_cache),
        Arc::new(solana_ingestor),
    )
    .await?;

    Ok(())
}
