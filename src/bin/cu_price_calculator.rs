use dotenv::dotenv;
use relayer_core::config::config_from_yaml;
use relayer_core::logging::setup_logging;
use relayer_core::redis::connection_manager;
use solana::fees_client::{FeesClient, FeesClientTrait as _};
use solana::includer_client::IncluderClient;
use solana::redis::RedisConnection;
use solana::{config::SolanaConfig, redis::RedisConnectionTrait as _};
use std::time::Duration;
use tokio::time;
use tracing::{debug, error, info};

const CU_PRICE_CALCULATION_INTERVAL: u64 = 2;
const CU_PRICE_PERCENTILE: u64 = 50;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv().ok();
    let network = std::env::var("NETWORK").expect("NETWORK must be set");
    let config: SolanaConfig = config_from_yaml(&format!("config.{network}.yaml"))?;

    let (_sentry_guard, _otel_guard) = setup_logging(&config.common_config);

    let redis_client = redis::Client::open(config.common_config.redis_server.clone())?;
    let redis_conn_manager =
        connection_manager(redis_client.clone(), None, None, None, None).await?;
    let redis_conn = RedisConnection::new(redis_conn_manager.clone());

    let includer_client = IncluderClient::new(&config.solana_poll_rpc, config.solana_commitment, 3)
        .map_err(|e| anyhow::anyhow!("Failed to create includer client: {}", e))?;

    let fees_client = FeesClient::new(includer_client, 5)
        .map_err(|e| anyhow::anyhow!("Failed to create fees client: {}", e))?;

    let mut interval = time::interval(Duration::from_secs(CU_PRICE_CALCULATION_INTERVAL));
    interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

    info!("CU price calculator started");

    loop {
        match calculate_and_write_cu_price(&redis_conn, &fees_client).await {
            Ok(cu_price) => {
                debug!("CU price calculation completed successfully: {}", cu_price);
            }
            Err(e) => {
                error!("Error during CU price calculation: {}", e);
            }
        }
        interval.tick().await;
    }
}

async fn calculate_and_write_cu_price(
    redis_conn: &RedisConnection,
    fees_client: &FeesClient<IncluderClient>,
) -> anyhow::Result<u64> {
    let cu_price = fees_client
        .get_recent_prioritization_fees(CU_PRICE_PERCENTILE)
        .await;
    redis_conn.set_cu_price(cu_price).await?;
    Ok(cu_price)
}
