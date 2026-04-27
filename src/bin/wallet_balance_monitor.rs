use axelar_relayer_solana::config::SolanaConfig;
use dotenv::dotenv;
use relayer_core::config::config_from_yaml;
use relayer_core::logging::setup_logging;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::signer::Signer;
use std::time::Duration;
use tokio::time;
use tracing::{debug, error, info};

const CHECK_INTERVAL_SECS: u64 = 300;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv().ok();
    let network = std::env::var("NETWORK").expect("NETWORK must be set");
    let config: SolanaConfig = config_from_yaml(&format!("config.{network}.yaml"))?;

    let (_sentry_guard, _otel_guard) = setup_logging(&config.common_config);

    let threshold_lamports = config.min_wallet_balance_lamports;

    let pubkey = config.signing_keypair().pubkey();
    let rpc =
        RpcClient::new_with_commitment(config.solana_poll_rpc.clone(), config.solana_commitment());

    info!(
        wallet = %pubkey,
        threshold_lamports,
        interval_secs = CHECK_INTERVAL_SECS,
        "Wallet balance monitor started"
    );

    let mut interval = time::interval(Duration::from_secs(CHECK_INTERVAL_SECS));
    interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

    loop {
        interval.tick().await;
        match rpc.get_balance(&pubkey).await {
            Ok(balance) => {
                if balance < threshold_lamports {
                    error!(
                        wallet = %pubkey,
                        balance_lamports = balance,
                        threshold_lamports,
                        "Relayer wallet balance is below threshold"
                    );
                } else {
                    debug!(
                        wallet = %pubkey,
                        balance_lamports = balance,
                        "Relayer wallet balance OK"
                    );
                }
            }
            Err(e) => {
                error!(
                    wallet = %pubkey,
                    error = %e,
                    "Failed to fetch wallet balance"
                );
            }
        }
    }
}
