use dotenv::dotenv;
use relayer_core::config::config_from_yaml;
use relayer_core::logging::setup_logging;
use relayer_core::redis::connection_manager;
use solana::config::SolanaConfig;
use solana::includer_client::{IncluderClient, IncluderClientTrait};
use solana::redis::{RedisConnection, RedisConnectionTrait};
use solana::versioned_transaction::SolanaTransactionType;
use solana_sdk::address_lookup_table::instruction::{close_lookup_table, deactivate_lookup_table};
use solana_sdk::address_lookup_table::state::AddressLookupTable;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signer::Signer as _;
use solana_sdk::transaction::Transaction;
use std::time::Duration;
use tokio::time;
use tracing::{error, info, warn};

const MAX_RETRY_ATTEMPTS: u32 = 3;
const ALT_MANAGEMENT_INTERVAL_SECS: u64 = 30;
const ALT_LIFETIME_SECONDS: i64 = 600; // 10 minutes

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

    let keypair = config.signing_keypair();
    let authority_pubkey = keypair.pubkey();

    info!("ALT Manager started. Authority: {}", authority_pubkey);

    let mut interval = time::interval(Duration::from_secs(ALT_MANAGEMENT_INTERVAL_SECS));
    interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

    loop {
        interval.tick().await;

        match manage_alts(&redis_conn, &includer_client, &keypair, authority_pubkey).await {
            Ok(_) => {
                info!("ALT management cycle completed successfully");
            }
            Err(e) => {
                error!("Error during ALT management cycle: {}", e);
            }
        }
    }
}

async fn manage_alts(
    redis_conn: &RedisConnection,
    includer_client: &IncluderClient,
    keypair: &solana_sdk::signer::keypair::Keypair,
    authority_pubkey: Pubkey,
) -> anyhow::Result<()> {
    // Get all ALT keys from Redis
    let alt_keys = redis_conn
        .get_all_alt_keys()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get ALT keys from Redis: {}", e))?;

    info!("Found {} ALT(s) in Redis", alt_keys.len());

    // Get current timestamp
    let current_timestamp = chrono::Utc::now().timestamp();

    for (message_id, alt_pubkey, created_at, retry_count) in alt_keys {
        match process_alt(
            &message_id,
            alt_pubkey,
            created_at,
            current_timestamp,
            includer_client,
            keypair,
            authority_pubkey,
            retry_count,
        )
        .await
        {
            Ok(should_remove) => {
                if should_remove {
                    // Remove from Redis after successful closure
                    if let Err(e) = redis_conn
                        .remove_alt_key_from_redis(message_id.clone())
                        .await
                    {
                        error!(
                            "Failed to remove ALT {} from Redis after closure: {}",
                            alt_pubkey, e
                        );
                    } else {
                        info!("Removed ALT {} from Redis after closure", alt_pubkey);
                    }
                }
            }
            Err(e) => {
                let new_retry_count = retry_count + 1;
                if new_retry_count >= MAX_RETRY_ATTEMPTS {
                    // Exceeded max retries, remove from Redis and log error
                    error!(
                        "Failed to process ALT {} ({}): {} (retry_count: {}). Removing from Redis.",
                        alt_pubkey, message_id, e, retry_count
                    );
                    if let Err(redis_err) = redis_conn
                        .remove_alt_key_from_redis(message_id.clone())
                        .await
                    {
                        error!(
                            "Failed to remove ALT {} from Redis: {}",
                            alt_pubkey, redis_err
                        );
                    }
                } else {
                    // Update retry count in Redis
                    if let Err(redis_err) = redis_conn
                        .update_alt_retry_count(message_id.clone(), new_retry_count)
                        .await
                    {
                        error!(
                            "Failed to update retry count for ALT {} ({}): {}",
                            alt_pubkey, message_id, redis_err
                        );
                    } else {
                        warn!(
                            "Failed to process ALT {} ({}): {} (retry_count: {}/{})",
                            alt_pubkey, message_id, e, new_retry_count, MAX_RETRY_ATTEMPTS
                        );
                    }
                }
            }
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn process_alt(
    _message_id: &str,
    alt_pubkey: Pubkey,
    created_at: i64,
    current_timestamp: i64,
    includer_client: &IncluderClient,
    keypair: &solana_sdk::signer::keypair::Keypair,
    authority_pubkey: Pubkey,
    _retry_count: u32,
) -> anyhow::Result<bool> {
    // Check if 10 minutes have passed since creation
    let seconds_since_creation = current_timestamp - created_at;

    if seconds_since_creation < ALT_LIFETIME_SECONDS {
        info!(
            "ALT {} is not old enough yet (created {} seconds ago, {} seconds remaining)",
            alt_pubkey,
            seconds_since_creation,
            ALT_LIFETIME_SECONDS - seconds_since_creation
        );
        return Ok(false);
    }

    // Get ALT account data to check if it's already deactivated
    let alt_account_data = includer_client
        .inner()
        .get_account_data(&alt_pubkey)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get ALT account data: {}", e))?;

    let alt_state = AddressLookupTable::deserialize(&alt_account_data)
        .map_err(|e| anyhow::anyhow!("Failed to deserialize ALT state: {}", e))?;

    // AddressLookupTable fields - deactivation_slot is u64 (0 means not deactivated)
    let deactivation_slot = alt_state.meta.deactivation_slot;

    // If ALT is already deactivated, close it immediately
    if deactivation_slot != 0 {
        info!(
            "ALT {} is already deactivated (deactivated at slot {}), closing it",
            alt_pubkey, deactivation_slot
        );
        return close_alt(alt_pubkey, includer_client, keypair, authority_pubkey).await;
    }

    // ALT is still active and old enough (>10 minutes), deactivate it first
    info!(
        "ALT {} is old enough to deactivate and close (created {} seconds ago)",
        alt_pubkey, seconds_since_creation
    );
    deactivate_alt(alt_pubkey, includer_client, keypair, authority_pubkey).await?;

    // After successful deactivation, close it immediately
    close_alt(alt_pubkey, includer_client, keypair, authority_pubkey).await
}

async fn deactivate_alt(
    alt_pubkey: Pubkey,
    includer_client: &IncluderClient,
    keypair: &solana_sdk::signer::keypair::Keypair,
    authority_pubkey: Pubkey,
) -> anyhow::Result<()> {
    info!("Deactivating ALT: {}", alt_pubkey);

    let deactivate_ix = deactivate_lookup_table(alt_pubkey, authority_pubkey);

    let recent_blockhash = includer_client
        .inner()
        .get_latest_blockhash()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get recent blockhash: {}", e))?;

    let transaction = Transaction::new_signed_with_payer(
        &[deactivate_ix],
        Some(&keypair.pubkey()),
        &[keypair],
        recent_blockhash,
    );

    match includer_client
        .send_transaction(SolanaTransactionType::Legacy(transaction))
        .await
    {
        Ok((signature, _)) => {
            info!("Successfully deactivated ALT {}: {}", alt_pubkey, signature);
            Ok(())
        }
        Err(e) => {
            // Check if ALT was already deactivated
            let error_str = e.to_string();
            if error_str.contains("already deactivated")
                || error_str.contains("AddressLookupTableError")
            {
                warn!(
                    "ALT {} appears to be already deactivated: {}",
                    alt_pubkey, error_str
                );
                Ok(()) // Continue to closure
            } else {
                Err(anyhow::anyhow!(
                    "Failed to deactivate ALT {}: {}",
                    alt_pubkey,
                    e
                ))
            }
        }
    }
}

async fn close_alt(
    alt_pubkey: Pubkey,
    includer_client: &IncluderClient,
    keypair: &solana_sdk::signer::keypair::Keypair,
    authority_pubkey: Pubkey,
) -> anyhow::Result<bool> {
    info!("Closing ALT: {}", alt_pubkey);

    let recipient_pubkey = authority_pubkey; // Close to the authority wallet
    let close_ix = close_lookup_table(alt_pubkey, authority_pubkey, recipient_pubkey);

    let recent_blockhash = includer_client
        .inner()
        .get_latest_blockhash()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get recent blockhash: {}", e))?;

    let transaction = Transaction::new_signed_with_payer(
        &[close_ix],
        Some(&keypair.pubkey()),
        &[keypair],
        recent_blockhash,
    );

    match includer_client
        .send_transaction(SolanaTransactionType::Legacy(transaction))
        .await
    {
        Ok((signature, _)) => {
            info!("Successfully closed ALT {}: {}", alt_pubkey, signature);
            Ok(true) // Return true to indicate removal from Redis
        }
        Err(e) => {
            // Check if ALT was already closed
            if e.to_string().contains("already closed") || e.to_string().contains("AccountNotFound")
            {
                warn!("ALT {} appears to be already closed", alt_pubkey);
                Ok(true) // Still remove from Redis
            } else {
                Err(anyhow::anyhow!("Failed to close ALT {}: {}", alt_pubkey, e))
            }
        }
    }
}
