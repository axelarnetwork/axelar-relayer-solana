use dotenv::dotenv;
use relayer_core::config::config_from_yaml;
use relayer_core::logging::setup_logging;
use relayer_core::redis::connection_manager;
use solana::config::SolanaConfig;
use solana::includer_client::{IncluderClient, IncluderClientTrait};
use solana::redis::{RedisConnection, RedisConnectionTrait};
use solana::transaction_type::SolanaTransactionType;
use solana_sdk::address_lookup_table::instruction::{close_lookup_table, deactivate_lookup_table};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signer::Signer as _;
use solana_sdk::transaction::Transaction;
use std::time::Duration;
use tokio::time;
use tracing::{debug, error, info, warn};

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

#[allow(clippy::type_complexity)]
async fn manage_alts(
    redis_conn: &RedisConnection,
    includer_client: &IncluderClient,
    keypair: &solana_sdk::signer::keypair::Keypair,
    authority_pubkey: Pubkey,
) -> anyhow::Result<()> {
    let alt_keys = redis_conn
        .get_all_alt_keys()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get ALT keys from Redis: {}", e))?;

    debug!("Found {} ALT(s) in Redis", alt_keys.len());

    let current_timestamp = chrono::Utc::now().timestamp();

    let (active_alts, deactivated_alts): (
        Vec<(String, Pubkey, i64, u32, bool)>,
        Vec<(String, Pubkey, i64, u32, bool)>,
    ) = alt_keys
        .into_iter()
        .partition(|(_, _, _, _, active)| *active);

    debug!(
        "Processing {} active ALT(s) and {} deactivated ALT(s)",
        active_alts.len(),
        deactivated_alts.len()
    );

    process_active_alts(
        active_alts,
        current_timestamp,
        includer_client,
        keypair,
        authority_pubkey,
        redis_conn,
    )
    .await?;

    process_deactivated_alts(
        deactivated_alts,
        current_timestamp,
        includer_client,
        keypair,
        authority_pubkey,
        redis_conn,
    )
    .await?;

    Ok(())
}

async fn process_active_alts(
    active_alts: Vec<(String, Pubkey, i64, u32, bool)>,
    current_timestamp: i64,
    includer_client: &IncluderClient,
    keypair: &solana_sdk::signer::keypair::Keypair,
    authority_pubkey: Pubkey,
    redis_conn: &RedisConnection,
) -> anyhow::Result<()> {
    for (message_id, alt_pubkey, created_at, retry_count, _) in active_alts {
        match process_alt(
            &message_id,
            alt_pubkey,
            created_at,
            current_timestamp,
            true, // active
            includer_client,
            keypair,
            authority_pubkey,
            redis_conn,
        )
        .await
        {
            Ok(should_remove) => {
                if should_remove {
                    if let Err(e) = redis_conn.remove_alt_key(message_id.clone()).await {
                        error!(
                            "Failed to remove ALT {} from Redis after closure: {}",
                            alt_pubkey, e
                        );
                    } else {
                        debug!("Removed ALT {} from Redis after closure", alt_pubkey);
                    }
                }
            }
            Err(e) => {
                handle_alt_processing_error(message_id, alt_pubkey, retry_count, e, redis_conn)
                    .await?;
            }
        }
    }
    Ok(())
}

async fn process_deactivated_alts(
    deactivated_alts: Vec<(String, Pubkey, i64, u32, bool)>,
    current_timestamp: i64,
    includer_client: &IncluderClient,
    keypair: &solana_sdk::signer::keypair::Keypair,
    authority_pubkey: Pubkey,
    redis_conn: &RedisConnection,
) -> anyhow::Result<()> {
    for (message_id, alt_pubkey, created_at, retry_count, _) in deactivated_alts {
        match process_alt(
            &message_id,
            alt_pubkey,
            created_at,
            current_timestamp,
            false, // inactive
            includer_client,
            keypair,
            authority_pubkey,
            redis_conn,
        )
        .await
        {
            Ok(should_remove) => {
                if should_remove {
                    if let Err(e) = redis_conn.remove_alt_key(message_id.clone()).await {
                        error!(
                            "Failed to remove ALT {} from Redis after closure: {}",
                            alt_pubkey, e
                        );
                    } else {
                        debug!("Removed ALT {} from Redis after closure", alt_pubkey);
                    }
                }
            }
            Err(e) => {
                handle_alt_processing_error(message_id, alt_pubkey, retry_count, e, redis_conn)
                    .await?;
            }
        }
    }
    Ok(())
}

async fn handle_alt_processing_error(
    message_id: String,
    alt_pubkey: Pubkey,
    retry_count: u32,
    error: anyhow::Error,
    redis_conn: &RedisConnection,
) -> anyhow::Result<()> {
    let new_retry_count = retry_count + 1;
    if new_retry_count >= MAX_RETRY_ATTEMPTS {
        error!(
            "Failed to process ALT {} ({}): {} (retry_count: {}). Removing from Redis.",
            alt_pubkey, message_id, error, retry_count
        );

        // Set ALT as failed and remove from Redis
        if let Err(redis_err) = redis_conn
            .set_alt_failed(message_id.clone(), alt_pubkey)
            .await
        {
            error!(
                "Failed to set failed ALT {} in Redis: {}",
                alt_pubkey, redis_err
            );
        }

        if let Err(redis_err) = redis_conn.remove_alt_key(message_id.clone()).await {
            error!(
                "Failed to remove ALT {} from Redis: {}",
                alt_pubkey, redis_err
            );
        }
    } else if let Err(redis_err) = redis_conn
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
            alt_pubkey, message_id, error, new_retry_count, MAX_RETRY_ATTEMPTS
        );
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn process_alt(
    message_id: &str,
    alt_pubkey: Pubkey,
    created_at: i64,
    current_timestamp: i64,
    active: bool,
    includer_client: &IncluderClient,
    keypair: &solana_sdk::signer::keypair::Keypair,
    authority_pubkey: Pubkey,
    redis_conn: &RedisConnection,
) -> anyhow::Result<bool> {
    let seconds_since_creation = current_timestamp - created_at;

    if active {
        // ALT is active, check if it's time to deactivate (after 10 minutes)
        if seconds_since_creation >= ALT_LIFETIME_SECONDS {
            debug!(
                "ALT {} is old enough to deactivate (created {} seconds ago)",
                alt_pubkey, seconds_since_creation
            );
            deactivate_alt(alt_pubkey, includer_client, keypair, authority_pubkey).await?;

            // Set active to false in Redis
            redis_conn.set_alt_inactive(message_id.to_string()).await?;

            debug!(
                "ALT {} has been deactivated and marked as inactive in Redis",
                alt_pubkey
            );
            Ok(false) // Don't remove from Redis yet
        } else {
            debug!(
                "ALT {} is not old enough to deactivate yet (created {} seconds ago, {} seconds remaining)",
                alt_pubkey,
                seconds_since_creation,
                ALT_LIFETIME_SECONDS - seconds_since_creation
            );
            Ok(false) // Leave in Redis
        }
    } else {
        // ALT is inactive (deactivated), check if it's time to close (after ALT_LIFETIME_SECONDS since deactivation)
        // Note: created_at timestamp is updated to "now" when we set active to false
        if seconds_since_creation >= ALT_LIFETIME_SECONDS {
            debug!(
                "ALT {} is old enough to close (inactive for {} seconds)",
                alt_pubkey, seconds_since_creation
            );
            close_alt(
                message_id,
                alt_pubkey,
                includer_client,
                keypair,
                authority_pubkey,
                redis_conn,
                0,
            )
            .await
        } else {
            debug!(
                "ALT {} is deactivated but not old enough to close yet (inactive for {} seconds, {} seconds remaining)",
                alt_pubkey,
                seconds_since_creation,
                ALT_LIFETIME_SECONDS - seconds_since_creation
            );
            Ok(false) // Leave in Redis
        }
    }
}

async fn deactivate_alt(
    alt_pubkey: Pubkey,
    includer_client: &IncluderClient,
    keypair: &solana_sdk::signer::keypair::Keypair,
    authority_pubkey: Pubkey,
) -> anyhow::Result<()> {
    debug!("Deactivating ALT: {}", alt_pubkey);

    let deactivate_ix = deactivate_lookup_table(alt_pubkey, authority_pubkey);

    let recent_blockhash = includer_client
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
        Err(e) => Err(anyhow::anyhow!(
            "Failed to deactivate ALT {}: {}",
            alt_pubkey,
            e
        )),
    }
}

async fn close_alt(
    message_id: &str,
    alt_pubkey: Pubkey,
    includer_client: &IncluderClient,
    keypair: &solana_sdk::signer::keypair::Keypair,
    authority_pubkey: Pubkey,
    redis_conn: &RedisConnection,
    close_attempts: u32,
) -> anyhow::Result<bool> {
    debug!("Closing ALT: {}", alt_pubkey);

    let recipient_pubkey = authority_pubkey; // Close to the authority wallet
    let close_ix = close_lookup_table(alt_pubkey, authority_pubkey, recipient_pubkey);

    let recent_blockhash = includer_client
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
            Ok(true)
        }
        Err(e) => {
            // Add a custom retry logic that does not go through redis for this case
            // because we don't want to go through the deactivate flow again
            // if it fails after MAX_RETRY_ATTEMPTS, we store it as FAILED:ALT:{message_id} and remove from redis
            if close_attempts >= MAX_RETRY_ATTEMPTS {
                error!("Failed to close ALT {} ({}): {}", alt_pubkey, message_id, e);

                if let Err(redis_err) = redis_conn
                    .set_alt_failed(message_id.to_string(), alt_pubkey)
                    .await
                {
                    error!(
                        "Failed to set failed ALT {} in Redis: {}",
                        alt_pubkey, redis_err
                    );
                }

                Ok(true) // Return true to indicate removal from Redis
            } else {
                tokio::time::sleep(Duration::from_secs(1)).await;
                Box::pin(close_alt(
                    message_id,
                    alt_pubkey,
                    includer_client,
                    keypair,
                    authority_pubkey,
                    redis_conn,
                    close_attempts + 1,
                ))
                .await
            }
        }
    }
}
