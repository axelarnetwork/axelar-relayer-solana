use chrono;
use dotenv::dotenv;
use redis::AsyncCommands;
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

const ALT_MANAGEMENT_INTERVAL_SECS: u64 = 30;
const ALT_LIFETIME_SECONDS: i64 = 600; // 10 minutes
const ALT_CLOSURE_DELAY_SLOTS: u64 = 512; // Minimum slots after deactivation before closure (still needed for closing)

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv().ok();
    let network = std::env::var("NETWORK").expect("NETWORK must be set");
    let config: SolanaConfig = config_from_yaml(&format!("config.{network}.yaml"))?;

    let (_sentry_guard, _otel_guard) = setup_logging(&config.common_config);

    let redis_client = redis::Client::open(config.common_config.redis_server.clone())?;
    let redis_conn_manager = connection_manager(redis_client.clone(), None, None, None).await?;
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

    for (message_id, alt_pubkey, created_at) in alt_keys {
        match process_alt(
            &message_id,
            alt_pubkey,
            created_at,
            current_timestamp,
            includer_client,
            keypair,
            authority_pubkey,
        )
        .await
        {
            Ok(should_remove) => {
                if should_remove {
                    // Remove from Redis after successful closure
                    let mut conn = redis_conn.inner().clone();
                    let key = format!("ALT:{}", message_id);
                    let _: () = AsyncCommands::del(&mut conn, key).await.map_err(|e| {
                        anyhow::anyhow!("Failed to delete ALT key from Redis: {}", e)
                    })?;
                    info!("Removed ALT {} from Redis after closure", alt_pubkey);
                }
            }
            Err(e) => {
                warn!(
                    "Failed to process ALT {} ({}): {}",
                    alt_pubkey, message_id, e
                );
            }
        }
    }

    Ok(())
}

async fn process_alt(
    _message_id: &str,
    alt_pubkey: Pubkey,
    created_at: i64,
    current_timestamp: i64,
    includer_client: &IncluderClient,
    keypair: &solana_sdk::signer::keypair::Keypair,
    authority_pubkey: Pubkey,
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

    // Check if ALT is already deactivated
    if deactivation_slot != 0 {
        // ALT is deactivated, check if enough slots have passed for closure
        // Get current slot to check closure delay
        let current_slot = includer_client
            .inner()
            .get_slot()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get current slot: {}", e))?;

        let slots_since_deactivation = current_slot.saturating_sub(deactivation_slot);

        if slots_since_deactivation >= ALT_CLOSURE_DELAY_SLOTS {
            info!(
                "Closing ALT {} (deactivated at slot {}, {} slots ago)",
                alt_pubkey, deactivation_slot, slots_since_deactivation
            );

            return close_alt(alt_pubkey, includer_client, keypair, authority_pubkey).await;
        } else {
            info!(
                "ALT {} is deactivated but not ready for closure ({} slots remaining)",
                alt_pubkey,
                ALT_CLOSURE_DELAY_SLOTS - slots_since_deactivation
            );
            return Ok(false);
        }
    }

    // ALT is still active and old enough, deactivate it
    info!(
        "ALT {} is old enough to deactivate (created {} seconds ago)",
        alt_pubkey, seconds_since_creation
    );
    deactivate_alt(alt_pubkey, includer_client, keypair, authority_pubkey).await?;

    Ok(false)
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
        Err(e) => Err(anyhow::anyhow!(
            "Failed to deactivate ALT {}: {}",
            alt_pubkey,
            e
        )),
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
