use dotenv::dotenv;
use futures::TryFutureExt;
use relayer_core::config::config_from_yaml;
use relayer_core::logging::setup_logging;
use relayer_core::redis::connection_manager;
use solana::config::SolanaConfig;
use solana::includer_client::{IncluderClient, IncluderClientTrait};
use solana::redis::{RedisConnection, RedisConnectionTrait};
use solana::transaction_type::SolanaTransactionType;
use solana::utils::keypair_from_base58_string;
use solana_sdk::address_lookup_table::instruction::{close_lookup_table, deactivate_lookup_table};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Keypair;
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

    info!("ALT Manager started. Authority: {}", keypair.pubkey());

    let mut interval = time::interval(Duration::from_secs(ALT_MANAGEMENT_INTERVAL_SECS));
    interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

    loop {
        interval.tick().await;

        match manage_alts(&redis_conn, &includer_client, &keypair).await {
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
) -> anyhow::Result<()> {
    let alt_keys = redis_conn
        .get_all_alt_keys()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get ALT keys from Redis: {}", e))?;

    debug!("Found {} ALT(s) in Redis", alt_keys.len());

    let (active_alts, deactivated_alts): (
        Vec<(String, Pubkey, String, i64, u32, bool)>,
        Vec<(String, Pubkey, String, i64, u32, bool)>,
    ) = alt_keys
        .into_iter()
        .partition(|(_, _, _, _, _, active)| *active);

    debug!(
        "Processing {} active ALT(s) and {} deactivated ALT(s)",
        active_alts.len(),
        deactivated_alts.len()
    );

    process_active_alts(active_alts, includer_client, keypair, redis_conn).await?;

    process_deactivated_alts(deactivated_alts, includer_client, keypair, redis_conn).await?;

    Ok(())
}

async fn process_active_alts(
    active_alts: Vec<(String, Pubkey, String, i64, u32, bool)>,
    includer_client: &IncluderClient,
    keypair: &solana_sdk::signer::keypair::Keypair,
    redis_conn: &RedisConnection,
) -> anyhow::Result<()> {
    let current_timestamp = chrono::Utc::now().timestamp();

    for (message_id, alt_pubkey, authority_keypair_str, created_at, retry_count, _) in active_alts {
        let authority_keypair = match keypair_from_base58_string(&authority_keypair_str) {
            Ok(keypair) => keypair,
            Err(e) => {
                error!(
                    "Failed to parse authority keypair for message id: {}: {}",
                    message_id, e
                );
                continue;
            }
        };

        let seconds_since_creation = current_timestamp - created_at;
        if seconds_since_creation >= ALT_LIFETIME_SECONDS {
            if let Err(e) = deactivate_alt(alt_pubkey, includer_client, keypair, &authority_keypair)
                .and_then(|_| {
                    redis_conn
                        .set_alt_inactive(message_id.to_string())
                        .map_err(|e| {
                            anyhow::anyhow!("Failed to set ALT as inactive in Redis: {}", e)
                        })
                })
                .await
            {
                handle_alt_processing_error(message_id, alt_pubkey, retry_count, e, redis_conn)
                    .await?;
            } else {
                debug!(
                    "ALT {} has been deactivated and marked as inactive in Redis",
                    alt_pubkey
                );
            }
        }
    }
    Ok(())
}

async fn process_deactivated_alts(
    deactivated_alts: Vec<(String, Pubkey, String, i64, u32, bool)>,
    includer_client: &IncluderClient,
    keypair: &solana_sdk::signer::keypair::Keypair,
    redis_conn: &RedisConnection,
) -> anyhow::Result<()> {
    let current_timestamp = chrono::Utc::now().timestamp();

    for (message_id, alt_pubkey, authority_keypair_str, deactivated_at, retry_count, _) in
        deactivated_alts
    {
        let authority_keypair = keypair_from_base58_string(&authority_keypair_str)?;
        let seconds_since_creation = current_timestamp - deactivated_at;
        if seconds_since_creation >= ALT_LIFETIME_SECONDS {
            debug!(
                "ALT {} is old enough to close (inactive for {} seconds)",
                alt_pubkey, seconds_since_creation
            );
            if let Err(e) = close_alt(
                &message_id,
                alt_pubkey,
                includer_client,
                keypair,
                &authority_keypair,
                redis_conn,
                0,
            )
            .await
            {
                handle_alt_processing_error(message_id, alt_pubkey, retry_count, e, redis_conn)
                    .await?;
            } else {
                debug!("ALT {} has been closed successfully.", alt_pubkey);
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
            .remove_and_set_failed_alt_key(message_id.clone(), alt_pubkey)
            .await
        {
            error!(
                "Failed to remove and set failed ALT {} in Redis: {}",
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

async fn deactivate_alt(
    alt_pubkey: Pubkey,
    includer_client: &IncluderClient,
    keypair: &solana_sdk::signer::keypair::Keypair,
    authority_keypair: &Keypair,
) -> anyhow::Result<()> {
    debug!("Deactivating ALT: {}", alt_pubkey);

    let deactivate_ix = deactivate_lookup_table(alt_pubkey, authority_keypair.pubkey());

    let recent_blockhash = includer_client
        .get_latest_blockhash()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get recent blockhash: {}", e))?;

    let transaction = Transaction::new_signed_with_payer(
        &[deactivate_ix],
        Some(&keypair.pubkey()),
        &[keypair, authority_keypair],
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
    authority_keypair: &Keypair,
    redis_conn: &RedisConnection,
    close_attempts: u32,
) -> anyhow::Result<()> {
    debug!("Closing ALT: {}", alt_pubkey);

    let recipient_pubkey = authority_keypair.pubkey(); // Close to the authority wallet
    let close_ix = close_lookup_table(alt_pubkey, authority_keypair.pubkey(), recipient_pubkey);

    let recent_blockhash = includer_client
        .get_latest_blockhash()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get recent blockhash: {}", e))?;

    let transaction = Transaction::new_signed_with_payer(
        &[close_ix],
        Some(&keypair.pubkey()),
        &[keypair, authority_keypair],
        recent_blockhash,
    );

    match includer_client
        .send_transaction(SolanaTransactionType::Legacy(transaction))
        .await
    {
        Ok((signature, _)) => {
            info!("Successfully closed ALT {}: {}", alt_pubkey, signature);
            redis_conn.remove_alt_key(message_id.to_string()).await?;
            Ok(())
        }
        Err(e) => {
            // Add a custom retry logic that does not go through redis for this case
            // because we don't want to go through the deactivate flow again
            // if it fails after MAX_RETRY_ATTEMPTS, we store it as FAILED:ALT:{message_id} and remove from redis
            if close_attempts >= MAX_RETRY_ATTEMPTS {
                error!("Failed to close ALT {} ({}): {}", alt_pubkey, message_id, e);

                if let Err(redis_err) = redis_conn
                    .remove_and_set_failed_alt_key(message_id.to_string(), alt_pubkey)
                    .await
                {
                    error!(
                        "Failed to remove and set failed ALT {} in Redis: {}",
                        alt_pubkey, redis_err
                    );
                }

                Ok(())
            } else {
                tokio::time::sleep(Duration::from_secs(1)).await;
                Box::pin(close_alt(
                    message_id,
                    alt_pubkey,
                    includer_client,
                    keypair,
                    authority_keypair,
                    redis_conn,
                    close_attempts + 1,
                ))
                .await
            }
        }
    }
}

// #[cfg(test)]
// mod tests {
//     use std::{str::FromStr, thread::sleep};

//     use super::*;
//     use solana_sdk::{
//         address_lookup_table::{
//             instruction::{create_lookup_table, extend_lookup_table},
//             state::AddressLookupTable,
//         },
//         signer::keypair::Keypair,
//     };

//     #[tokio::test]
//     async fn test_new_keypair_as_authority() -> anyhow::Result<()> {
//         dotenv().ok();
//         let network = std::env::var("NETWORK").expect("NETWORK must be set");
//         let config: SolanaConfig = config_from_yaml(&format!("config.{network}.yaml"))?;

//         let payer = config.signing_keypair();
//         let authority = Keypair::new();

//         let includer_client =
//             IncluderClient::new(&config.solana_poll_rpc, config.solana_commitment, 3)
//                 .map_err(|e| anyhow::anyhow!("Failed to create includer client: {}", e))?;

//         let recent_slot = includer_client
//             .get_slot()
//             .await
//             .map_err(|e| anyhow::anyhow!("Failed to fetch slot: {}", e))?;

//         let (create_ix, alt_pubkey) =
//             create_lookup_table(authority.pubkey(), payer.pubkey(), recent_slot);

//         info!(
//             "Creating ALT: authority={}, payer={}, slot={}, alt_pubkey={}",
//             authority.pubkey(),
//             payer.pubkey(),
//             recent_slot,
//             alt_pubkey
//         );

//         let alt_addresses: Vec<Pubkey> = (0..5).map(|_| Pubkey::new_unique()).collect();
//         let extend_ix = extend_lookup_table(
//             alt_pubkey,
//             authority.pubkey(),
//             Some(payer.pubkey()),
//             alt_addresses.clone(),
//         );

//         let recent_blockhash = includer_client
//             .get_latest_blockhash()
//             .await
//             .map_err(|e| anyhow::anyhow!("Failed to get recent blockhash: {}", e))?;

//         info!(
//             "Transaction signers: payer={}, authority={}",
//             payer.pubkey(),
//             authority.pubkey()
//         );
//         info!(
//             "Create instruction: program_id={}, accounts={:?}",
//             create_ix.program_id,
//             create_ix.accounts.len()
//         );

//         let transaction = Transaction::new_signed_with_payer(
//             &[create_ix, extend_ix],
//             Some(&payer.pubkey()),
//             &[&payer, &authority],
//             recent_blockhash,
//         );

//         let (signature, _) = includer_client
//             .send_transaction(SolanaTransactionType::Legacy(transaction))
//             .await
//             .map_err(|e| {
//                 anyhow::anyhow!(
//                     "Failed to send ALT transaction: {}. Authority: {}, Payer: {}, Slot: {}, ALT: {}",
//                     e,
//                     authority.pubkey(),
//                     payer.pubkey(),
//                     recent_slot,
//                     alt_pubkey
//                 )
//             })?;

//         info!("Created ALT {alt_pubkey} with signature {signature}");

//         sleep(Duration::from_secs(2));

//         let account_data = includer_client
//             .get_account_data(&alt_pubkey)
//             .await
//             .map_err(|e| anyhow::anyhow!("Failed to fetch ALT account data: {}", e))?;

//         let alt_state = AddressLookupTable::deserialize(&account_data)
//             .map_err(|e| anyhow::anyhow!("Failed to deserialize ALT state: {}", e))?;

//         assert_eq!(alt_state.meta.authority, Some(authority.pubkey()));
//         assert_eq!(alt_state.addresses.as_ref(), alt_addresses.as_slice());

//         println!("pubkey: {:?}", alt_pubkey);

//         sleep(Duration::from_secs(600));

//         let deactivate_ix = deactivate_lookup_table(alt_pubkey, authority.pubkey());

//         let recent_blockhash = includer_client
//             .get_latest_blockhash()
//             .await
//             .map_err(|e| anyhow::anyhow!("Failed to get recent blockhash: {}", e))?;

//         let transaction = Transaction::new_signed_with_payer(
//             &[deactivate_ix],
//             Some(&payer.pubkey()),
//             &[&payer, &authority],
//             recent_blockhash,
//         );

//         let (signature, _) = includer_client
//             .send_transaction(SolanaTransactionType::Legacy(transaction))
//             .await
//             .map_err(|e| anyhow::anyhow!("Failed to send transaction: {}", e))?;

//         println!("signature: {:?}", signature);

//         Ok(())
//     }
// }
