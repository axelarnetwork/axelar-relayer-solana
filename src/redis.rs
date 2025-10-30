use async_trait::async_trait;
use redis::{AsyncTypedCommands, SetExpiry, SetOptions};
use relayer_core::error::IncluderError;
use relayer_core::utils::ThreadSafe;
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use solana_transaction_parser::redis::TransactionType;
use tracing::{debug, warn};

use redis::aio::ConnectionManager;

const GAS_COST_EXPIRATION: u64 = 604800; // one week

#[derive(Serialize, Deserialize, Debug, Clone)]
struct AltEntry {
    pubkey: String,
    created_at: i64, // Unix timestamp in seconds
}

#[cfg_attr(any(test), mockall::automock)]
#[async_trait]
pub trait RedisConnectionTrait: ThreadSafe {
    fn inner(&self) -> &ConnectionManager;
    async fn write_gas_cost_to_redis(
        &self,
        message_id: String,
        gas_cost: u64,
        transaction_type: TransactionType,
    );
    async fn write_alt_pubkey_to_redis(
        &self,
        message_id: String,
        alt_pubkey: Pubkey,
    ) -> Result<(), IncluderError>;
    async fn get_alt_pubkey_from_redis(
        &self,
        message_id: String,
    ) -> Result<Option<Pubkey>, IncluderError>;
    async fn get_all_alt_keys(&self) -> Result<Vec<(String, Pubkey, i64)>, IncluderError>;
}

#[derive(Clone)]
pub struct RedisConnection {
    conn: ConnectionManager,
}

impl RedisConnection {
    pub fn new(conn: ConnectionManager) -> Self {
        Self { conn }
    }
}

#[async_trait]
impl RedisConnectionTrait for RedisConnection {
    fn inner(&self) -> &ConnectionManager {
        &self.conn
    }

    async fn write_gas_cost_to_redis(
        &self,
        message_id: String,
        gas_cost: u64,
        transaction_type: TransactionType,
    ) {
        debug!("Writing gas cost to Redis");
        let mut redis_conn = self.conn.clone();
        let set_opts = SetOptions::default().with_expiration(SetExpiry::EX(GAS_COST_EXPIRATION));
        let key = format!("cost:{}:{}", transaction_type, message_id);
        let result = redis_conn
            .set_options(key.clone(), gas_cost, set_opts)
            .await;

        match result {
            Ok(_) => {
                debug!(
                    "Gas cost written to Redis successfully, key: {}, value: {}",
                    key, gas_cost
                );
            }
            Err(e) => {
                warn!("Failed to write gas cost to Redis: {}", e);
            }
        }
    }

    async fn write_alt_pubkey_to_redis(
        &self,
        message_id: String,
        alt_pubkey: Pubkey,
    ) -> Result<(), IncluderError> {
        debug!("Writing ALT pubkey to Redis");
        let mut redis_conn = self.conn.clone();
        let set_opts = SetOptions::default().with_expiration(SetExpiry::EX(GAS_COST_EXPIRATION));
        let key = format!("ALT:{}", message_id);

        // Store pubkey with timestamp
        let created_at = chrono::Utc::now().timestamp();
        let entry = AltEntry {
            pubkey: alt_pubkey.to_string(),
            created_at,
        };

        let entry_json = serde_json::to_string(&entry).map_err(|e| {
            IncluderError::GenericError(format!("Failed to serialize ALT entry: {}", e))
        })?;

        redis_conn
            .set_options(key.clone(), entry_json.clone(), set_opts)
            .await
            .map_err(|e| {
                IncluderError::GenericError(format!("Failed to write ALT pubkey to Redis: {}", e))
            })?;

        debug!(
            "ALT pubkey written to Redis successfully, key: {}, value: {}",
            key, entry_json
        );

        Ok(())
    }

    async fn get_alt_pubkey_from_redis(
        &self,
        message_id: String,
    ) -> Result<Option<Pubkey>, IncluderError> {
        let mut redis_conn = self.conn.clone();
        let key = format!("ALT:{}", message_id);
        let result: Result<Option<String>, redis::RedisError> = redis_conn.get(key.clone()).await;

        match result {
            Ok(Some(value_str)) => {
                debug!("Found ALT pubkey in Redis for message_id: {}", message_id);

                let entry = serde_json::from_str::<AltEntry>(&value_str).map_err(|e| {
                    warn!("Failed to deserialize ALT entry: {}", e);
                    IncluderError::GenericError(format!("Failed to deserialize ALT entry: {}", e))
                })?;

                match entry.pubkey.parse::<Pubkey>() {
                    Ok(pubkey) => Ok(Some(pubkey)),
                    Err(e) => {
                        warn!("Failed to parse ALT pubkey from entry: {}", e);
                        Ok(None)
                    }
                }
            }
            Ok(None) => {
                debug!(
                    "No ALT pubkey found in Redis for message_id: {}",
                    message_id
                );
                Ok(None)
            }
            Err(e) => {
                warn!("Failed to read ALT pubkey from Redis: {}", e);
                Ok(None)
            }
        }
    }

    async fn get_all_alt_keys(&self) -> Result<Vec<(String, Pubkey, i64)>, IncluderError> {
        let mut redis_conn = self.conn.clone();
        let mut all_keys = Vec::new();

        // Use KEYS command to get all ALT keys (for ALT management, we won't have thousands)
        let keys: Vec<String> = redis::AsyncCommands::keys(&mut redis_conn, "ALT:*")
            .await
            .map_err(|e| {
                IncluderError::GenericError(format!("Failed to get ALT keys from Redis: {}", e))
            })?;

        for key in keys {
            // Extract message_id from key (format: "ALT:{message_id}")
            if let Some(message_id) = key.strip_prefix("ALT:") {
                if let Ok(Some(value_str)) =
                    redis::AsyncCommands::get::<_, Option<String>>(&mut redis_conn, &key).await
                {
                    match serde_json::from_str::<AltEntry>(&value_str) {
                        Ok(entry) => {
                            if let Ok(pubkey) = entry.pubkey.parse::<Pubkey>() {
                                all_keys.push((message_id.to_string(), pubkey, entry.created_at));
                            }
                        }
                        Err(_) => {
                            // Skip invalid entries
                        }
                    }
                }
            }
        }

        Ok(all_keys)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis::Client;
    use solana_transaction_parser::redis::TransactionType;
    use std::time::Duration;
    use testcontainers::core::{IntoContainerPort, WaitFor};
    use testcontainers::runners::AsyncRunner;
    use testcontainers::GenericImage;

    async fn create_redis_connection() -> (
        testcontainers::ContainerAsync<GenericImage>,
        RedisConnection,
    ) {
        let container = GenericImage::new("redis", "7.2.4")
            .with_exposed_port(9991.tcp())
            .with_wait_for(WaitFor::message_on_stdout("Ready to accept connections"))
            .start()
            .await
            .unwrap();

        let host = container.get_host().await.unwrap();
        let host_port = container.get_host_port_ipv4(6379).await.unwrap();

        let url = format!("redis://{host}:{host_port}");
        let client = Client::open(url.as_ref()).unwrap();

        let conn = relayer_core::redis::connection_manager(
            client,
            Some(Duration::from_millis(100)),
            Some(Duration::from_millis(100)),
            Some(2),
        )
        .await
        .unwrap();

        let redis_conn = RedisConnection::new(conn);

        (container, redis_conn)
    }

    #[tokio::test]
    async fn test_write_and_read_gas_cost_execute() {
        let (_container, redis_conn) = create_redis_connection().await;

        let message_id = "test-message-123".to_string();
        let gas_cost = 50000u64;

        redis_conn
            .write_gas_cost_to_redis(message_id.clone(), gas_cost, TransactionType::Execute)
            .await;

        // Verify it was written by reading directly from Redis
        let mut conn = redis_conn.inner().clone();
        let key = format!("cost:{}:{}", TransactionType::Execute, message_id);
        let stored_value: Option<String> = redis::AsyncCommands::get(&mut conn, key).await.unwrap();

        assert_eq!(stored_value, Some(gas_cost.to_string()));
    }

    #[tokio::test]
    async fn test_write_and_read_gas_cost_approve() {
        let (_container, redis_conn) = create_redis_connection().await;

        let message_id = "test-message-456".to_string();
        let gas_cost = 30000u64;

        redis_conn
            .write_gas_cost_to_redis(message_id.clone(), gas_cost, TransactionType::Approve)
            .await;

        // Verify it was written by reading directly from Redis
        let mut conn = redis_conn.inner().clone();
        let key = format!("cost:{}:{}", TransactionType::Approve, message_id);
        let stored_value: Option<String> = redis::AsyncCommands::get(&mut conn, key).await.unwrap();

        assert_eq!(stored_value, Some(gas_cost.to_string()));
    }

    #[tokio::test]
    async fn test_write_and_read_alt_pubkey() {
        let (_container, redis_conn) = create_redis_connection().await;

        let message_id = "test-message-789".to_string();
        let alt_pubkey = Pubkey::new_unique();
        let before_timestamp = chrono::Utc::now().timestamp();

        redis_conn
            .write_alt_pubkey_to_redis(message_id.clone(), alt_pubkey)
            .await
            .unwrap();

        let after_timestamp = chrono::Utc::now().timestamp();

        // Read it back
        let result = redis_conn
            .get_alt_pubkey_from_redis(message_id.clone())
            .await
            .unwrap();

        assert_eq!(result, Some(alt_pubkey));

        // Verify it was written in JSON format with timestamp
        let mut conn = redis_conn.inner().clone();
        let key = format!("ALT:{}", message_id);
        let stored_value: Option<String> = redis::AsyncCommands::get(&mut conn, key).await.unwrap();

        assert!(stored_value.is_some());
        let entry: AltEntry = serde_json::from_str(&stored_value.unwrap()).unwrap();
        assert_eq!(entry.pubkey, alt_pubkey.to_string());
        assert!(entry.created_at >= before_timestamp);
        assert!(entry.created_at <= after_timestamp);
    }

    #[tokio::test]
    async fn test_get_alt_pubkey_not_found() {
        let (_container, redis_conn) = create_redis_connection().await;

        let message_id = "non-existent-message".to_string();

        let result = redis_conn
            .get_alt_pubkey_from_redis(message_id)
            .await
            .unwrap();

        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn test_get_alt_pubkey_invalid_json_format() {
        let (_container, redis_conn) = create_redis_connection().await;

        let message_id = "test-message-invalid".to_string();
        let invalid_json = "not-a-valid-json";

        // Write invalid JSON directly to Redis
        let mut conn = redis_conn.inner().clone();
        let key = format!("ALT:{}", message_id);
        let _: () = redis::AsyncCommands::set(&mut conn, key, invalid_json)
            .await
            .unwrap();

        // Try to read it back - should return error due to deserialize error
        let result = redis_conn.get_alt_pubkey_from_redis(message_id).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_all_alt_keys() {
        let (_container, redis_conn) = create_redis_connection().await;

        let message_id_1 = "test-message-1".to_string();
        let message_id_2 = "test-message-2".to_string();
        let alt_pubkey_1 = Pubkey::new_unique();
        let alt_pubkey_2 = Pubkey::new_unique();

        let before_timestamp = chrono::Utc::now().timestamp();

        redis_conn
            .write_alt_pubkey_to_redis(message_id_1.clone(), alt_pubkey_1)
            .await
            .unwrap();
        redis_conn
            .write_alt_pubkey_to_redis(message_id_2.clone(), alt_pubkey_2)
            .await
            .unwrap();

        let after_timestamp = chrono::Utc::now().timestamp();

        // Get all ALT keys
        let result = redis_conn.get_all_alt_keys().await.unwrap();

        assert_eq!(result.len(), 2);

        // Verify both entries are present with correct format
        let mut found_1 = false;
        let mut found_2 = false;

        for (msg_id, pubkey, timestamp) in result {
            if msg_id == message_id_1 && pubkey == alt_pubkey_1 {
                assert!(timestamp >= before_timestamp);
                assert!(timestamp <= after_timestamp);
                found_1 = true;
            }
            if msg_id == message_id_2 && pubkey == alt_pubkey_2 {
                assert!(timestamp >= before_timestamp);
                assert!(timestamp <= after_timestamp);
                found_2 = true;
            }
        }

        assert!(found_1);
        assert!(found_2);
    }

    #[tokio::test]
    async fn test_alt_pubkey_expiration() {
        let (_container, redis_conn) = create_redis_connection().await;

        let message_id = "test-message-expiration".to_string();
        let alt_pubkey = Pubkey::new_unique();

        redis_conn
            .write_alt_pubkey_to_redis(message_id.clone(), alt_pubkey)
            .await
            .unwrap();

        // Verify it exists
        let result = redis_conn
            .get_alt_pubkey_from_redis(message_id.clone())
            .await
            .unwrap();
        assert_eq!(result, Some(alt_pubkey));

        // Verify TTL is set (should be approximately GAS_COST_EXPIRATION)
        let mut conn = redis_conn.inner().clone();
        let key = format!("ALT:{}", message_id);
        let ttl: i64 = redis::AsyncCommands::ttl(&mut conn, key).await.unwrap();

        // TTL should be close to GAS_COST_EXPIRATION (604800 seconds)
        // Allow some margin for test execution time
        // Redis returns -1 if key exists but has no TTL, -2 if key doesn't exist
        assert!(ttl > 0, "TTL should be positive");
        assert!(ttl <= GAS_COST_EXPIRATION as i64);
        // Allow some margin for test execution time
        assert!(ttl > (GAS_COST_EXPIRATION as i64 - 10));
    }

    #[tokio::test]
    async fn test_gas_cost_expiration() {
        let (_container, redis_conn) = create_redis_connection().await;

        let message_id = "test-message-gas-expiration".to_string();
        let gas_cost = 75000u64;

        redis_conn
            .write_gas_cost_to_redis(message_id.clone(), gas_cost, TransactionType::Execute)
            .await;

        // Verify TTL is set
        let mut conn = redis_conn.inner().clone();
        let key = format!("cost:{}:{}", TransactionType::Execute, message_id);
        let ttl: i64 = redis::AsyncCommands::ttl(&mut conn, key).await.unwrap();

        // TTL should be close to GAS_COST_EXPIRATION (604800 seconds)
        // Redis returns -1 if key exists but has no TTL, -2 if key doesn't exist
        assert!(ttl > 0, "TTL should be positive");
        assert!(ttl <= GAS_COST_EXPIRATION as i64);
        // Allow some margin for test execution time
        assert!(ttl > (GAS_COST_EXPIRATION as i64 - 10));
    }

    #[tokio::test]
    async fn test_multiple_gas_costs_different_types() {
        let (_container, redis_conn) = create_redis_connection().await;

        let message_id = "test-message-multi".to_string();
        let execute_cost = 50000u64;
        let approve_cost = 30000u64;

        redis_conn
            .write_gas_cost_to_redis(message_id.clone(), execute_cost, TransactionType::Execute)
            .await;
        redis_conn
            .write_gas_cost_to_redis(message_id.clone(), approve_cost, TransactionType::Approve)
            .await;

        // Verify both are stored with different keys
        let mut conn = redis_conn.inner().clone();
        let execute_key = format!("cost:{}:{}", TransactionType::Execute, message_id);
        let approve_key = format!("cost:{}:{}", TransactionType::Approve, message_id);

        let stored_execute: Option<String> = redis::AsyncCommands::get(&mut conn, execute_key)
            .await
            .unwrap();
        let stored_approve: Option<String> = redis::AsyncCommands::get(&mut conn, approve_key)
            .await
            .unwrap();

        assert_eq!(stored_execute, Some(execute_cost.to_string()));
        assert_eq!(stored_approve, Some(approve_cost.to_string()));
    }

    #[tokio::test]
    async fn test_multiple_alt_pubkeys_different_messages() {
        let (_container, redis_conn) = create_redis_connection().await;

        let message_id_1 = "test-message-1".to_string();
        let message_id_2 = "test-message-2".to_string();
        let alt_pubkey_1 = Pubkey::new_unique();
        let alt_pubkey_2 = Pubkey::new_unique();

        redis_conn
            .write_alt_pubkey_to_redis(message_id_1.clone(), alt_pubkey_1)
            .await
            .unwrap();
        redis_conn
            .write_alt_pubkey_to_redis(message_id_2.clone(), alt_pubkey_2)
            .await
            .unwrap();

        // Verify both are stored and retrieved correctly
        let result_1 = redis_conn
            .get_alt_pubkey_from_redis(message_id_1.clone())
            .await
            .unwrap();
        let result_2 = redis_conn
            .get_alt_pubkey_from_redis(message_id_2.clone())
            .await
            .unwrap();

        assert_eq!(result_1, Some(alt_pubkey_1));
        assert_eq!(result_2, Some(alt_pubkey_2));
        assert_ne!(result_1, result_2);
    }

    #[tokio::test]
    async fn test_overwrite_alt_pubkey() {
        let (_container, redis_conn) = create_redis_connection().await;

        let message_id = "test-message-overwrite".to_string();
        let alt_pubkey_1 = Pubkey::new_unique();
        let alt_pubkey_2 = Pubkey::new_unique();

        redis_conn
            .write_alt_pubkey_to_redis(message_id.clone(), alt_pubkey_1)
            .await
            .unwrap();

        // Overwrite with new pubkey
        redis_conn
            .write_alt_pubkey_to_redis(message_id.clone(), alt_pubkey_2)
            .await
            .unwrap();

        // Verify new pubkey is stored
        let result = redis_conn
            .get_alt_pubkey_from_redis(message_id)
            .await
            .unwrap();

        assert_eq!(result, Some(alt_pubkey_2));
    }

    #[tokio::test]
    async fn test_redis_connection_failure_graceful() {
        let (container, redis_conn) = create_redis_connection().await;

        let message_id = "test-message-failure".to_string();
        let alt_pubkey = Pubkey::new_unique();

        // Write should succeed
        redis_conn
            .write_alt_pubkey_to_redis(message_id.clone(), alt_pubkey)
            .await
            .unwrap();

        // Stop Redis container
        container.stop_with_timeout(Some(1)).await.unwrap();

        // Write should fail gracefully (returns error, doesn't panic)
        let write_result = redis_conn
            .write_alt_pubkey_to_redis(message_id.clone(), alt_pubkey)
            .await;

        assert!(
            write_result.is_err(),
            "Write should fail when Redis is unavailable"
        );

        // Read should return None gracefully
        let result = redis_conn
            .get_alt_pubkey_from_redis(message_id.clone())
            .await
            .unwrap();

        assert_eq!(result, None);
    }
}
