use async_trait::async_trait;
use redis::{AsyncTypedCommands, SetExpiry, SetOptions};
use relayer_core::error::IncluderError;
use relayer_core::utils::ThreadSafe;
use solana_sdk::pubkey::Pubkey;
use solana_transaction_parser::redis::TransactionType;
use tracing::{debug, warn};

use redis::aio::ConnectionManager;

const GAS_COST_EXPIRATION: u64 = 604800; // one week

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
    async fn write_alt_pubkey_to_redis(&self, message_id: String, alt_pubkey: Pubkey);
    async fn get_alt_pubkey_from_redis(
        &self,
        message_id: String,
    ) -> Result<Option<Pubkey>, IncluderError>;
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

    async fn write_alt_pubkey_to_redis(&self, message_id: String, alt_pubkey: Pubkey) {
        debug!("Writing ALT pubkey to Redis");
        let mut redis_conn = self.conn.clone();
        let set_opts = SetOptions::default().with_expiration(SetExpiry::EX(GAS_COST_EXPIRATION));
        let key = format!("ALT:{}", message_id);
        let pubkey_str = alt_pubkey.to_string();
        let result = redis_conn
            .set_options(key.clone(), pubkey_str.clone(), set_opts)
            .await;

        match result {
            Ok(_) => {
                debug!(
                    "ALT pubkey written to Redis successfully, key: {}, value: {}",
                    key, pubkey_str
                );
            }
            Err(e) => {
                warn!("Failed to write ALT pubkey to Redis: {}", e);
            }
        }
    }

    async fn get_alt_pubkey_from_redis(
        &self,
        message_id: String,
    ) -> Result<Option<Pubkey>, IncluderError> {
        let mut redis_conn = self.conn.clone();
        let key = format!("ALT:{}", message_id);
        let result: Result<Option<String>, redis::RedisError> = redis_conn.get(key.clone()).await;

        match result {
            Ok(Some(pubkey_str)) => {
                debug!("Found ALT pubkey in Redis for message_id: {}", message_id);
                match pubkey_str.parse::<Pubkey>() {
                    Ok(pubkey) => Ok(Some(pubkey)),
                    Err(e) => {
                        warn!("Failed to parse ALT pubkey from Redis: {}", e);
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

        redis_conn
            .write_alt_pubkey_to_redis(message_id.clone(), alt_pubkey)
            .await;

        // Read it back
        let result = redis_conn
            .get_alt_pubkey_from_redis(message_id.clone())
            .await
            .unwrap();

        assert_eq!(result, Some(alt_pubkey));

        // Verify it was written correctly by reading directly from Redis
        let mut conn = redis_conn.inner().clone();
        let key = format!("ALT:{}", message_id);
        let stored_value: Option<String> = redis::AsyncCommands::get(&mut conn, key).await.unwrap();

        assert_eq!(stored_value, Some(alt_pubkey.to_string()));
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
    async fn test_get_alt_pubkey_invalid_format() {
        let (_container, redis_conn) = create_redis_connection().await;

        let message_id = "test-message-invalid".to_string();
        let invalid_pubkey_str = "not-a-valid-pubkey";

        // Write invalid pubkey string directly to Redis
        let mut conn = redis_conn.inner().clone();
        let key = format!("ALT:{}", message_id);
        let _: () = redis::AsyncCommands::set(&mut conn, key, invalid_pubkey_str)
            .await
            .unwrap();

        // Try to read it back - should return None due to parse error
        let result = redis_conn
            .get_alt_pubkey_from_redis(message_id)
            .await
            .unwrap();

        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn test_alt_pubkey_expiration() {
        let (_container, redis_conn) = create_redis_connection().await;

        let message_id = "test-message-expiration".to_string();
        let alt_pubkey = Pubkey::new_unique();

        redis_conn
            .write_alt_pubkey_to_redis(message_id.clone(), alt_pubkey)
            .await;

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
            .await;
        redis_conn
            .write_alt_pubkey_to_redis(message_id_2.clone(), alt_pubkey_2)
            .await;

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
            .await;

        // Overwrite with new pubkey
        redis_conn
            .write_alt_pubkey_to_redis(message_id.clone(), alt_pubkey_2)
            .await;

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
            .await;

        // Stop Redis container
        container.stop_with_timeout(Some(1)).await.unwrap();

        // Write should fail gracefully (no panic, just logs warning)
        redis_conn
            .write_alt_pubkey_to_redis(message_id.clone(), alt_pubkey)
            .await;

        // Read should return None gracefully
        let result = redis_conn
            .get_alt_pubkey_from_redis(message_id.clone())
            .await
            .unwrap();

        assert_eq!(result, None);
    }
}
