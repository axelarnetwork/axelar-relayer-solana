use crate::error::RedisInterfaceError;
use async_trait::async_trait;
use redis::{AsyncTypedCommands, SetExpiry, SetOptions};
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
    #[serde(default)]
    retry_count: u32, // Number of retry attempts
    #[serde(default = "default_true")]
    active: bool, // true if ALT is active, false if deactivated
}

fn default_true() -> bool {
    true
}

#[cfg_attr(any(test), mockall::automock)]
#[async_trait]
pub trait RedisConnectionTrait: ThreadSafe {
    fn inner(&self) -> &ConnectionManager;
    async fn write_gas_cost(
        &self,
        message_id: String,
        gas_cost: u64,
        transaction_type: TransactionType,
    );
    async fn write_alt_pubkey(
        &self,
        message_id: String,
        alt_pubkey: Pubkey,
    ) -> Result<(), RedisInterfaceError>;
    async fn get_alt_pubkey(
        &self,
        message_id: String,
    ) -> Result<Option<Pubkey>, RedisInterfaceError>;
    #[allow(clippy::type_complexity)]
    async fn get_all_alt_keys(
        &self,
    ) -> Result<Vec<(String, Pubkey, i64, u32, bool)>, RedisInterfaceError>;
    async fn remove_alt_key(&self, message_id: String) -> Result<(), RedisInterfaceError>;
    async fn update_alt_retry_count(
        &self,
        message_id: String,
        retry_count: u32,
    ) -> Result<(), RedisInterfaceError>;
    async fn set_alt_inactive(&self, message_id: String) -> Result<(), RedisInterfaceError>;
    async fn set_alt_failed(
        &self,
        message_id: String,
        alt_pubkey: Pubkey,
    ) -> Result<(), RedisInterfaceError>;
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

    async fn write_gas_cost(
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

    async fn write_alt_pubkey(
        &self,
        message_id: String,
        alt_pubkey: Pubkey,
    ) -> Result<(), RedisInterfaceError> {
        debug!("Writing ALT pubkey to Redis");
        let mut redis_conn = self.conn.clone();
        let set_opts = SetOptions::default().with_expiration(SetExpiry::EX(GAS_COST_EXPIRATION));
        let key = format!("ALT:{}", message_id);

        let created_at = chrono::Utc::now().timestamp();
        let entry = AltEntry {
            pubkey: alt_pubkey.to_string(),
            created_at,
            retry_count: 0,
            active: true,
        };

        let entry_json = serde_json::to_string(&entry).map_err(|e| {
            RedisInterfaceError::WriteAltPubkeyError(format!(
                "Failed to serialize ALT entry: {}",
                e
            ))
        })?;

        redis_conn
            .set_options(key.clone(), entry_json.clone(), set_opts)
            .await
            .map_err(|e| {
                RedisInterfaceError::WriteAltPubkeyError(format!(
                    "Failed to write ALT pubkey to Redis: {}",
                    e
                ))
            })?;

        debug!(
            "ALT pubkey written to Redis successfully, key: {}, value: {}",
            key, entry_json
        );

        Ok(())
    }

    async fn get_alt_pubkey(
        &self,
        message_id: String,
    ) -> Result<Option<Pubkey>, RedisInterfaceError> {
        let mut redis_conn = self.conn.clone();
        let key = format!("ALT:{}", message_id);
        let result: Result<Option<String>, redis::RedisError> = redis_conn.get(key.clone()).await;

        match result {
            Ok(Some(value_str)) => {
                debug!("Found ALT pubkey in Redis for message_id: {}", message_id);

                let entry = serde_json::from_str::<AltEntry>(&value_str).map_err(|e| {
                    warn!("Failed to deserialize ALT entry: {}", e);
                    RedisInterfaceError::GetAltPubkeyError(format!(
                        "Failed to deserialize ALT entry: {}",
                        e
                    ))
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

    async fn get_all_alt_keys(
        &self,
    ) -> Result<Vec<(String, Pubkey, i64, u32, bool)>, RedisInterfaceError> {
        let mut redis_conn = self.conn.clone();
        let mut all_keys = Vec::new();

        let mut cursor = 0;
        let mut keys = Vec::new();
        loop {
            let (next_cursor, mut scanned_keys): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg("ALT:*")
                .arg("COUNT")
                .arg(100)
                .query_async(&mut redis_conn)
                .await
                .map_err(|e| {
                    RedisInterfaceError::GenericError(format!(
                        "Failed to scan ALT keys from Redis: {}",
                        e
                    ))
                })?;

            keys.append(&mut scanned_keys);
            cursor = next_cursor;

            if cursor == 0 {
                break;
            }
        }

        for key in keys {
            if let Some(message_id) = key.strip_prefix("ALT:") {
                if let Ok(Some(value_str)) =
                    redis::AsyncCommands::get::<_, Option<String>>(&mut redis_conn, &key).await
                {
                    if let Ok(entry) = serde_json::from_str::<AltEntry>(&value_str) {
                        if let Ok(pubkey) = entry.pubkey.parse::<Pubkey>() {
                            all_keys.push((
                                message_id.to_string(),
                                pubkey,
                                entry.created_at,
                                entry.retry_count,
                                entry.active,
                            ));
                        }
                    }
                }
            }
        }

        Ok(all_keys)
    }

    async fn remove_alt_key(&self, message_id: String) -> Result<(), RedisInterfaceError> {
        let mut redis_conn = self.conn.clone();
        let key = format!("ALT:{}", message_id);

        redis::AsyncCommands::del::<_, ()>(&mut redis_conn, key.clone())
            .await
            .map_err(|e| {
                RedisInterfaceError::RemoveAltKeyError(format!(
                    "Failed to remove ALT key from Redis: {}",
                    e
                ))
            })?;

        debug!("Removed ALT key from Redis: {}", key);
        Ok(())
    }

    async fn update_alt_retry_count(
        &self,
        message_id: String,
        retry_count: u32,
    ) -> Result<(), RedisInterfaceError> {
        let mut redis_conn = self.conn.clone();
        let key = format!("ALT:{}", message_id);
        let set_opts = SetOptions::default().with_expiration(SetExpiry::EX(GAS_COST_EXPIRATION));

        let existing_value: Option<String> = redis::AsyncCommands::get(&mut redis_conn, &key)
            .await
            .map_err(|e| {
                RedisInterfaceError::UpdateAltRetryCountError(format!(
                    "Failed to get ALT entry from Redis: {}",
                    e
                ))
            })?;

        if let Some(value_str) = existing_value {
            let mut entry: AltEntry = serde_json::from_str(&value_str).map_err(|e| {
                RedisInterfaceError::UpdateAltRetryCountError(format!(
                    "Failed to deserialize ALT entry: {}",
                    e
                ))
            })?;

            entry.retry_count = retry_count;

            let entry_json = serde_json::to_string(&entry).map_err(|e| {
                RedisInterfaceError::UpdateAltRetryCountError(format!(
                    "Failed to serialize ALT entry: {}",
                    e
                ))
            })?;

            redis_conn
                .set_options(key.clone(), entry_json.clone(), set_opts)
                .await
                .map_err(|e| {
                    RedisInterfaceError::UpdateAltRetryCountError(format!(
                        "Failed to update ALT retry count in Redis: {}",
                        e
                    ))
                })?;

            debug!(
                "Updated ALT retry count in Redis: {}, retry_count: {}",
                key, retry_count
            );
        } else {
            return Err(RedisInterfaceError::UpdateAltRetryCountError(format!(
                "ALT entry not found for message_id: {}",
                message_id
            )));
        }

        Ok(())
    }

    async fn set_alt_inactive(&self, message_id: String) -> Result<(), RedisInterfaceError> {
        let mut redis_conn = self.conn.clone();
        let key = format!("ALT:{}", message_id);
        let set_opts = SetOptions::default().with_expiration(SetExpiry::EX(GAS_COST_EXPIRATION));

        let existing_value: Option<String> = redis::AsyncCommands::get(&mut redis_conn, &key)
            .await
            .map_err(|e| {
                RedisInterfaceError::SetAltInactiveError(format!(
                    "Failed to get ALT entry from Redis: {}",
                    e
                ))
            })?;

        if let Some(value_str) = existing_value {
            let mut entry: AltEntry = serde_json::from_str(&value_str).map_err(|e| {
                RedisInterfaceError::SetAltInactiveError(format!(
                    "Failed to deserialize ALT entry: {}",
                    e
                ))
            })?;

            entry.active = false;
            entry.created_at = chrono::Utc::now().timestamp();

            let entry_json = serde_json::to_string(&entry).map_err(|e| {
                RedisInterfaceError::SetAltInactiveError(format!(
                    "Failed to serialize ALT entry: {}",
                    e
                ))
            })?;

            redis_conn
                .set_options(key.clone(), entry_json.clone(), set_opts)
                .await
                .map_err(|e| {
                    RedisInterfaceError::SetAltInactiveError(format!(
                        "Failed to set ALT as inactive in Redis: {}",
                        e
                    ))
                })?;

            debug!("Set ALT as inactive in Redis: {}", key);
            Ok(())
        } else {
            Err(RedisInterfaceError::SetAltInactiveError(format!(
                "ALT entry not found for message_id: {}",
                message_id
            )))
        }
    }

    async fn set_alt_failed(
        &self,
        message_id: String,
        alt_pubkey: Pubkey,
    ) -> Result<(), RedisInterfaceError> {
        let mut redis_conn = self.conn.clone();
        let key = format!("FAILED:ALT:{}", message_id);

        let pubkey_str = alt_pubkey.to_string();

        redis::AsyncCommands::set::<_, _, ()>(&mut redis_conn, key.clone(), pubkey_str.clone())
            .await
            .map_err(|e| {
                RedisInterfaceError::SetAltFailedError(format!(
                    "Failed to set failed ALT in Redis: {}",
                    e
                ))
            })?;

        debug!("Set failed ALT in Redis: {} -> {}", key, pubkey_str);
        Ok(())
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
            .with_exposed_port(6379.tcp())
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
            Some(500),
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
            .write_gas_cost(message_id.clone(), gas_cost, TransactionType::Execute)
            .await;

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
            .write_gas_cost(message_id.clone(), gas_cost, TransactionType::Approve)
            .await;

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
            .write_alt_pubkey(message_id.clone(), alt_pubkey)
            .await
            .unwrap();

        let after_timestamp = chrono::Utc::now().timestamp();

        let result = redis_conn.get_alt_pubkey(message_id.clone()).await.unwrap();

        assert_eq!(result, Some(alt_pubkey));

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

        let result = redis_conn.get_alt_pubkey(message_id).await.unwrap();

        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn test_get_alt_pubkey_invalid_json_format() {
        let (_container, redis_conn) = create_redis_connection().await;

        let message_id = "test-message-invalid".to_string();
        let invalid_json = "not-a-valid-json";

        let mut conn = redis_conn.inner().clone();
        let key = format!("ALT:{}", message_id);
        let _: () = redis::AsyncCommands::set(&mut conn, key, invalid_json)
            .await
            .unwrap();

        let result = redis_conn.get_alt_pubkey(message_id).await;

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
            .write_alt_pubkey(message_id_1.clone(), alt_pubkey_1)
            .await
            .unwrap();
        redis_conn
            .write_alt_pubkey(message_id_2.clone(), alt_pubkey_2)
            .await
            .unwrap();

        let after_timestamp = chrono::Utc::now().timestamp();

        let result = redis_conn.get_all_alt_keys().await.unwrap();

        assert_eq!(result.len(), 2);

        let mut found_1 = false;
        let mut found_2 = false;

        for (msg_id, pubkey, timestamp, _retry_count, active) in result {
            if msg_id == message_id_1 && pubkey == alt_pubkey_1 {
                assert!(timestamp >= before_timestamp);
                assert!(timestamp <= after_timestamp);
                assert!(active); // Should be active by default
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
    async fn test_get_all_alt_keys_with_many_keys() {
        let (_container, redis_conn) = create_redis_connection().await;

        let mut message_ids = Vec::new();
        let mut alt_pubkeys = Vec::new();
        for i in 0..150 {
            let message_id = format!("test-message-{}", i);
            let alt_pubkey = Pubkey::new_unique();
            message_ids.push(message_id.clone());
            alt_pubkeys.push(alt_pubkey);
            redis_conn
                .write_alt_pubkey(message_id, alt_pubkey)
                .await
                .unwrap();
        }

        let result = redis_conn.get_all_alt_keys().await.unwrap();

        assert_eq!(result.len(), 150);

        let mut found_count = 0;
        for (msg_id, pubkey, _timestamp, _retry_count, _active) in result {
            if let Some(idx) = message_ids.iter().position(|m| m == &msg_id) {
                if alt_pubkeys[idx] == pubkey {
                    found_count += 1;
                }
            }
        }

        assert_eq!(found_count, 150);
    }

    #[tokio::test]
    async fn test_remove_alt_key_from_redis() {
        let (_container, redis_conn) = create_redis_connection().await;

        let message_id = "test-message-remove".to_string();
        let alt_pubkey = Pubkey::new_unique();

        redis_conn
            .write_alt_pubkey(message_id.clone(), alt_pubkey)
            .await
            .unwrap();

        let result = redis_conn.get_alt_pubkey(message_id.clone()).await.unwrap();
        assert_eq!(result, Some(alt_pubkey));

        redis_conn.remove_alt_key(message_id.clone()).await.unwrap();

        let result = redis_conn.get_alt_pubkey(message_id.clone()).await.unwrap();
        assert_eq!(result, None);

        let all_keys = redis_conn.get_all_alt_keys().await.unwrap();
        assert!(!all_keys
            .iter()
            .any(|(msg_id, _, _, _, _)| msg_id == &message_id));
    }

    #[tokio::test]
    async fn test_remove_alt_key_from_redis_non_existent() {
        let (_container, redis_conn) = create_redis_connection().await;

        let message_id = "non-existent-message".to_string();

        let result = redis_conn.remove_alt_key(message_id.clone()).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_remove_alt_key_from_redis_multiple_keys() {
        let (_container, redis_conn) = create_redis_connection().await;

        let message_id_1 = "test-message-remove-1".to_string();
        let message_id_2 = "test-message-remove-2".to_string();
        let message_id_3 = "test-message-remove-3".to_string();
        let alt_pubkey_1 = Pubkey::new_unique();
        let alt_pubkey_2 = Pubkey::new_unique();
        let alt_pubkey_3 = Pubkey::new_unique();

        redis_conn
            .write_alt_pubkey(message_id_1.clone(), alt_pubkey_1)
            .await
            .unwrap();
        redis_conn
            .write_alt_pubkey(message_id_2.clone(), alt_pubkey_2)
            .await
            .unwrap();
        redis_conn
            .write_alt_pubkey(message_id_3.clone(), alt_pubkey_3)
            .await
            .unwrap();

        let all_keys = redis_conn.get_all_alt_keys().await.unwrap();
        assert_eq!(all_keys.len(), 3);

        redis_conn
            .remove_alt_key(message_id_2.clone())
            .await
            .unwrap();
        let result_1 = redis_conn
            .get_alt_pubkey(message_id_1.clone())
            .await
            .unwrap();
        let result_2 = redis_conn
            .get_alt_pubkey(message_id_2.clone())
            .await
            .unwrap();
        let result_3 = redis_conn
            .get_alt_pubkey(message_id_3.clone())
            .await
            .unwrap();

        assert_eq!(result_1, Some(alt_pubkey_1));
        assert_eq!(result_2, None);
        assert_eq!(result_3, Some(alt_pubkey_3));

        let all_keys = redis_conn.get_all_alt_keys().await.unwrap();
        assert_eq!(all_keys.len(), 2);
        assert!(all_keys
            .iter()
            .any(|(msg_id, _, _, _, _)| msg_id == &message_id_1));
        assert!(!all_keys
            .iter()
            .any(|(msg_id, _, _, _, _)| msg_id == &message_id_2));
        assert!(all_keys
            .iter()
            .any(|(msg_id, _, _, _, _)| msg_id == &message_id_3));
    }

    #[tokio::test]
    async fn test_alt_pubkey_expiration() {
        let (_container, redis_conn) = create_redis_connection().await;

        let message_id = "test-message-expiration".to_string();
        let alt_pubkey = Pubkey::new_unique();

        redis_conn
            .write_alt_pubkey(message_id.clone(), alt_pubkey)
            .await
            .unwrap();

        let result = redis_conn.get_alt_pubkey(message_id.clone()).await.unwrap();
        assert_eq!(result, Some(alt_pubkey));

        let mut conn = redis_conn.inner().clone();
        let key = format!("ALT:{}", message_id);
        let ttl: i64 = redis::AsyncCommands::ttl(&mut conn, key).await.unwrap();

        assert!(ttl > 0, "TTL should be positive");
        assert!(ttl <= GAS_COST_EXPIRATION as i64);
        assert!(ttl > (GAS_COST_EXPIRATION as i64 - 10));
    }

    #[tokio::test]
    async fn test_gas_cost_expiration() {
        let (_container, redis_conn) = create_redis_connection().await;

        let message_id = "test-message-gas-expiration".to_string();
        let gas_cost = 75000u64;

        redis_conn
            .write_gas_cost(message_id.clone(), gas_cost, TransactionType::Execute)
            .await;

        let mut conn = redis_conn.inner().clone();
        let key = format!("cost:{}:{}", TransactionType::Execute, message_id);
        let ttl: i64 = redis::AsyncCommands::ttl(&mut conn, key).await.unwrap();

        assert!(ttl > 0, "TTL should be positive");
        assert!(ttl <= GAS_COST_EXPIRATION as i64);
        assert!(ttl > (GAS_COST_EXPIRATION as i64 - 10));
    }

    #[tokio::test]
    async fn test_multiple_gas_costs_different_types() {
        let (_container, redis_conn) = create_redis_connection().await;

        let message_id = "test-message-multi".to_string();
        let execute_cost = 50000u64;
        let approve_cost = 30000u64;

        redis_conn
            .write_gas_cost(message_id.clone(), execute_cost, TransactionType::Execute)
            .await;
        redis_conn
            .write_gas_cost(message_id.clone(), approve_cost, TransactionType::Approve)
            .await;

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
            .write_alt_pubkey(message_id_1.clone(), alt_pubkey_1)
            .await
            .unwrap();
        redis_conn
            .write_alt_pubkey(message_id_2.clone(), alt_pubkey_2)
            .await
            .unwrap();

        let result_1 = redis_conn
            .get_alt_pubkey(message_id_1.clone())
            .await
            .unwrap();
        let result_2 = redis_conn
            .get_alt_pubkey(message_id_2.clone())
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
            .write_alt_pubkey(message_id.clone(), alt_pubkey_1)
            .await
            .unwrap();

        redis_conn
            .write_alt_pubkey(message_id.clone(), alt_pubkey_2)
            .await
            .unwrap();

        let result = redis_conn.get_alt_pubkey(message_id).await.unwrap();

        assert_eq!(result, Some(alt_pubkey_2));
    }

    #[tokio::test]
    async fn test_redis_connection_failure_graceful() {
        let (container, redis_conn) = create_redis_connection().await;

        let message_id = "test-message-failure".to_string();
        let alt_pubkey = Pubkey::new_unique();

        redis_conn
            .write_alt_pubkey(message_id.clone(), alt_pubkey)
            .await
            .unwrap();

        container.stop_with_timeout(Some(1)).await.unwrap();

        let write_result = redis_conn
            .write_alt_pubkey(message_id.clone(), alt_pubkey)
            .await;

        assert!(
            write_result.is_err(),
            "Write should fail when Redis is unavailable"
        );

        let result = redis_conn.get_alt_pubkey(message_id.clone()).await.unwrap();

        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn test_set_alt_inactive() {
        let (_container, redis_conn) = create_redis_connection().await;

        let message_id = "test-message-inactive".to_string();
        let alt_pubkey = Pubkey::new_unique();

        redis_conn
            .write_alt_pubkey(message_id.clone(), alt_pubkey)
            .await
            .unwrap();

        let all_keys = redis_conn.get_all_alt_keys().await.unwrap();
        let entry = all_keys
            .iter()
            .find(|(msg_id, _, _, _, _)| msg_id == &message_id)
            .unwrap();
        assert!(entry.4, "ALT should be active by default");
        let original_timestamp = entry.2;

        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        redis_conn
            .set_alt_inactive(message_id.clone())
            .await
            .unwrap();

        let all_keys = redis_conn.get_all_alt_keys().await.unwrap();
        let entry = all_keys
            .iter()
            .find(|(msg_id, _, _, _, _)| msg_id == &message_id)
            .unwrap();
        assert!(!entry.4, "ALT should be inactive after set_alt_inactive");

        assert!(
            entry.2 > original_timestamp,
            "Timestamp should be updated when setting to inactive"
        );

        let result = redis_conn.get_alt_pubkey(message_id.clone()).await.unwrap();
        assert_eq!(result, Some(alt_pubkey));
    }

    #[tokio::test]
    async fn test_set_alt_inactive_non_existent() {
        let (_container, redis_conn) = create_redis_connection().await;

        let message_id = "non-existent-message".to_string();

        let result = redis_conn.set_alt_inactive(message_id).await;

        assert!(
            result.is_err(),
            "Should fail when trying to set non-existent ALT to inactive"
        );
    }

    #[tokio::test]
    async fn test_set_alt_failed() {
        let (_container, redis_conn) = create_redis_connection().await;

        let message_id = "test-message-failed".to_string();
        let alt_pubkey = Pubkey::new_unique();

        redis_conn
            .set_alt_failed(message_id.clone(), alt_pubkey)
            .await
            .unwrap();

        let mut conn = redis_conn.inner().clone();
        let key = format!("FAILED:ALT:{}", message_id);
        let value: String = redis::AsyncCommands::get(&mut conn, &key).await.unwrap();
        assert_eq!(value, alt_pubkey.to_string());

        let all_keys = redis_conn.get_all_alt_keys().await.unwrap();
        assert!(!all_keys
            .iter()
            .any(|(msg_id, _, _, _, _)| msg_id == &message_id));

        let result = redis_conn.get_alt_pubkey(message_id.clone()).await.unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn test_write_multiple_failed_alts() {
        let (_container, redis_conn) = create_redis_connection().await;

        let message_id_1 = "test-failed-1".to_string();
        let message_id_2 = "test-failed-2".to_string();
        let alt_pubkey_1 = Pubkey::new_unique();
        let alt_pubkey_2 = Pubkey::new_unique();

        redis_conn
            .set_alt_failed(message_id_1.clone(), alt_pubkey_1)
            .await
            .unwrap();
        redis_conn
            .set_alt_failed(message_id_2.clone(), alt_pubkey_2)
            .await
            .unwrap();

        let mut conn = redis_conn.inner().clone();
        let key_1 = format!("FAILED:ALT:{}", message_id_1);
        let key_2 = format!("FAILED:ALT:{}", message_id_2);

        let value_1: String = redis::AsyncCommands::get(&mut conn, &key_1).await.unwrap();
        let value_2: String = redis::AsyncCommands::get(&mut conn, &key_2).await.unwrap();

        assert_eq!(value_1, alt_pubkey_1.to_string());
        assert_eq!(value_2, alt_pubkey_2.to_string());
    }
}
