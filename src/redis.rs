use crate::error::RedisInterfaceError;
use async_trait::async_trait;
use redis::{AsyncTypedCommands, SetExpiry, SetOptions};
use relayer_core::utils::ThreadSafe;
use solana_transaction_parser::redis::TransactionType;
use tracing::{debug, warn};

use redis::aio::ConnectionManager;

const GAS_COST_EXPIRATION: u64 = 604800; // one week
const CU_PRICE_EXPIRATION: u64 = 600; // ten minutes

#[cfg_attr(any(test, feature = "test-mocks"), mockall::automock)]
#[async_trait]
pub trait RedisConnectionTrait: ThreadSafe {
    fn inner(&self) -> &ConnectionManager;
    async fn add_gas_cost_for_task_id(
        &self,
        task_id: String,
        gas_cost: u64,
        transaction_type: TransactionType,
    );
    async fn get_gas_cost_for_task_id(
        &self,
        task_id: String,
        transaction_type: TransactionType,
    ) -> Result<u64, RedisInterfaceError>;
    async fn write_gas_cost_for_message_id(
        &self,
        message_id: String,
        gas_cost: u64,
        transaction_type: TransactionType,
    );
    async fn get_cu_price(&self) -> Result<Option<u64>, RedisInterfaceError>;
    async fn set_cu_price(&self, cu_price: u64) -> Result<(), RedisInterfaceError>;
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

    async fn add_gas_cost_for_task_id(
        &self,
        task_id: String,
        gas_cost: u64,
        transaction_type: TransactionType,
    ) {
        if gas_cost == 0 {
            debug!("Gas cost is 0 for task id: {}, skipping write", task_id);
            return;
        }
        debug!("Adding gas cost for task id: {} to Redis", task_id);
        let mut redis_conn = self.conn.clone();
        let set_opts = SetOptions::default().with_expiration(SetExpiry::EX(GAS_COST_EXPIRATION));
        let key = format!("task_cost:{}:{}", transaction_type, task_id);

        // Get existing cost, default to 0 if not found
        let existing_cost =
            match redis::AsyncCommands::get::<_, Option<String>>(&mut redis_conn, &key).await {
                Ok(Some(serialized)) => serialized.parse::<u64>().unwrap_or(0),
                Ok(None) => 0,
                Err(_) => 0, // If there's an error reading, assume 0
            };

        let total_cost = existing_cost.saturating_add(gas_cost);

        let result = redis_conn
            .set_options(key.clone(), total_cost, set_opts)
            .await;

        match result {
            Ok(_) => {
                debug!(
                    "Gas cost for added to Redis successfully, key: {}, existing: {}, added: {}, total: {}",
                    key, existing_cost, gas_cost, total_cost
                );
            }
            Err(e) => {
                warn!("Failed to write gas cost to Redis: {}", e);
            }
        }
    }

    async fn get_gas_cost_for_task_id(
        &self,
        task_id: String,
        transaction_type: TransactionType,
    ) -> Result<u64, RedisInterfaceError> {
        let key = format!("task_cost:{}:{}", transaction_type, task_id);
        let mut conn = self.conn.clone();
        match redis::AsyncCommands::get::<_, Option<String>>(&mut conn, &key).await {
            Ok(Some(serialized)) => {
                if let Ok(cost) = serialized.parse::<u64>() {
                    debug!("Cost for key {} is {}", key, cost);
                    return Ok(cost);
                } else {
                    return Err(RedisInterfaceError::GenericError(format!(
                        "Failed to parse cost for key {}: {}",
                        key, serialized
                    )));
                }
            }
            Ok(None) => {
                return Err(RedisInterfaceError::GenericError(format!(
                    "Failed to get cost for key {}: Key not found in Redis",
                    key
                )));
            }
            Err(e) => {
                return Err(RedisInterfaceError::GenericError(format!(
                    "Failed to get cost for key {}: {}",
                    key, e
                )));
            }
        }
    }

    async fn write_gas_cost_for_message_id(
        &self,
        message_id: String,
        gas_cost: u64,
        transaction_type: TransactionType,
    ) {
        if gas_cost == 0 {
            debug!(
                "Gas cost is 0 for message id: {}, skipping write",
                message_id
            );
            return;
        }
        debug!(
            "Writing gas cost for message id: {}, gas cost: {}",
            message_id, gas_cost
        );
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

    async fn get_cu_price(&self) -> Result<Option<u64>, RedisInterfaceError> {
        let mut redis_conn = self.conn.clone();
        let key = "cu_price";
        let result: Option<String> = redis::AsyncCommands::get(&mut redis_conn, key)
            .await
            .map_err(|e| {
                RedisInterfaceError::GenericError(format!(
                    "Failed to get CU price from Redis: {}",
                    e
                ))
            })?;
        Ok(result.map(|s| s.parse::<u64>().unwrap_or(0)))
    }

    async fn set_cu_price(&self, cu_price: u64) -> Result<(), RedisInterfaceError> {
        let mut redis_conn = self.conn.clone();
        let key = "cu_price";
        let set_opts = SetOptions::default().with_expiration(SetExpiry::EX(CU_PRICE_EXPIRATION));
        redis_conn
            .set_options(key, cu_price, set_opts)
            .await
            .map_err(|e| {
                RedisInterfaceError::GenericError(format!("Failed to set CU price in Redis: {}", e))
            })?;
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
        // Retry container creation to handle transient Docker/testcontainers issues in CI
        let mut retries = 0;
        let max_retries = 3;
        let mut delay = Duration::from_millis(500);

        loop {
            match GenericImage::new("redis", "7.2.4")
                .with_exposed_port(6379.tcp())
                .with_wait_for(WaitFor::message_on_stdout("Ready to accept connections"))
                .start()
                .await
            {
                Ok(container) => {
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

                    return (container, redis_conn);
                }
                Err(e) => {
                    if retries >= max_retries {
                        panic!(
                            "Failed to create Redis container after {} retries: {:?}",
                            max_retries, e
                        );
                    }
                    eprintln!("Failed to create Redis container (attempt {}/{}): {:?}. Retrying in {:?}...", retries + 1, max_retries + 1, e, delay);
                    tokio::time::sleep(delay).await;
                    retries += 1;
                    delay = delay.mul_f32(2.0);
                }
            }
        }
    }

    #[tokio::test]
    async fn test_write_and_read_gas_cost_execute() {
        let (_container, redis_conn) = create_redis_connection().await;

        let message_id = "test-message-123".to_string();
        let gas_cost = 50000u64;

        redis_conn
            .write_gas_cost_for_message_id(message_id.clone(), gas_cost, TransactionType::Execute)
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
            .write_gas_cost_for_message_id(message_id.clone(), gas_cost, TransactionType::Approve)
            .await;

        let mut conn = redis_conn.inner().clone();
        let key = format!("cost:{}:{}", TransactionType::Approve, message_id);
        let stored_value: Option<String> = redis::AsyncCommands::get(&mut conn, key).await.unwrap();

        assert_eq!(stored_value, Some(gas_cost.to_string()));
    }

    #[tokio::test]
    async fn test_gas_cost_expiration() {
        let (_container, redis_conn) = create_redis_connection().await;

        let message_id = "test-message-gas-expiration".to_string();
        let gas_cost = 75000u64;

        redis_conn
            .write_gas_cost_for_message_id(message_id.clone(), gas_cost, TransactionType::Execute)
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
            .write_gas_cost_for_message_id(
                message_id.clone(),
                execute_cost,
                TransactionType::Execute,
            )
            .await;
        redis_conn
            .write_gas_cost_for_message_id(
                message_id.clone(),
                approve_cost,
                TransactionType::Approve,
            )
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
    async fn test_redis_connection_failure_graceful() {
        let (container, redis_conn) = create_redis_connection().await;

        redis_conn.set_cu_price(1_000).await.unwrap();

        container.stop_with_timeout(Some(1)).await.unwrap();

        assert!(
            redis_conn.set_cu_price(2_000).await.is_err(),
            "Write should fail when Redis is unavailable"
        );
        assert!(
            redis_conn.get_cu_price().await.is_err(),
            "Read should fail when Redis is unavailable"
        );
    }

    #[tokio::test]
    async fn test_add_gas_cost_for_task_id_first_time() {
        let (_container, redis_conn) = create_redis_connection().await;

        let task_id = "test-task-123".to_string();
        let gas_cost = 50000u64;

        redis_conn
            .add_gas_cost_for_task_id(task_id.clone(), gas_cost, TransactionType::Execute)
            .await;

        let mut conn = redis_conn.inner().clone();
        let key = format!("task_cost:{}:{}", TransactionType::Execute, task_id);
        let stored_value: Option<String> = redis::AsyncCommands::get(&mut conn, key).await.unwrap();

        assert_eq!(stored_value, Some(gas_cost.to_string()));
    }

    #[tokio::test]
    async fn test_add_gas_cost_for_task_id_accumulates() {
        let (_container, redis_conn) = create_redis_connection().await;

        let task_id = "test-task-456".to_string();
        let gas_cost_1 = 30000u64;
        let gas_cost_2 = 20000u64;
        let expected_total = gas_cost_1 + gas_cost_2;

        // First addition
        redis_conn
            .add_gas_cost_for_task_id(task_id.clone(), gas_cost_1, TransactionType::Execute)
            .await;

        // Second addition - should accumulate
        redis_conn
            .add_gas_cost_for_task_id(task_id.clone(), gas_cost_2, TransactionType::Execute)
            .await;

        let mut conn = redis_conn.inner().clone();
        let key = format!("task_cost:{}:{}", TransactionType::Execute, task_id);
        let stored_value: Option<String> = redis::AsyncCommands::get(&mut conn, key).await.unwrap();

        assert_eq!(stored_value, Some(expected_total.to_string()));
    }

    #[tokio::test]
    async fn test_add_gas_cost_for_task_id_multiple_additions() {
        let (_container, redis_conn) = create_redis_connection().await;

        let task_id = "test-task-789".to_string();
        let costs = vec![10000u64, 20000u64, 30000u64, 40000u64];
        let expected_total: u64 = costs.iter().sum();

        for cost in costs {
            redis_conn
                .add_gas_cost_for_task_id(task_id.clone(), cost, TransactionType::Execute)
                .await;
        }

        let mut conn = redis_conn.inner().clone();
        let key = format!("task_cost:{}:{}", TransactionType::Execute, task_id);
        let stored_value: Option<String> = redis::AsyncCommands::get(&mut conn, key).await.unwrap();

        assert_eq!(stored_value, Some(expected_total.to_string()));
    }

    #[tokio::test]
    async fn test_get_gas_cost_for_task_id_exists() {
        let (_container, redis_conn) = create_redis_connection().await;

        let task_id = "test-task-get-123".to_string();
        let gas_cost = 75000u64;

        // First add the cost
        redis_conn
            .add_gas_cost_for_task_id(task_id.clone(), gas_cost, TransactionType::Execute)
            .await;

        // Then retrieve it
        let retrieved_cost = redis_conn
            .get_gas_cost_for_task_id(task_id.clone(), TransactionType::Execute)
            .await
            .unwrap();

        assert_eq!(retrieved_cost, gas_cost);
    }

    #[tokio::test]
    async fn test_get_gas_cost_for_task_id_not_found() {
        let (_container, redis_conn) = create_redis_connection().await;

        let task_id = "non-existent-task".to_string();

        let result = redis_conn
            .get_gas_cost_for_task_id(task_id, TransactionType::Execute)
            .await;

        assert!(result.is_err());
        if let Err(RedisInterfaceError::GenericError(msg)) = result {
            assert!(msg.contains("Key not found in Redis"));
        } else {
            panic!("Expected GenericError with 'Key not found' message");
        }
    }

    #[tokio::test]
    async fn test_get_gas_cost_for_task_id_different_transaction_types() {
        let (_container, redis_conn) = create_redis_connection().await;

        let task_id = "test-task-types".to_string();
        let execute_cost = 50000u64;
        let approve_cost = 30000u64;

        redis_conn
            .add_gas_cost_for_task_id(task_id.clone(), execute_cost, TransactionType::Execute)
            .await;
        redis_conn
            .add_gas_cost_for_task_id(task_id.clone(), approve_cost, TransactionType::Approve)
            .await;

        let retrieved_execute = redis_conn
            .get_gas_cost_for_task_id(task_id.clone(), TransactionType::Execute)
            .await
            .unwrap();
        let retrieved_approve = redis_conn
            .get_gas_cost_for_task_id(task_id.clone(), TransactionType::Approve)
            .await
            .unwrap();

        assert_eq!(retrieved_execute, execute_cost);
        assert_eq!(retrieved_approve, approve_cost);
    }

    #[tokio::test]
    async fn test_add_gas_cost_for_task_id_different_transaction_types() {
        let (_container, redis_conn) = create_redis_connection().await;

        let task_id = "test-task-multi-type".to_string();
        let execute_cost_1 = 25000u64;
        let execute_cost_2 = 15000u64;
        let approve_cost = 10000u64;

        redis_conn
            .add_gas_cost_for_task_id(task_id.clone(), execute_cost_1, TransactionType::Execute)
            .await;
        redis_conn
            .add_gas_cost_for_task_id(task_id.clone(), execute_cost_2, TransactionType::Execute)
            .await;
        redis_conn
            .add_gas_cost_for_task_id(task_id.clone(), approve_cost, TransactionType::Approve)
            .await;

        let retrieved_execute = redis_conn
            .get_gas_cost_for_task_id(task_id.clone(), TransactionType::Execute)
            .await
            .unwrap();
        let retrieved_approve = redis_conn
            .get_gas_cost_for_task_id(task_id.clone(), TransactionType::Approve)
            .await
            .unwrap();

        assert_eq!(retrieved_execute, execute_cost_1 + execute_cost_2);
        assert_eq!(retrieved_approve, approve_cost);
    }

    #[tokio::test]
    async fn test_add_gas_cost_for_task_id_expiration() {
        let (_container, redis_conn) = create_redis_connection().await;

        let task_id = "test-task-expiration".to_string();
        let gas_cost = 60000u64;

        redis_conn
            .add_gas_cost_for_task_id(task_id.clone(), gas_cost, TransactionType::Execute)
            .await;

        let mut conn = redis_conn.inner().clone();
        let key = format!("task_cost:{}:{}", TransactionType::Execute, task_id);
        let ttl: i64 = redis::AsyncCommands::ttl(&mut conn, key).await.unwrap();

        assert!(ttl > 0, "TTL should be positive");
        assert!(ttl <= GAS_COST_EXPIRATION as i64);
        assert!(ttl > (GAS_COST_EXPIRATION as i64 - 10));
    }

    #[tokio::test]
    async fn test_add_gas_cost_for_task_id_zero_cost_returns_early() {
        // Test that when add_gas_cost_for_task_id is called with 0,
        // it returns early and doesn't write to Redis, leaving existing cost unchanged.
        // This happens in two scenarios:
        // 1. AccountInUseError in InitializePayloadVerificationSession (cost is 0)
        // 2. SlotAlreadyVerifiedError in VerifySignatures (total_cost is 0)
        let (_container, redis_conn) = create_redis_connection().await;

        let task_id = "test-task-zero-cost-returns-early".to_string();
        let existing_cost = 5000u64;

        // First add some existing cost
        redis_conn
            .add_gas_cost_for_task_id(task_id.clone(), existing_cost, TransactionType::Approve)
            .await;
        // Simulate calling with 0 cost (e.g., AccountInUseError or SlotAlreadyVerifiedError)
        // This should return early and not write to Redis
        redis_conn
            .add_gas_cost_for_task_id(task_id.clone(), 0, TransactionType::Approve)
            .await;

        // Verify the cost remains unchanged (function returns early, no write occurs)
        let mut conn = redis_conn.inner().clone();
        let key = format!("task_cost:{}:{}", TransactionType::Approve, task_id);
        let stored_value: Option<String> = redis::AsyncCommands::get(&mut conn, key).await.unwrap();

        assert_eq!(stored_value, Some(existing_cost.to_string()));
    }

    #[tokio::test]
    async fn test_add_gas_cost_for_task_id_slot_already_verified_error() {
        // Test that when SlotAlreadyVerifiedError occurs in VerifySignatures,
        // no cost is added for that signature (cost remains 0 or doesn't accumulate)
        let (_container, redis_conn) = create_redis_connection().await;

        let task_id = "test-task-slot-already-verified".to_string();
        let init_cost = 3000u64;

        // First add initialization cost
        redis_conn
            .add_gas_cost_for_task_id(task_id.clone(), init_cost, TransactionType::Approve)
            .await;

        // Simulate SlotAlreadyVerifiedError: when signature is already verified,
        // no cost is added (verify_signatures continues without adding cost)
        // So we add 0 cost to simulate this scenario
        redis_conn
            .add_gas_cost_for_task_id(task_id.clone(), 0, TransactionType::Approve)
            .await;

        // Verify the cost remains as init_cost (0 + init_cost = init_cost)
        let mut conn = redis_conn.inner().clone();
        let key = format!("task_cost:{}:{}", TransactionType::Approve, task_id);
        let stored_value: Option<String> = redis::AsyncCommands::get(&mut conn, key).await.unwrap();

        assert_eq!(stored_value, Some(init_cost.to_string()));
    }

    #[tokio::test]
    async fn test_write_gas_cost_for_message_id_account_in_use_error_approve() {
        // Test that when AccountInUseError occurs in ApproveMessages,
        // write_gas_cost_for_message_id is called with 0 (or overhead + 0 = overhead if overhead > 0),
        // but if the total is 0, it should return early and not write to Redis
        let (_container, redis_conn) = create_redis_connection().await;

        let message_id = "test-message-account-in-use-approve".to_string();

        // Simulate AccountInUseError in ApproveMessages: write with 0 cost
        // This should return early and not write to Redis
        redis_conn
            .write_gas_cost_for_message_id(message_id.clone(), 0, TransactionType::Approve)
            .await;

        // Verify nothing was written to Redis
        let mut conn = redis_conn.inner().clone();
        let key = format!("cost:{}:{}", TransactionType::Approve, message_id);
        let stored_value: Option<String> = redis::AsyncCommands::get(&mut conn, key).await.unwrap();

        assert_eq!(
            stored_value, None,
            "No cost should be written when gas_cost is 0"
        );
    }

    #[tokio::test]
    async fn test_write_gas_cost_for_message_id_account_in_use_error_with_overhead() {
        // Test that when AccountInUseError occurs in ApproveMessages but overhead > 0,
        // write_gas_cost_for_message_id is called with overhead (0 + overhead = overhead)
        // and should write the overhead cost
        let (_container, redis_conn) = create_redis_connection().await;

        let message_id = "test-message-account-in-use-with-overhead".to_string();
        let overhead_cost = 1000u64;

        // Simulate AccountInUseError in ApproveMessages with overhead:
        // gas_cost is 0, but overhead_cost_per_message is overhead_cost
        // So write_gas_cost_for_message_id is called with 0 + overhead = overhead
        redis_conn
            .write_gas_cost_for_message_id(
                message_id.clone(),
                overhead_cost,
                TransactionType::Approve,
            )
            .await;

        // Verify overhead cost was written to Redis
        let mut conn = redis_conn.inner().clone();
        let key = format!("cost:{}:{}", TransactionType::Approve, message_id);
        let stored_value: Option<String> = redis::AsyncCommands::get(&mut conn, key).await.unwrap();

        assert_eq!(stored_value, Some(overhead_cost.to_string()));
    }

    #[tokio::test]
    async fn test_add_gas_cost_for_task_id_zero_cost_initial() {
        // Test that adding 0 cost initially returns early and does not write to Redis
        let (_container, redis_conn) = create_redis_connection().await;

        let task_id = "test-task-zero-initial".to_string();

        // Add 0 cost initially (simulating AccountInUseError or SlotAlreadyVerifiedError)
        // This should return early and not write to Redis
        redis_conn
            .add_gas_cost_for_task_id(task_id.clone(), 0, TransactionType::Approve)
            .await;

        // Verify nothing was written to Redis (function returns early)
        let mut conn = redis_conn.inner().clone();
        let key = format!("task_cost:{}:{}", TransactionType::Approve, task_id);
        let stored_value: Option<String> = redis::AsyncCommands::get(&mut conn, key).await.unwrap();

        assert_eq!(
            stored_value, None,
            "No cost should be written when gas_cost is 0"
        );
    }

    #[tokio::test]
    async fn test_set_and_get_cu_price() {
        let (_container, redis_conn) = create_redis_connection().await;

        let cu_price = 1000000u64;

        // Set the CU price
        redis_conn.set_cu_price(cu_price).await.unwrap();

        // Get the CU price and verify it matches
        let result = redis_conn.get_cu_price().await.unwrap();
        assert_eq!(result, Some(cu_price));
    }

    #[tokio::test]
    async fn test_get_cu_price_not_found() {
        let (_container, redis_conn) = create_redis_connection().await;

        // Try to get CU price when it doesn't exist
        let result = redis_conn.get_cu_price().await.unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn test_cu_price_expiration() {
        let (_container, redis_conn) = create_redis_connection().await;

        let cu_price = 12345u64;

        redis_conn.set_cu_price(cu_price).await.unwrap();

        let mut conn = redis_conn.inner().clone();
        let ttl: i64 = redis::AsyncCommands::ttl(&mut conn, "cu_price")
            .await
            .unwrap();

        assert!(ttl > 0, "TTL should be positive");
        assert!(ttl <= CU_PRICE_EXPIRATION as i64);
        assert!(ttl > (CU_PRICE_EXPIRATION as i64 - 10));
    }

    #[tokio::test]
    async fn test_set_and_update_cu_price() {
        let (_container, redis_conn) = create_redis_connection().await;

        let initial_price = 500000u64;
        let updated_price = 750000u64;

        // Set initial CU price
        redis_conn.set_cu_price(initial_price).await.unwrap();
        let result = redis_conn.get_cu_price().await.unwrap();
        assert_eq!(result, Some(initial_price));

        // Update the CU price
        redis_conn.set_cu_price(updated_price).await.unwrap();
        let result = redis_conn.get_cu_price().await.unwrap();
        assert_eq!(result, Some(updated_price));
    }
}
