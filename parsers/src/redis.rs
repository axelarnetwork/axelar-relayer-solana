use redis::{aio::ConnectionManager, AsyncCommands};
use relayer_core::utils::ThreadSafe;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, error};

#[derive(Clone)]
pub struct CostCache {
    conn: ConnectionManager,
}

impl CostCache {
    pub fn new(conn: ConnectionManager) -> Self {
        Self { conn }
    }
}

#[cfg_attr(any(test, feature = "mocks"), mockall::automock)]
#[async_trait::async_trait]
pub trait CostCacheTrait: ThreadSafe {
    async fn get_cost_by_message_id(
        &self,
        message_id: String,
        transaction_type: TransactionType,
    ) -> Result<u64, anyhow::Error>;
}

// Type alias for Arc-wrapped trait objects to make them cloneable
pub type CostCacheRef = Arc<dyn CostCacheTrait>;

#[async_trait::async_trait]
impl CostCacheTrait for CostCache {
    async fn get_cost_by_message_id(
        &self,
        message_id: String,
        transaction_type: TransactionType,
    ) -> Result<u64, anyhow::Error> {
        let key = format!("cost:{}:{}", transaction_type, message_id);
        let mut conn = self.conn.clone();
        let max_retries = 5;
        let mut backoff_duration = Duration::from_millis(500);

        for attempt in 0..max_retries {
            match conn.get::<_, Option<String>>(&key).await {
                Ok(Some(serialized)) => {
                    if let Ok(cost) = serialized.parse::<u64>() {
                        debug!("Cost for key {} is {}", key, cost);
                        return Ok(cost);
                    } else {
                        error!("Failed to parse cost for key {}: {}", key, serialized);
                        return Err(anyhow::anyhow!(
                            "Failed to parse cost for key {}: {}",
                            key,
                            serialized
                        ));
                    }
                }
                _ => {
                    if attempt < max_retries - 1 {
                        debug!(
                            "Failed to get cost from Redis for key {} (attempt {}/{}): Key not found. Retrying in {:?}...",
                            key,
                            attempt + 1,
                            max_retries,
                            backoff_duration
                        );
                        sleep(backoff_duration).await;
                        backoff_duration *= 2;
                    } else {
                        error!(
                            "Failed to get cost from Redis for key {} after {} attempts: Key not found in Redis",
                            key, max_retries
                        );
                        return Err(anyhow::anyhow!(
                            "Failed to get cost for key {} after {} attempts: Key not found in Redis",
                            key,
                            max_retries
                        ));
                    }
                }
            }
        }

        Err(anyhow::anyhow!(
            "Failed to get cost for key {}: Max retries exceeded",
            key
        ))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransactionType {
    #[serde(rename = "execute")]
    Execute,
    #[serde(rename = "approve")]
    Approve,
}

impl Display for TransactionType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            TransactionType::Execute => "execute",
            TransactionType::Approve => "approve",
        };
        write!(f, "{}", s)
    }
}
