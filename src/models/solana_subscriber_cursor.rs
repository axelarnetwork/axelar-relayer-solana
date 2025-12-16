use anyhow::Result;
use async_trait::async_trait;
use relayer_core::utils::ThreadSafe;
use sqlx::PgPool;

#[async_trait]
#[cfg_attr(any(test, feature = "test-mocks"), mockall::automock)]
pub trait SubscriberCursor: ThreadSafe {
    // Subscriber functions
    async fn store_latest_signature(
        &self,
        context: String,
        signature: String,
        account_type: AccountPollerEnum,
    ) -> Result<()>;
    async fn get_latest_signature(
        &self,
        context: String,
        account_type: AccountPollerEnum,
    ) -> Result<Option<String>>;
}

#[derive(Clone, Debug, sqlx::Type, PartialEq)]
#[sqlx(type_name = "account_poller_enum")]
pub enum AccountPollerEnum {
    #[sqlx(rename = "gas_service")]
    GasService,
    #[sqlx(rename = "gateway")]
    Gateway,
    #[sqlx(rename = "its")]
    ITS,
}

#[derive(Clone, Debug)]
pub struct PostgresDB {
    pool: PgPool,
}

impl PostgresDB {
    pub async fn new(url: &str) -> Result<Self> {
        let pool = PgPool::connect(url).await?;
        Ok(Self { pool })
    }

    pub fn inner(&self) -> &PgPool {
        &self.pool
    }
}

#[async_trait]
impl SubscriberCursor for PostgresDB {
    async fn store_latest_signature(
        &self,
        context: String,
        signature: String,
        account_type: AccountPollerEnum,
    ) -> Result<()> {
        let query =
            "INSERT INTO solana_subscriber_cursors (context, signature, account_type) VALUES ($1, $2, $3) ON CONFLICT (context, account_type) DO UPDATE SET signature = $2, updated_at = now() RETURNING context, signature";

        sqlx::query(query)
            .bind(context)
            .bind(signature)
            .bind(account_type)
            .execute(self.inner())
            .await?;
        Ok(())
    }

    async fn get_latest_signature(
        &self,
        context: String,
        account_type: AccountPollerEnum,
    ) -> Result<Option<String>> {
        let query =
            "SELECT signature FROM solana_subscriber_cursors WHERE context = $1 AND account_type = $2";
        let signature = sqlx::query_scalar(query)
            .bind(context)
            .bind(account_type)
            .fetch_optional(self.inner())
            .await?;
        Ok(signature)
    }
}
