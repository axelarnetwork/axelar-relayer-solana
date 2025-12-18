use anyhow::Result;
use async_trait::async_trait;
use relayer_core::utils::ThreadSafe;

use crate::models::solana_subscriber_cursor::PostgresDB;

#[async_trait]
#[cfg_attr(any(test, feature = "test-mocks"), mockall::automock)]
pub trait RefundsModel: ThreadSafe {
    async fn find(
        &self,
        refund_id: String,
    ) -> Result<Option<(String, chrono::DateTime<chrono::Utc>)>>;
    async fn upsert(&self, refund_id: String, signature: String) -> Result<()>;
    async fn delete(&self, refund_id: String) -> Result<()>;
}

#[async_trait]
impl RefundsModel for PostgresDB {
    async fn find(
        &self,
        refund_id: String,
    ) -> Result<Option<(String, chrono::DateTime<chrono::Utc>)>> {
        let query = "SELECT signature, updated_at FROM solana_refunds WHERE refund_id = $1";
        let result = sqlx::query_as::<_, (String, chrono::DateTime<chrono::Utc>)>(query)
            .bind(refund_id)
            .fetch_optional(self.inner())
            .await?;
        Ok(result)
    }

    async fn upsert(&self, refund_id: String, signature: String) -> Result<()> {
        let query = "INSERT INTO solana_refunds (refund_id, signature, updated_at) VALUES ($1, $2, NOW()) ON CONFLICT (refund_id) DO UPDATE SET signature = $2, updated_at = NOW()";
        sqlx::query(query)
            .bind(refund_id)
            .bind(signature)
            .execute(self.inner())
            .await?;
        Ok(())
    }

    async fn delete(&self, refund_id: String) -> Result<()> {
        let query = "DELETE FROM solana_refunds WHERE refund_id = $1";
        sqlx::query(query)
            .bind(refund_id)
            .execute(self.inner())
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{PostgresDB, RefundsModel};
    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::postgres;

    #[tokio::test]
    async fn test_find_and_upsert_refund() {
        let init_sql = format!(
            "{}\n",
            include_str!("../../migrations/0008_solana_refunds.sql"),
        );
        let container = postgres::Postgres::default()
            .with_init_sql(init_sql.into_bytes())
            .start()
            .await
            .unwrap();
        let connection_string = format!(
            "postgres://postgres:postgres@{}:{}/postgres",
            container.get_host().await.unwrap(),
            container.get_host_port_ipv4(5432).await.unwrap()
        );
        let model = PostgresDB::new(&connection_string).await.unwrap();

        let refund_id = "test-refund-123".to_string();
        let signature = "test-signature-abc".to_string();

        model
            .upsert(refund_id.clone(), signature.clone())
            .await
            .unwrap();

        let (retrieved, _) = model.find(refund_id.clone()).await.unwrap().unwrap();
        assert_eq!(retrieved, signature);
    }

    #[tokio::test]
    async fn test_find_not_found() {
        let init_sql = format!(
            "{}\n",
            include_str!("../../migrations/0008_solana_refunds.sql"),
        );
        let container = postgres::Postgres::default()
            .with_init_sql(init_sql.into_bytes())
            .start()
            .await
            .unwrap();
        let connection_string = format!(
            "postgres://postgres:postgres@{}:{}/postgres",
            container.get_host().await.unwrap(),
            container.get_host_port_ipv4(5432).await.unwrap()
        );
        let model = PostgresDB::new(&connection_string).await.unwrap();

        let refund_id = "non-existent-refund".to_string();

        let retrieved = model.find(refund_id).await.unwrap();
        assert!(retrieved.is_none());
    }

    #[tokio::test]
    async fn test_upsert_multiple_refunds() {
        let init_sql = format!(
            "{}\n",
            include_str!("../../migrations/0008_solana_refunds.sql"),
        );
        let container = postgres::Postgres::default()
            .with_init_sql(init_sql.into_bytes())
            .start()
            .await
            .unwrap();
        let connection_string = format!(
            "postgres://postgres:postgres@{}:{}/postgres",
            container.get_host().await.unwrap(),
            container.get_host_port_ipv4(5432).await.unwrap()
        );
        let model = PostgresDB::new(&connection_string).await.unwrap();

        model
            .upsert("test-refund-1".to_string(), "signature-1".to_string())
            .await
            .unwrap();
        model
            .upsert("test-refund-2".to_string(), "signature-2".to_string())
            .await
            .unwrap();
        model
            .upsert("test-refund-3".to_string(), "signature-3".to_string())
            .await
            .unwrap();

        let (retrieved_1, _) = model
            .find("test-refund-1".to_string())
            .await
            .unwrap()
            .unwrap();
        let (retrieved_2, _) = model
            .find("test-refund-2".to_string())
            .await
            .unwrap()
            .unwrap();
        let (retrieved_3, _) = model
            .find("test-refund-3".to_string())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(retrieved_1, "signature-1");
        assert_eq!(retrieved_2, "signature-2");
        assert_eq!(retrieved_3, "signature-3");
    }

    #[tokio::test]
    async fn test_upsert_overwrite() {
        let init_sql = format!(
            "{}\n",
            include_str!("../../migrations/0008_solana_refunds.sql"),
        );
        let container = postgres::Postgres::default()
            .with_init_sql(init_sql.into_bytes())
            .start()
            .await
            .unwrap();
        let connection_string = format!(
            "postgres://postgres:postgres@{}:{}/postgres",
            container.get_host().await.unwrap(),
            container.get_host_port_ipv4(5432).await.unwrap()
        );
        let model = PostgresDB::new(&connection_string).await.unwrap();

        let refund_id = "test-refund-overwrite".to_string();
        let original_signature = "original-signature".to_string();
        let updated_signature = "updated-signature".to_string();

        model
            .upsert(refund_id.clone(), original_signature.clone())
            .await
            .unwrap();

        let (first_retrieved, _) = model.find(refund_id.clone()).await.unwrap().unwrap();
        assert_eq!(first_retrieved, original_signature);

        model
            .upsert(refund_id.clone(), updated_signature.clone())
            .await
            .unwrap();

        let (second_retrieved, _) = model.find(refund_id.clone()).await.unwrap().unwrap();
        assert_eq!(second_retrieved, updated_signature);
        assert_ne!(second_retrieved, original_signature);
    }

    #[tokio::test]
    async fn test_delete_refund() {
        let init_sql = format!(
            "{}\n",
            include_str!("../../migrations/0008_solana_refunds.sql"),
        );
        let container = postgres::Postgres::default()
            .with_init_sql(init_sql.into_bytes())
            .start()
            .await
            .unwrap();
        let connection_string = format!(
            "postgres://postgres:postgres@{}:{}/postgres",
            container.get_host().await.unwrap(),
            container.get_host_port_ipv4(5432).await.unwrap()
        );
        let model = PostgresDB::new(&connection_string).await.unwrap();

        let refund_id = "test-refund-delete".to_string();
        let signature = "signature-to-delete".to_string();

        model
            .upsert(refund_id.clone(), signature.clone())
            .await
            .unwrap();

        let retrieved = model.find(refund_id.clone()).await.unwrap();
        assert!(retrieved.is_some());

        model.delete(refund_id.clone()).await.unwrap();

        let deleted = model.find(refund_id).await.unwrap();
        assert!(deleted.is_none());
    }

    #[tokio::test]
    async fn test_crud_operations() {
        let init_sql = format!(
            "{}\n",
            include_str!("../../migrations/0008_solana_refunds.sql"),
        );
        let container = postgres::Postgres::default()
            .with_init_sql(init_sql.into_bytes())
            .start()
            .await
            .unwrap();
        let connection_string = format!(
            "postgres://postgres:postgres@{}:{}/postgres",
            container.get_host().await.unwrap(),
            container.get_host_port_ipv4(5432).await.unwrap()
        );
        let model = PostgresDB::new(&connection_string).await.unwrap();

        let refund_id = "test-crud-refund".to_string();
        let signature = "test-signature".to_string();

        model
            .upsert(refund_id.clone(), signature.clone())
            .await
            .unwrap();

        let (found, _) = model.find(refund_id.clone()).await.unwrap().unwrap();
        assert_eq!(found, signature);

        let updated_signature = "updated-signature".to_string();
        model
            .upsert(refund_id.clone(), updated_signature.clone())
            .await
            .unwrap();

        let (found_after_update, _) = model.find(refund_id.clone()).await.unwrap().unwrap();
        assert_eq!(found_after_update, updated_signature);

        model.delete(refund_id.clone()).await.unwrap();

        let deleted = model.find(refund_id).await.unwrap();
        assert!(deleted.is_none());
    }

    #[tokio::test]
    async fn test_upsert_empty_signature() {
        let init_sql = format!(
            "{}\n",
            include_str!("../../migrations/0008_solana_refunds.sql"),
        );
        let container = postgres::Postgres::default()
            .with_init_sql(init_sql.into_bytes())
            .start()
            .await
            .unwrap();
        let connection_string = format!(
            "postgres://postgres:postgres@{}:{}/postgres",
            container.get_host().await.unwrap(),
            container.get_host_port_ipv4(5432).await.unwrap()
        );
        let model = PostgresDB::new(&connection_string).await.unwrap();

        let refund_id = "empty-signature-refund".to_string();
        let empty_signature = String::new();

        model
            .upsert(refund_id.clone(), empty_signature.clone())
            .await
            .unwrap();

        let (retrieved, _) = model.find(refund_id.clone()).await.unwrap().unwrap();
        assert_eq!(retrieved, empty_signature);
    }
}
