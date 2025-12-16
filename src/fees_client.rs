use async_trait::async_trait;

use relayer_core::{error::ClientError, utils::ThreadSafe};
use solana_sdk::pubkey::Pubkey;
use statrs::statistics::{Data, OrderStatistics};

use crate::{error::FeesClientError, includer_client::IncluderClientTrait};

#[async_trait]
#[cfg_attr(test, mockall::automock)]
pub trait FeesClientTrait: ThreadSafe {
    async fn get_recent_prioritization_fees(&self, addresses: &[Pubkey], percentile: u64) -> u64;
    async fn get_prioritization_fee_percentile(
        &self,
        addresses: &[Pubkey],
        percentile: u64,
    ) -> Result<u64, FeesClientError>;
}

#[derive(Clone)]
pub struct FeesClient<IC: IncluderClientTrait> {
    includer_client: IC,
    last_n_blocks: usize,
}

impl<IC: IncluderClientTrait> FeesClient<IC> {
    pub fn new(includer_client: IC, last_n_blocks: usize) -> Result<Self, ClientError> {
        Ok(Self {
            includer_client,
            last_n_blocks,
        })
    }
}

#[async_trait]
impl<IC: IncluderClientTrait> FeesClientTrait for FeesClient<IC> {
    async fn get_recent_prioritization_fees(&self, addresses: &[Pubkey], percentile: u64) -> u64 {
        self.get_prioritization_fee_percentile(addresses, percentile)
            .await
            .unwrap_or(0)
    }

    async fn get_prioritization_fee_percentile(
        &self,
        addresses: &[Pubkey],
        percentile: u64,
    ) -> Result<u64, FeesClientError> {
        let recent_fees = self
            .includer_client
            .get_recent_prioritization_fees(addresses)
            .await
            .map_err(|e| FeesClientError::GenericError(e.to_string()))?;

        if recent_fees.is_empty() {
            return Err(FeesClientError::GenericError(
                "No recent prioritization fees found".to_string(),
            ));
        }

        let fees_to_consider: Vec<f64> = recent_fees
            .into_iter()
            .take(self.last_n_blocks)
            .map(|fee| fee.prioritization_fee as f64)
            .collect();

        if fees_to_consider.is_empty() {
            return Err(FeesClientError::GenericError(
                "No valid fees found in recent blocks".to_string(),
            ));
        }

        let mut data = Data::new(fees_to_consider);

        let percentile_value = data.percentile(percentile as usize).max(0.0);

        Ok(percentile_value.ceil() as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::IncluderClientError;
    use crate::includer_client::MockIncluderClientTrait;
    use solana_client::rpc_response::RpcPrioritizationFee;
    use solana_sdk::pubkey::Pubkey;

    fn create_fee(slot: u64, prioritization_fee: u64) -> RpcPrioritizationFee {
        RpcPrioritizationFee {
            slot,
            prioritization_fee,
        }
    }

    #[tokio::test]
    async fn test_get_prioritization_fee_percentile_50th() {
        let mut mock_client = MockIncluderClientTrait::new();

        // Fees: [100, 200, 300, 400, 500] - 50th percentile should be 300
        let fees = vec![
            create_fee(1, 100),
            create_fee(2, 200),
            create_fee(3, 300),
            create_fee(4, 400),
            create_fee(5, 500),
        ];

        mock_client
            .expect_get_recent_prioritization_fees()
            .times(1)
            .returning(move |_| {
                let fees_clone = fees.clone();
                Box::pin(async move { Ok(fees_clone) })
            });

        let fees_client = FeesClient::new(mock_client, 10).unwrap();
        let result = fees_client
            .get_prioritization_fee_percentile(&[Pubkey::new_unique()], 50)
            .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 300);
    }

    #[tokio::test]
    async fn test_get_prioritization_fee_percentile_0th() {
        let mut mock_client = MockIncluderClientTrait::new();

        let fees = vec![create_fee(1, 100), create_fee(2, 200), create_fee(3, 300)];

        mock_client
            .expect_get_recent_prioritization_fees()
            .times(1)
            .returning(move |_| {
                let fees_clone = fees.clone();
                Box::pin(async move { Ok(fees_clone) })
            });

        let fees_client = FeesClient::new(mock_client, 10).unwrap();
        let result = fees_client
            .get_prioritization_fee_percentile(&[Pubkey::new_unique()], 0)
            .await;

        assert!(result.is_ok());
        // 0th percentile should be the minimum value
        assert_eq!(result.unwrap(), 100);
    }

    #[tokio::test]
    async fn test_get_prioritization_fee_percentile_100th() {
        let mut mock_client = MockIncluderClientTrait::new();

        let fees = vec![create_fee(1, 100), create_fee(2, 200), create_fee(3, 300)];

        mock_client
            .expect_get_recent_prioritization_fees()
            .times(1)
            .returning(move |_| {
                let fees_clone = fees.clone();
                Box::pin(async move { Ok(fees_clone) })
            });

        let fees_client = FeesClient::new(mock_client, 10).unwrap();
        let result = fees_client
            .get_prioritization_fee_percentile(&[Pubkey::new_unique()], 100)
            .await;

        assert!(result.is_ok());
        // 100th percentile should be the maximum value
        assert_eq!(result.unwrap(), 300);
    }

    #[tokio::test]
    async fn test_get_prioritization_fee_percentile_empty_fees_returns_error() {
        let mut mock_client = MockIncluderClientTrait::new();

        mock_client
            .expect_get_recent_prioritization_fees()
            .times(1)
            .returning(|_| Box::pin(async { Ok(vec![]) }));

        let fees_client = FeesClient::new(mock_client, 10).unwrap();
        let result = fees_client
            .get_prioritization_fee_percentile(&[Pubkey::new_unique()], 50)
            .await;

        assert!(result.is_err());
        match result {
            Err(FeesClientError::GenericError(msg)) => {
                assert_eq!(msg, "No recent prioritization fees found");
            }
            _ => panic!("Expected GenericError with specific message"),
        }
    }

    #[tokio::test]
    async fn test_get_prioritization_fee_percentile_includer_client_error() {
        let mut mock_client = MockIncluderClientTrait::new();

        mock_client
            .expect_get_recent_prioritization_fees()
            .times(1)
            .returning(|_| {
                Box::pin(async { Err(IncluderClientError::GenericError("RPC error".to_string())) })
            });

        let fees_client = FeesClient::new(mock_client, 10).unwrap();
        let result = fees_client
            .get_prioritization_fee_percentile(&[Pubkey::new_unique()], 50)
            .await;

        assert!(result.is_err());
        match result {
            Err(FeesClientError::GenericError(msg)) => {
                assert!(msg.contains("RPC error"));
            }
            _ => panic!("Expected GenericError"),
        }
    }

    #[tokio::test]
    async fn test_get_prioritization_fee_percentile_respects_last_n_blocks() {
        let mut mock_client = MockIncluderClientTrait::new();

        // Return 10 fees, but last_n_blocks is 3
        // Fees: [1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000]
        // Only first 3 should be used: [1000, 2000, 3000]
        let fees: Vec<RpcPrioritizationFee> = (1..=10).map(|i| create_fee(i, i * 1000)).collect();

        mock_client
            .expect_get_recent_prioritization_fees()
            .times(1)
            .returning(move |_| {
                let fees_clone = fees.clone();
                Box::pin(async move { Ok(fees_clone) })
            });

        let fees_client = FeesClient::new(mock_client, 3).unwrap();
        let result = fees_client
            .get_prioritization_fee_percentile(&[Pubkey::new_unique()], 50)
            .await;

        assert!(result.is_ok());
        // 50th percentile of [1000, 2000, 3000] is 2000
        assert_eq!(result.unwrap(), 2000);
    }

    #[tokio::test]
    async fn test_get_recent_prioritization_fees_returns_zero_on_error() {
        let mut mock_client = MockIncluderClientTrait::new();

        mock_client
            .expect_get_recent_prioritization_fees()
            .times(1)
            .returning(|_| {
                Box::pin(async { Err(IncluderClientError::GenericError("RPC error".to_string())) })
            });

        let fees_client = FeesClient::new(mock_client, 10).unwrap();
        let result = fees_client
            .get_recent_prioritization_fees(&[Pubkey::new_unique()], 50)
            .await;

        // Should return 0 on error (unwrap_or(0))
        assert_eq!(result, 0);
    }

    #[tokio::test]
    async fn test_get_recent_prioritization_fees_returns_zero_on_empty_fees() {
        let mut mock_client = MockIncluderClientTrait::new();

        mock_client
            .expect_get_recent_prioritization_fees()
            .times(1)
            .returning(|_| Box::pin(async { Ok(vec![]) }));

        let fees_client = FeesClient::new(mock_client, 10).unwrap();
        let result = fees_client
            .get_recent_prioritization_fees(&[Pubkey::new_unique()], 50)
            .await;

        // Should return 0 when no fees found
        assert_eq!(result, 0);
    }

    #[tokio::test]
    async fn test_get_prioritization_fee_percentile_with_zero_fees() {
        let mut mock_client = MockIncluderClientTrait::new();

        // All zero fees
        let fees = vec![create_fee(1, 0), create_fee(2, 0), create_fee(3, 0)];

        mock_client
            .expect_get_recent_prioritization_fees()
            .times(1)
            .returning(move |_| {
                let fees_clone = fees.clone();
                Box::pin(async move { Ok(fees_clone) })
            });

        let fees_client = FeesClient::new(mock_client, 10).unwrap();
        let result = fees_client
            .get_prioritization_fee_percentile(&[Pubkey::new_unique()], 50)
            .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_get_prioritization_fee_percentile_with_large_fees() {
        let mut mock_client = MockIncluderClientTrait::new();

        // Large fee values
        let fees = vec![
            create_fee(1, 1_000_000_000),
            create_fee(2, 2_000_000_000),
            create_fee(3, 3_000_000_000),
        ];

        mock_client
            .expect_get_recent_prioritization_fees()
            .times(1)
            .returning(move |_| {
                let fees_clone = fees.clone();
                Box::pin(async move { Ok(fees_clone) })
            });

        let fees_client = FeesClient::new(mock_client, 10).unwrap();
        let result = fees_client
            .get_prioritization_fee_percentile(&[Pubkey::new_unique()], 50)
            .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 2_000_000_000);
    }

    #[tokio::test]
    async fn test_get_prioritization_fee_percentile_75th() {
        let mut mock_client = MockIncluderClientTrait::new();

        // Fees: [100, 200, 300, 400]
        let fees = vec![
            create_fee(1, 100),
            create_fee(2, 200),
            create_fee(3, 300),
            create_fee(4, 400),
        ];

        mock_client
            .expect_get_recent_prioritization_fees()
            .times(1)
            .returning(move |_| {
                let fees_clone = fees.clone();
                Box::pin(async move { Ok(fees_clone) })
            });

        let fees_client = FeesClient::new(mock_client, 10).unwrap();
        let result = fees_client
            .get_prioritization_fee_percentile(&[Pubkey::new_unique()], 75)
            .await;

        assert!(result.is_ok());
        // 75th percentile of [100, 200, 300, 400] using statrs linear interpolation
        assert_eq!(result.unwrap(), 359);
    }
}
