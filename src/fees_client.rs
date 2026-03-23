use async_trait::async_trait;

use relayer_core::{error::ClientError, utils::ThreadSafe};
use statrs::statistics::{Data, OrderStatistics};
use tracing::warn;

use crate::{error::FeesClientError, includer_client::IncluderClientTrait};

const MAX_CU_PRICE: u64 = 10_000;

/// Fallback CU price when the percentile computation yields zero.
const FALLBACK_CU_PRICE: u64 = 1_000;

#[async_trait]
#[cfg_attr(test, mockall::automock)]
pub trait FeesClientTrait: ThreadSafe {
    async fn get_recent_prioritization_fees(&self, percentile: u64) -> u64;
    async fn get_prioritization_fee_percentile(
        &self,
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
    async fn get_recent_prioritization_fees(&self, percentile: u64) -> u64 {
        self.get_prioritization_fee_percentile(percentile)
            .await
            .unwrap_or(FALLBACK_CU_PRICE)
    }

    async fn get_prioritization_fee_percentile(
        &self,
        percentile: u64,
    ) -> Result<u64, FeesClientError> {
        let recent_fees = self
            .includer_client
            .get_recent_prioritization_fees()
            .await
            .map_err(|e| FeesClientError::GenericError(e.to_string()))?;

        if recent_fees.is_empty() {
            return Err(FeesClientError::GenericError(
                "No recent prioritization fees found".to_string(),
            ));
        }

        let len = recent_fees.len();
        let fees_to_consider: Vec<f64> = recent_fees
            .into_iter()
            .skip(len.saturating_sub(self.last_n_blocks))
            .map(|fee| fee.prioritization_fee as f64)
            .collect();

        if fees_to_consider.is_empty() {
            return Err(FeesClientError::GenericError(
                "No valid fees found in recent blocks".to_string(),
            ));
        }

        let mut data = Data::new(fees_to_consider);

        let percentile_value = data.percentile(percentile as usize).max(0.0);

        let cu_price = percentile_value.ceil() as u64;

        // Apply fallback floor and cap
        let cu_price = if cu_price == 0 {
            FALLBACK_CU_PRICE
        } else {
            cu_price
        };

        if cu_price > MAX_CU_PRICE {
            warn!(
                computed = cu_price,
                cap = MAX_CU_PRICE,
                "CU price exceeded cap, clamping"
            );
            return Ok(MAX_CU_PRICE);
        }

        Ok(cu_price)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::IncluderClientError;
    use crate::includer_client::MockIncluderClientTrait;
    use solana_client::rpc_response::RpcPrioritizationFee;

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
            .returning(move || {
                let fees_clone = fees.clone();
                Box::pin(async move { Ok(fees_clone) })
            });

        let fees_client = FeesClient::new(mock_client, 10).unwrap();
        let result = fees_client.get_prioritization_fee_percentile(50).await;

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
            .returning(move || {
                let fees_clone = fees.clone();
                Box::pin(async move { Ok(fees_clone) })
            });

        let fees_client = FeesClient::new(mock_client, 10).unwrap();
        let result = fees_client.get_prioritization_fee_percentile(0).await;

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
            .returning(move || {
                let fees_clone = fees.clone();
                Box::pin(async move { Ok(fees_clone) })
            });

        let fees_client = FeesClient::new(mock_client, 10).unwrap();
        let result = fees_client.get_prioritization_fee_percentile(100).await;

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
            .returning(|| Box::pin(async { Ok(vec![]) }));

        let fees_client = FeesClient::new(mock_client, 10).unwrap();
        let result = fees_client.get_prioritization_fee_percentile(50).await;

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
            .returning(|| {
                Box::pin(async { Err(IncluderClientError::GenericError("RPC error".to_string())) })
            });

        let fees_client = FeesClient::new(mock_client, 10).unwrap();
        let result = fees_client.get_prioritization_fee_percentile(50).await;

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

        let fees: Vec<RpcPrioritizationFee> = (1..=10).map(|i| create_fee(i, i * 1000)).collect();

        mock_client
            .expect_get_recent_prioritization_fees()
            .times(1)
            .returning(move || {
                let fees_clone = fees.clone();
                Box::pin(async move { Ok(fees_clone) })
            });

        let fees_client = FeesClient::new(mock_client, 3).unwrap();
        let result = fees_client.get_prioritization_fee_percentile(50).await;

        assert!(result.is_ok());
        // 50th percentile of [8000, 9000, 10000] is 9000
        assert_eq!(result.unwrap(), 9000);
    }

    #[tokio::test]
    async fn test_get_recent_prioritization_fees_returns_fallback_on_error() {
        let mut mock_client = MockIncluderClientTrait::new();

        mock_client
            .expect_get_recent_prioritization_fees()
            .times(1)
            .returning(|| {
                Box::pin(async { Err(IncluderClientError::GenericError("RPC error".to_string())) })
            });

        let fees_client = FeesClient::new(mock_client, 10).unwrap();
        let result = fees_client.get_recent_prioritization_fees(50).await;

        // Should return FALLBACK_CU_PRICE on error
        assert_eq!(result, FALLBACK_CU_PRICE);
    }

    #[tokio::test]
    async fn test_get_recent_prioritization_fees_returns_fallback_on_empty_fees() {
        let mut mock_client = MockIncluderClientTrait::new();

        mock_client
            .expect_get_recent_prioritization_fees()
            .times(1)
            .returning(|| Box::pin(async { Ok(vec![]) }));

        let fees_client = FeesClient::new(mock_client, 10).unwrap();
        let result = fees_client.get_recent_prioritization_fees(50).await;

        // Should return FALLBACK_CU_PRICE when no fees found
        assert_eq!(result, FALLBACK_CU_PRICE);
    }

    #[tokio::test]
    async fn test_get_prioritization_fee_percentile_with_zero_fees_returns_fallback() {
        let mut mock_client = MockIncluderClientTrait::new();

        // All zero fees — should return FALLBACK_CU_PRICE instead of 0
        let fees = vec![create_fee(1, 0), create_fee(2, 0), create_fee(3, 0)];

        mock_client
            .expect_get_recent_prioritization_fees()
            .times(1)
            .returning(move || {
                let fees_clone = fees.clone();
                Box::pin(async move { Ok(fees_clone) })
            });

        let fees_client = FeesClient::new(mock_client, 10).unwrap();
        let result = fees_client.get_prioritization_fee_percentile(50).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), FALLBACK_CU_PRICE);
    }

    #[tokio::test]
    async fn test_get_prioritization_fee_percentile_with_large_fees_capped() {
        let mut mock_client = MockIncluderClientTrait::new();

        // Large fee values — should be capped to MAX_CU_PRICE
        let fees = vec![
            create_fee(1, 1_000_000_000),
            create_fee(2, 2_000_000_000),
            create_fee(3, 3_000_000_000),
        ];

        mock_client
            .expect_get_recent_prioritization_fees()
            .times(1)
            .returning(move || {
                let fees_clone = fees.clone();
                Box::pin(async move { Ok(fees_clone) })
            });

        let fees_client = FeesClient::new(mock_client, 10).unwrap();
        let result = fees_client.get_prioritization_fee_percentile(50).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), MAX_CU_PRICE);
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
            .returning(move || {
                let fees_clone = fees.clone();
                Box::pin(async move { Ok(fees_clone) })
            });

        let fees_client = FeesClient::new(mock_client, 10).unwrap();
        let result = fees_client.get_prioritization_fee_percentile(75).await;

        assert!(result.is_ok());
        // 75th percentile of [100, 200, 300, 400] using statrs linear interpolation
        assert_eq!(result.unwrap(), 359);
    }

    #[tokio::test]
    async fn test_get_prioritization_fee_percentile_caps_at_max() {
        let mut mock_client = MockIncluderClientTrait::new();

        // Fees above the cap — simulates a congestion spike
        let fees = vec![
            create_fee(1, 50_000),
            create_fee(2, 100_000),
            create_fee(3, 200_000),
        ];

        mock_client
            .expect_get_recent_prioritization_fees()
            .times(1)
            .returning(move || {
                let fees_clone = fees.clone();
                Box::pin(async move { Ok(fees_clone) })
            });

        let fees_client = FeesClient::new(mock_client, 10).unwrap();
        let result = fees_client.get_prioritization_fee_percentile(50).await;

        assert!(result.is_ok());
        // 50th percentile of [50000, 100000, 200000] = 100000, but capped to MAX_CU_PRICE
        assert_eq!(result.unwrap(), MAX_CU_PRICE);
    }

    #[tokio::test]
    async fn test_takes_most_recent_blocks_not_oldest() {
        let mut mock_client = MockIncluderClientTrait::new();

        // Simulate RPC returning 10 slots ascending by slot.
        // Old slots (1-7) have low fees, recent slots (8-10) have high fees.
        let mut fees: Vec<RpcPrioritizationFee> = (1..=7).map(|i| create_fee(i, 100)).collect();
        fees.extend((8..=10).map(|i| create_fee(i, 5000)));

        mock_client
            .expect_get_recent_prioritization_fees()
            .times(1)
            .returning(move || {
                let fees_clone = fees.clone();
                Box::pin(async move { Ok(fees_clone) })
            });

        // last_n_blocks = 3 should take slots 8, 9, 10 (the most recent)
        let fees_client = FeesClient::new(mock_client, 3).unwrap();
        let result = fees_client.get_prioritization_fee_percentile(50).await;

        assert!(result.is_ok());
        // 50th percentile of [5000, 5000, 5000] = 5000
        assert_eq!(result.unwrap(), 5000);
    }
}
