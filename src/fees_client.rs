use async_trait::async_trait;

use relayer_core::{error::ClientError, utils::ThreadSafe};
use solana_sdk::pubkey::Pubkey;
use statrs::statistics::{Data, OrderStatistics};

use crate::{error::FeesClientError, includer_client::IncluderClientTrait};

#[async_trait]
#[cfg_attr(test, mockall::automock)]
pub trait FeesClientTrait: ThreadSafe {
    async fn get_recent_prioritization_fees(&self, addresses: &[Pubkey], percentile: f64) -> u64;
    async fn get_prioritization_fee_percentile(
        &self,
        addresses: &[Pubkey],
        percentile: f64,
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
    async fn get_recent_prioritization_fees(&self, addresses: &[Pubkey], percentile: f64) -> u64 {
        self.get_prioritization_fee_percentile(addresses, percentile)
            .await
            .unwrap_or(0)
    }

    async fn get_prioritization_fee_percentile(
        &self,
        addresses: &[Pubkey],
        percentile: f64,
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

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::config::SolanaConfig;
//     use relayer_core::config::config_from_yaml;
//     use solana_sdk::pubkey::Pubkey;
//     use std::path::PathBuf;
//     use std::str::FromStr;

//     #[tokio::test]
//     async fn test_get_recent_prioritization_fees_real() {
//         dotenv::dotenv().ok();
//         let network = std::env::var("NETWORK").expect("NETWORK must be set");
//         let config = load_local_config(&network);

//         let url = &config.fees_client_url;
//         println!("Using FeesClient URL: {}", url);

//         let client = FeesClient::new(url).expect("failed to create FeesClient");

//         let addr = Pubkey::from_str("483jTxdFmFGRnzgx9nBoQM2Zao5mZxKvFgHzTb4Ytn1L")
//             .expect("invalid pubkey in test");

//         let fee = client
//             .get_recent_prioritization_fees(&[addr])
//             .await
//             .expect("RPC call should succeed");

//         println!("priorityFeeEstimate = {}", fee);
//         assert!(fee > 0, "Fee estimate should be positive");
//     }

//     #[tokio::test]
//     async fn test_raw_priority_fee_response() {
//         dotenv::dotenv().ok();
//         let network = std::env::var("NETWORK").expect("NETWORK must be set");
//         let config = load_local_config(&network);

//         let url = &config.fees_client_url;
//         println!("Using FeesClient URL: {}", url);

//         let addr = Pubkey::from_str("483jTxdFmFGRnzgx9nBoQM2Zao5mZxKvFgHzTb4Ytn1L")
//             .expect("invalid pubkey in test");

//         let body = get_recent_prioritization_fees_command(vec![addr]);
//         let raw = post_request(url, &body)
//             .await
//             .expect("RPC request should succeed");

//         println!("Raw getPriorityFeeEstimate response:\n{}", raw);

//         let v: serde_json::Value =
//             serde_json::from_str(&raw).expect("Response should be valid JSON");

//         assert_eq!(v.get("jsonrpc").and_then(|j| j.as_str()), Some("2.0"));
//         assert!(v.get("id").is_some(), "id field should be present");
//         assert!(v.get("result").is_some(), "result field should be present");

//         let fee = v
//             .get("result")
//             .and_then(|r| r.get("priorityFeeEstimate"))
//             .expect("priorityFeeEstimate should be present");
//         let fee = fee
//             .as_f64()
//             .map(|f| f.ceil() as u64)
//             .expect("priorityFeeEstimate should be a floating point number");

//         println!("Parsed priorityFeeEstimate = {}", fee);
//         assert!(fee > 0);
//     }

//     fn load_local_config(network: &str) -> SolanaConfig {
//         let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
//             .join("config")
//             .join(format!("config.{}.yaml", network));
//         config_from_yaml(path.to_str().expect("valid config path")).unwrap()
//     }
// }
