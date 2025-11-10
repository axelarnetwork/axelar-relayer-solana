use async_trait::async_trait;

use relayer_core::{error::ClientError, utils::ThreadSafe};
use solana_sdk::pubkey::Pubkey;

use crate::{
    error::FeesClientError,
    utils::{get_recent_prioritization_fees_command, post_request},
};

#[async_trait]
#[cfg_attr(test, mockall::automock)]
pub trait FeesClientTrait: ThreadSafe {
    async fn get_recent_prioritization_fees(
        &self,
        addresses: &[Pubkey],
    ) -> Result<u64, FeesClientError>;
}

#[derive(Clone)]
pub struct FeesClient {
    url: String,
}

impl FeesClient {
    pub fn new(url: &str) -> Result<Self, ClientError> {
        Ok(Self {
            url: url.to_string(),
        })
    }
}

#[async_trait]
impl FeesClientTrait for FeesClient {
    async fn get_recent_prioritization_fees(
        &self,
        addresses: &[Pubkey],
    ) -> Result<u64, FeesClientError> {
        let body_json_str = get_recent_prioritization_fees_command(addresses.to_vec());
        let raw = post_request(&self.url, &body_json_str)
            .await
            .map_err(|e| FeesClientError::GenericError(e.to_string()))?;

        let parsed: serde_json::Value =
            serde_json::from_str(&raw).map_err(|e| FeesClientError::GenericError(e.to_string()))?;

        parsed
            .get("result")
            .and_then(|result| result.get("priorityFeeEstimate"))
            .ok_or(FeesClientError::GenericError(
                "priorityFeeEstimate not found in response".to_string(),
            ))?
            .as_u64()
            .ok_or(FeesClientError::GenericError(
                "priorityFeeEstimate is not a u64".to_string(),
            ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SolanaConfig;
    use relayer_core::config::config_from_yaml;
    use solana_sdk::pubkey::Pubkey;
    use std::path::PathBuf;
    use std::str::FromStr;

    /// Integration test: check that get_recent_prioritization_fees returns a valid number
    /// Run manually:
    ///     NETWORK=devnet cargo test -- --nocapture --ignored test_get_recent_prioritization_fees_real
    #[tokio::test]
    #[ignore]
    async fn test_get_recent_prioritization_fees_real() {
        let network = std::env::var("NETWORK").expect("NETWORK must be set");
        let config = load_local_config(&network);

        let url = &config.fees_client_url;
        println!("Using FeesClient URL: {}", url);

        let client = FeesClient::new(url).expect("failed to create FeesClient");

        // Example public key from docs (any valid address works)
        let addr = Pubkey::from_str("483jTxdFmFGRnzgx9nBoQM2Zao5mZxKvFgHzTb4Ytn1L")
            .expect("invalid pubkey in test");

        let fee = client
            .get_recent_prioritization_fees(&[addr])
            .await
            .expect("RPC call should succeed");

        println!("priorityFeeEstimate = {}", fee);
        assert!(fee > 0, "Fee estimate should be positive");
    }

    /// Integration test: print and inspect the raw RPC JSON response
    /// Run manually:
    ///     NETWORK=devnet cargo test -- --nocapture --ignored test_raw_priority_fee_response
    #[tokio::test]
    #[ignore]
    async fn test_raw_priority_fee_response() {
        let network = std::env::var("NETWORK").expect("NETWORK must be set");
        let config = load_local_config(&network);

        let url = &config.fees_client_url;
        println!("Using FeesClient URL: {}", url);

        let addr = Pubkey::from_str("483jTxdFmFGRnzgx9nBoQM2Zao5mZxKvFgHzTb4Ytn1L")
            .expect("invalid pubkey in test");

        let body = get_recent_prioritization_fees_command(vec![addr]);
        let raw = post_request(url, &body)
            .await
            .expect("RPC request should succeed");

        println!("Raw getPriorityFeeEstimate response:\n{}", raw);

        let v: serde_json::Value =
            serde_json::from_str(&raw).expect("Response should be valid JSON");

        // Assert the RPC envelope
        assert_eq!(v.get("jsonrpc").and_then(|j| j.as_str()), Some("2.0"));
        assert!(v.get("id").is_some(), "id field should be present");
        assert!(v.get("result").is_some(), "result field should be present");

        // Print or assert the fee value
        if let Some(fee) = v
            .get("result")
            .and_then(|r| r.get("priorityFeeEstimate"))
            .and_then(|f| f.as_u64())
        {
            println!("Parsed priorityFeeEstimate = {}", fee);
            assert!(fee > 0);
        } else {
            println!(
                "priorityFeeEstimate not found; full result: {}",
                v.get("result").unwrap_or(&serde_json::json!({}))
            );
        }
    }

    fn load_local_config(network: &str) -> SolanaConfig {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("config")
            .join(format!("config.{}.yaml", network));
        config_from_yaml(path.to_str().expect("valid config path")).unwrap()
    }
}
