use std::{future::Future, str::FromStr, time::Duration};

use anyhow::anyhow;
use serde_json::Value;

use crate::types::{RpcGetTransactionResponse, SolanaTransaction};
use relayer_core::error::ClientError;
use solana_client::{
    rpc_client::GetConfirmedSignaturesForAddress2Config, rpc_config::RpcTransactionConfig,
};
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use solana_transaction_status::UiTransactionEncoding;
use tracing::{debug, info};

use crate::utils::{exec_curl_batch, get_tx_batch_command};

const SIGNATURE_PAGE_LIMIT: usize = 100;

pub trait SolanaRpcClientTrait: Send + Sync {
    fn inner(&self) -> &RpcClient;

    fn get_transaction_by_signature(
        &self,
        signature: Signature,
    ) -> impl Future<Output = Result<SolanaTransaction, anyhow::Error>>;

    fn get_transactions_for_account(
        &self,
        address: &Pubkey,
        before: Option<Signature>,
        until: Option<Signature>,
    ) -> impl Future<Output = Result<Vec<SolanaTransaction>, anyhow::Error>>;
}

pub struct SolanaRpcClient {
    client: RpcClient,
    max_retries: usize,
    rpc_url: String,
}

impl SolanaRpcClient {
    pub fn new(
        url: &str,
        commitment: CommitmentConfig,
        max_retries: usize,
    ) -> Result<Self, ClientError> {
        Ok(Self {
            client: RpcClient::new_with_commitment(url.to_string(), commitment),
            max_retries,
            rpc_url: url.to_string(),
        })
    }
}

impl SolanaRpcClientTrait for SolanaRpcClient {
    fn inner(&self) -> &RpcClient {
        &self.client
    }

    async fn get_transaction_by_signature(
        &self,
        signature: Signature,
    ) -> Result<SolanaTransaction, anyhow::Error> {
        let config = RpcTransactionConfig {
            encoding: Some(UiTransactionEncoding::Json),
            commitment: Some(self.client.commitment()),
            max_supported_transaction_version: Some(0),
        };

        let mut retries = 0;
        let mut delay = Duration::from_millis(500);

        loop {
            match self
                .client
                .get_transaction_with_config(&signature, config)
                .await
            {
                Ok(response) => {
                    return SolanaTransaction::from_encoded_confirmed_transaction_with_status_meta(
                        signature, response,
                    );
                }
                Err(e) => {
                    if retries >= self.max_retries {
                        return Err(anyhow!(
                            "Failed to get transaction with config: {:?}",
                            e.to_string()
                        ));
                    }

                    debug!(
                        "RPC call ({}) failed (retry {}/{}): {}. Retrying in {:?}...",
                        "get_transaction_with_config",
                        retries + 1,
                        self.max_retries,
                        e,
                        delay
                    );

                    tokio::time::sleep(delay).await;
                    retries += 1;
                    delay = delay.mul_f32(2.0);
                }
            }
        }
    }

    // Traverses the account's tx history backwards
    // before and until are exclusive
    // before is the chronologically latest transaction
    // until is the chronologically earliest transaction
    async fn get_transactions_for_account(
        &self,
        address: &Pubkey,
        before: Option<Signature>,
        until: Option<Signature>,
    ) -> Result<Vec<SolanaTransaction>, anyhow::Error> {
        let mut retries = 0;
        let mut delay = Duration::from_millis(500);
        let mut txs: Vec<SolanaTransaction> = vec![];
        let mut before_sig = before;
        let until_sig = until;

        loop {
            // Config needs to be inside the loop because it does not implement clone
            let config = GetConfirmedSignaturesForAddress2Config {
                commitment: Some(self.client.commitment()),
                limit: Some(SIGNATURE_PAGE_LIMIT),
                before: before_sig,
                until: until_sig,
            };
            // This function returns the signatures. We then need to get the transactions for each signature
            match self
                .client
                .get_signatures_for_address_with_config(address, config)
                .await
            {
                Ok(response) => {
                    // edge case where last page had exactly LIMIT txs and we did one extra request
                    if response.is_empty() {
                        info!("No more signatures to fetch, empty response");
                        return Ok(txs);
                    }

                    debug!("Fetched {} signatures", response.len());

                    let body_json_str =
                        get_tx_batch_command(response.clone(), self.client.commitment());
                    let raw = exec_curl_batch(&self.rpc_url, &body_json_str)
                        .await
                        .map_err(|e| anyhow!(format!("batch curl failed: {}", e)))?;

                    let parsed: Vec<Value> = serde_json::from_str(&raw)
                        .map_err(|e| anyhow!(format!("batch parse failed: {}", e)))?;

                    for entry in parsed.into_iter() {
                        let rpc_response: RpcGetTransactionResponse =
                            serde_json::from_value(entry)?;

                        let tx = SolanaTransaction::from_rpc_response(rpc_response)?;
                        txs.push(tx.clone());
                        debug!("Pushed tx to vector: {:?}", tx.signature);
                    }

                    // If we have less than LIMIT txs, we can return since there are no more pages
                    if response.len() < SIGNATURE_PAGE_LIMIT {
                        info!(
                            "No more signatures to fetch, fetched {} and limit is {}",
                            response.len(),
                            SIGNATURE_PAGE_LIMIT
                        );
                        return Ok(txs);
                    }

                    let maybe_earliest_signature = response.last();
                    match maybe_earliest_signature {
                        Some(earliest_signature) => {
                            before_sig = Some(Signature::from_str(&earliest_signature.signature)?)
                        }
                        None => return Err(anyhow!("No earliest signature found")),
                    }
                }

                Err(e) => {
                    if retries >= self.max_retries {
                        return Err(anyhow!(
                            "Failed to get signatures for address with config: {:?}",
                            e.to_string()
                        ));
                    }

                    debug!(
                        "RPC call ({}) failed (retry {}/{}): {}. Retrying in {:?}...",
                        "get_signatures_for_address_with_config",
                        retries + 1,
                        self.max_retries,
                        e,
                        delay
                    );

                    tokio::time::sleep(delay).await;
                    retries += 1;
                    delay = delay.mul_f32(2.0);
                }
            }
        }
    }
}
