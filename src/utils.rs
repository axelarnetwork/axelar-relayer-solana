use crate::types::SolanaTransaction;
use anyhow::anyhow;
use relayer_core::queue::{QueueItem, QueueTrait};
use serde_json::json;
use std::str::FromStr;
use tracing::{debug, error};

use axelar_solana_gateway_v2::{seed_prefixes, VerifierSetHash, ID};
use solana_rpc_client_api::response::RpcConfirmedTransactionStatusWithSignature;
use solana_sdk::{
    commitment_config::{CommitmentConfig, CommitmentLevel},
    pubkey::Pubkey,
    signature::Signature,
};
use std::sync::Arc;

use crate::{
    solana_transaction::SolanaTransactionData, solana_transaction::SolanaTransactionModel,
};

pub fn get_tx_batch_command(
    txs: Vec<RpcConfirmedTransactionStatusWithSignature>,
    commitment: CommitmentConfig,
) -> String {
    let cfg = json!({
        "commitment": get_commitment_str(commitment),
        "maxSupportedTransactionVersion": 0,
        "encoding": "json",
    });

    let mut batch = Vec::with_capacity(txs.len());

    for (i, status_with_signature) in txs.into_iter().enumerate() {
        let sig_str = status_with_signature.signature;

        if let Err(e) = Signature::from_str(&sig_str) {
            error!("Error parsing signature: {}", e);
            continue;
        }

        batch.push(json!({
            "jsonrpc": "2.0",
            "id": (i + 1) as u64,
            "method": "getTransaction",
            "params": [ sig_str, cfg ],
        }));
    }

    serde_json::to_string(&batch).unwrap_or_else(|_| "[]".to_string())
}

pub async fn exec_curl_batch(url: &str, body_json: &str) -> anyhow::Result<String> {
    let output = tokio::process::Command::new("bash")
        .arg("-lc")
        .arg(format!(
            "curl '{}' -s -X POST -H 'Content-Type: application/json' --data-binary \"$BODY\"",
            url
        ))
        .env("BODY", body_json)
        .output()
        .await?;

    if !output.status.success() {
        return Err(anyhow::anyhow!(
            "Command failed with status: {}",
            output.status
        ));
    }

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    Ok(stdout)
}

fn get_commitment_str(commitment: CommitmentConfig) -> String {
    match commitment.commitment {
        CommitmentLevel::Processed => String::from("processed"),
        CommitmentLevel::Confirmed => String::from("confirmed"),
        CommitmentLevel::Finalized => String::from("finalized"),
    }
}

pub async fn upsert_and_publish<SM: SolanaTransactionModel>(
    transaction_model: &Arc<SM>,
    queue: &Arc<dyn QueueTrait>,
    tx: &SolanaTransaction,
    from_service: String,
) -> Result<bool, anyhow::Error> {
    let ixs = tx
        .ixs
        .iter()
        .map(|ix| serde_json::to_string(ix).unwrap_or_else(|_| "".to_string()))
        .collect::<Vec<String>>();
    let inserted = transaction_model
        .upsert(SolanaTransactionData {
            signature: tx.signature.to_string(),
            slot: tx.slot,
            logs: tx.logs.clone(),
            ixs,
            events: Vec::<String>::new(),
            cost_units: tx.cost_units as i64,
            account_keys: tx.account_keys.clone(),
            retries: 3,
            created_at: None,
        })
        .await
        .map_err(|e| anyhow!("Error upserting transaction: {:?}", e))?;

    if inserted {
        let chain_transaction = serde_json::to_string(&tx)?;

        let item = &QueueItem::Transaction(Box::new(chain_transaction.clone()));
        debug!(
            "Publishing transaction from {}: {:?}",
            from_service, chain_transaction
        );
        queue.publish(item.clone()).await;
    } else {
        debug!("Transaction already exists: {:?}", tx.signature);
    }
    Ok(inserted)
}

pub fn get_signature_verification_pda(payload_merkle_root: &[u8; 32]) -> (Pubkey, u8) {
    let (pubkey, bump) = Pubkey::find_program_address(
        &[
            seed_prefixes::SIGNATURE_VERIFICATION_SEED,
            payload_merkle_root,
        ],
        &ID,
    );
    (pubkey, bump)
}

pub fn get_verifier_set_tracker_pda(hash: VerifierSetHash) -> (Pubkey, u8) {
    Pubkey::find_program_address(
        &[seed_prefixes::VERIFIER_SET_TRACKER_SEED, hash.as_slice()],
        &ID,
    )
}

pub fn get_incoming_message_pda(command_id: &[u8]) -> (Pubkey, u8) {
    Pubkey::find_program_address(&[seed_prefixes::INCOMING_MESSAGE_SEED, command_id], &ID)
}
