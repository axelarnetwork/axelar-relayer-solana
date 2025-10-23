use crate::{
    types::SolanaTransaction,
    v2_program_types::{ExecuteData, MerkleisedPayload},
};
use anchor_spl::{associated_token::spl_associated_token_account, token_2022::spl_token_2022};
use anyhow::anyhow;
use bincode;
use relayer_core::{
    gmp_api::GmpApiTrait,
    queue::{QueueItem, QueueTrait},
};
use serde_json::json;
use solana_transaction_parser::gmp_types::{CannotExecuteMessageReason, Event};
use std::str::FromStr;
use tracing::{debug, error};

use axelar_solana_gateway_v2::{seed_prefixes, VerifierSetHash, ID};
use solana_rpc_client_api::response::RpcConfirmedTransactionStatusWithSignature;
use solana_sdk::{
    commitment_config::{CommitmentConfig, CommitmentLevel},
    compute_budget::ComputeBudgetInstruction,
    pubkey::Pubkey,
    signature::Signature,
    transaction::Transaction,
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
pub fn get_gateway_event_authority_pda() -> (Pubkey, u8) {
    Pubkey::find_program_address(&[b"__event_authority"], &axelar_solana_gateway_v2::ID)
}

pub fn get_governance_config_pda() -> (Pubkey, u8) {
    Pubkey::find_program_address(
        &[axelar_solana_governance_v2::GovernanceConfig::SEED_PREFIX],
        &axelar_solana_governance_v2::ID,
    )
}

pub fn get_governance_event_authority_pda() -> (Pubkey, u8) {
    Pubkey::find_program_address(&[b"__event_authority"], &axelar_solana_governance_v2::ID)
}

pub fn get_proposal_pda(command_id: &[u8]) -> (Pubkey, u8) {
    Pubkey::find_program_address(
        &[
            axelar_solana_governance_v2::seed_prefixes::PROPOSAL_PDA,
            command_id,
        ],
        &axelar_solana_governance_v2::ID,
    )
}

pub fn get_operator_proposal_pda(command_id: &[u8]) -> (Pubkey, u8) {
    Pubkey::find_program_address(
        &[b"operator_proposal", command_id],
        &axelar_solana_governance_v2::ID,
    )
}

pub fn get_validate_message_signing_pda(command_id: &[u8], program_id: &Pubkey) -> (Pubkey, u8) {
    Pubkey::find_program_address(
        &[
            axelar_solana_gateway_v2::seed_prefixes::VALIDATE_MESSAGE_SIGNING_SEED,
            command_id,
        ],
        program_id,
    )
}

pub fn get_gateway_root_config_internal() -> (Pubkey, u8) {
    Pubkey::find_program_address(
        &[axelar_solana_gateway_v2::seed_prefixes::GATEWAY_SEED],
        &axelar_solana_gateway_v2::ID,
    )
}

pub fn get_its_root_pda() -> (Pubkey, u8) {
    Pubkey::find_program_address(
        &[axelar_solana_its_v2::seed_prefixes::ITS_SEED],
        &axelar_solana_its_v2::ID,
    )
}

pub fn get_token_manager_pda(its_root_pda: &Pubkey, token_id: &[u8]) -> (Pubkey, u8) {
    Pubkey::find_program_address(
        &[
            axelar_solana_its_v2::seed_prefixes::TOKEN_MANAGER_SEED,
            its_root_pda.as_ref(),
            token_id,
        ],
        &axelar_solana_its_v2::ID,
    )
}

pub fn get_token_mint_pda(its_root_pda: &Pubkey, token_id: &[u8]) -> (Pubkey, u8) {
    Pubkey::find_program_address(
        &[
            axelar_solana_its_v2::seed_prefixes::INTERCHAIN_TOKEN_SEED,
            its_root_pda.as_ref(),
            token_id,
        ],
        &axelar_solana_its_v2::ID,
    )
}

pub fn get_token_manager_ata(token_manager_pda: &Pubkey, token_mint_pda: &Pubkey) -> (Pubkey, u8) {
    Pubkey::find_program_address(
        &[
            token_manager_pda.as_ref(),
            spl_token_2022::id().as_ref(),
            token_mint_pda.as_ref(),
        ],
        &spl_associated_token_account::id(),
    )
}

pub fn get_deployer_ata(payer: &Pubkey, token_mint_pda: &Pubkey) -> (Pubkey, u8) {
    Pubkey::find_program_address(
        &[
            payer.as_ref(),
            spl_token_2022::id().as_ref(),
            token_mint_pda.as_ref(),
        ],
        &spl_associated_token_account::id(),
    )
}

pub fn get_mpl_token_metadata_account(token_mint_pda: &Pubkey) -> (Pubkey, u8) {
    Pubkey::find_program_address(
        &[
            b"metadata",
            mpl_token_metadata::ID.as_ref(),
            token_mint_pda.as_ref(),
        ],
        &mpl_token_metadata::ID,
    )
}

pub fn get_minter_roles_pda(token_manager_pda: &Pubkey, minter: &Pubkey) -> (Pubkey, u8) {
    Pubkey::find_program_address(
        &[
            axelar_solana_its_v2::state::UserRoles::SEED_PREFIX,
            token_manager_pda.as_ref(),
            minter.as_ref(),
        ],
        &axelar_solana_its_v2::ID,
    )
}

pub async fn get_cannot_execute_events_from_execute_data<G: GmpApiTrait>(
    execute_data: &ExecuteData,
    reason: CannotExecuteMessageReason,
    details: String,
    task_id: String,
    gmp_api: Arc<G>,
) -> Result<Vec<Event>, anyhow::Error> {
    let mut cannot_execute_events = vec![];
    let payload_items = execute_data.payload_items.clone();
    match payload_items {
        MerkleisedPayload::VerifierSetRotation { .. } => {
            // skipping set rotation as it is not a message
        }
        MerkleisedPayload::NewMessages { messages } => {
            for message in messages {
                cannot_execute_events.push(
                    gmp_api
                        .cannot_execute_message(
                            task_id.clone(),
                            message.leaf.message.cc_id.id.clone(),
                            message.leaf.message.cc_id.chain.clone(),
                            details.clone(),
                            reason.clone(),
                        )
                        .await
                        .map_err(|e| anyhow!("Failed to create cannot execute event: {}", e))?,
                );
            }
        }
    }
    Ok(cannot_execute_events)
}

pub fn calculate_total_cost_lamports(tx: &Transaction, units: u64) -> Result<u64, anyhow::Error> {
    const LAMPORTS_PER_SIGNATURE: u64 = 5_000;
    const MICRO_PER_LAMPORT: u128 = 1_000_000;

    let mut micro_price: u64 = 0;
    for ix in &tx.message.instructions {
        if ix.program_id(&tx.message.account_keys) == &solana_sdk::compute_budget::id() {
            if let Ok(ComputeBudgetInstruction::SetComputeUnitPrice(p)) =
                bincode::deserialize(&ix.data)
            {
                micro_price = p;
            }
        }
    }

    // to avoid overflows
    #[inline]
    fn ceil_div_u128(n: u128, d: u128) -> u128 {
        let q = n / d;
        let r = n % d;
        if r == 0 {
            q
        } else {
            q.saturating_add(1)
        }
    }

    let total_micro = (micro_price as u128).saturating_mul(units as u128);
    let priority_u128 = ceil_div_u128(total_micro, MICRO_PER_LAMPORT);
    let priority_lamports: u64 = priority_u128.try_into().unwrap_or(u64::MAX);

    let sigs = tx.message.header.num_required_signatures as u64;
    let base_fee = LAMPORTS_PER_SIGNATURE.saturating_mul(sigs);

    Ok(base_fee.saturating_add(priority_lamports))
}
