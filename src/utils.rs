use crate::{includer::ALTInfo, transaction_type::SolanaTransactionType, types::SolanaTransaction};
use anchor_lang::Key;
use anchor_spl::{associated_token::spl_associated_token_account, token_2022::spl_token_2022};
use anyhow::anyhow;
use regex::Regex;
use relayer_core::{
    gmp_api::{gmp_types::ExecuteTask, GmpApiTrait},
    queue::{QueueItem, QueueTrait},
};
use serde_json::json;
use solana_axelar_std::execute_data::{ExecuteData, MerklizedPayload};
use solana_transaction_parser::gmp_types::{CannotExecuteMessageReason, Event};
use std::str::FromStr;
use tracing::{debug, error};

use solana_axelar_gateway::{seed_prefixes, ID};
use solana_rpc_client_api::response::RpcConfirmedTransactionStatusWithSignature;
use solana_sdk::{
    commitment_config::{CommitmentConfig, CommitmentLevel},
    compute_budget::ComputeBudgetInstruction,
    instruction::Instruction,
    message::{v0, AddressLookupTableAccount, VersionedMessage},
    pubkey::Pubkey,
    signature::{Keypair, Signature},
    signer::Signer,
    transaction::{Transaction, TransactionError, VersionedTransaction},
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

pub fn get_recent_prioritization_fees_command(addresses: Vec<Pubkey>) -> String {
    let account_keys: Vec<String> = addresses.iter().map(|pk| pk.to_string()).collect();

    let request = json!({
        "jsonrpc": "2.0",
        "id": "1",
        "method": "getPriorityFeeEstimate",
        "params": [
            {
                "accountKeys": account_keys,
                "options": {
                    "includeAllPriorityFeeLevels": false
                }
            }
        ]
    });

    serde_json::to_string(&request).unwrap_or_else(|_| "{}".to_string())
}

pub async fn post_request(url: &str, body_json: &str) -> anyhow::Result<String> {
    let client = reqwest::Client::new();
    debug!("Executing request: POST {} with body {}", url, body_json);
    let response = client
        .post(url)
        .header("Content-Type", "application/json")
        .body(body_json.to_string())
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("HTTP request failed: {}", e))?;

    if !response.status().is_success() {
        return Err(anyhow::anyhow!(
            "Request failed with status: {}",
            response.status()
        ));
    }

    let text = response
        .text()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to read response body: {}", e))?;

    Ok(text)
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
    force_publish: bool,
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

    if inserted || force_publish {
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

pub fn get_signature_verification_pda(
    payload_merkle_root: &[u8; 32],
    signing_verifier_set_merkle_root: &[u8; 32],
    payload_type: solana_axelar_std::PayloadType,
) -> Result<(Pubkey, u8), anyhow::Error> {
    let payload_type_seed: u8 = payload_type.into();
    Pubkey::try_find_program_address(
        &[
            seed_prefixes::SIGNATURE_VERIFICATION_SEED,
            payload_merkle_root,
            &[payload_type_seed],
            signing_verifier_set_merkle_root,
        ],
        &ID,
    )
    .ok_or_else(|| anyhow::anyhow!("Failed to get signature verification PDA"))
}

pub fn get_verifier_set_tracker_pda(
    signing_verifier_set_merkle_root: &[u8; 32],
) -> Result<(Pubkey, u8), anyhow::Error> {
    Pubkey::try_find_program_address(
        &[
            seed_prefixes::VERIFIER_SET_TRACKER_SEED,
            signing_verifier_set_merkle_root,
        ],
        &ID,
    )
    .ok_or_else(|| anyhow::anyhow!("Failed to get verifier set tracker PDA"))
}

pub fn get_incoming_message_pda(command_id: &[u8]) -> Result<(Pubkey, u8), anyhow::Error> {
    Pubkey::try_find_program_address(&[seed_prefixes::INCOMING_MESSAGE_SEED, command_id], &ID)
        .ok_or_else(|| anyhow::anyhow!("Failed to get incoming message PDA"))
}
pub fn get_gateway_event_authority_pda() -> Result<(Pubkey, u8), anyhow::Error> {
    Pubkey::try_find_program_address(&[b"__event_authority"], &solana_axelar_gateway::ID)
        .ok_or_else(|| anyhow!("Failed to derive gateway event authority PDA"))
}

pub fn get_governance_config_pda() -> Result<(Pubkey, u8), anyhow::Error> {
    Pubkey::try_find_program_address(
        &[solana_axelar_governance::GovernanceConfig::SEED_PREFIX],
        &solana_axelar_governance::ID,
    )
    .ok_or_else(|| anyhow!("Failed to derive governance config PDA"))
}

pub fn get_governance_event_authority_pda() -> Result<(Pubkey, u8), anyhow::Error> {
    Pubkey::try_find_program_address(&[b"__event_authority"], &solana_axelar_governance::ID)
        .ok_or_else(|| anyhow!("Failed to derive governance event authority PDA"))
}

pub fn get_proposal_pda(command_id: &[u8]) -> Result<(Pubkey, u8), anyhow::Error> {
    Pubkey::try_find_program_address(
        &[
            solana_axelar_governance::seed_prefixes::PROPOSAL_PDA,
            command_id,
        ],
        &solana_axelar_governance::ID,
    )
    .ok_or_else(|| anyhow!("Failed to derive proposal PDA"))
}

pub fn get_operator_proposal_pda(command_id: &[u8]) -> Result<(Pubkey, u8), anyhow::Error> {
    Pubkey::try_find_program_address(
        &[b"operator_proposal", command_id],
        &solana_axelar_governance::ID,
    )
    .ok_or_else(|| anyhow!("Failed to derive operator proposal PDA"))
}

pub fn get_validate_message_signing_pda(
    command_id: &[u8],
    program_id: &Pubkey,
) -> Result<(Pubkey, u8), anyhow::Error> {
    Pubkey::try_find_program_address(
        &[
            solana_axelar_gateway::seed_prefixes::VALIDATE_MESSAGE_SIGNING_SEED,
            command_id,
        ],
        program_id,
    )
    .ok_or_else(|| anyhow!("Failed to derive validate message signing PDA"))
}

pub fn get_gateway_root_config_internal() -> Result<(Pubkey, u8), anyhow::Error> {
    Pubkey::try_find_program_address(
        &[solana_axelar_gateway::seed_prefixes::GATEWAY_SEED],
        &solana_axelar_gateway::ID,
    )
    .ok_or_else(|| anyhow!("Failed to derive gateway root config PDA"))
}

pub fn get_its_root_pda() -> (Pubkey, u8) {
    solana_axelar_its::InterchainTokenService::find_pda()
}

pub fn get_its_event_authority_pda() -> Result<(Pubkey, u8), anyhow::Error> {
    Pubkey::try_find_program_address(&[b"__event_authority"], &solana_axelar_its::ID)
        .ok_or_else(|| anyhow!("Failed to derive ITS event authority PDA"))
}

pub fn get_token_manager_pda(
    its_root_pda: &Pubkey,
    token_id: &[u8],
) -> Result<(Pubkey, u8), anyhow::Error> {
    let token_id_array: [u8; 32] = token_id
        .try_into()
        .map_err(|e| anyhow::anyhow!("token_id must be 32 bytes: {}", e))?;
    Ok(solana_axelar_its::TokenManager::find_pda(
        token_id_array,
        *its_root_pda,
    ))
}

pub fn get_token_mint_pda(
    its_root_pda: &Pubkey,
    token_id: &[u8],
) -> Result<(Pubkey, u8), anyhow::Error> {
    let token_id_array: [u8; 32] = token_id
        .try_into()
        .map_err(|e| anyhow::anyhow!("token_id must be 32 bytes: {}", e))?;
    Ok(solana_axelar_its::TokenManager::find_token_mint(
        token_id_array,
        *its_root_pda,
    ))
}

pub fn get_token_manager_ata_with_program(
    token_manager_pda: &Pubkey,
    token_mint: &Pubkey,
    token_program: &Pubkey,
) -> (Pubkey, u8) {
    let ata = anchor_spl::associated_token::get_associated_token_address_with_program_id(
        token_manager_pda,
        token_mint,
        token_program,
    );
    // bump not used by the relayer so set it to 0
    (ata, 0)
}

pub fn get_mpl_token_metadata_account(
    token_mint_pda: &Pubkey,
) -> Result<(Pubkey, u8), anyhow::Error> {
    Pubkey::try_find_program_address(
        &[
            b"metadata",
            mpl_token_metadata::ID.as_ref(),
            token_mint_pda.as_ref(),
        ],
        &mpl_token_metadata::ID,
    )
    .ok_or_else(|| anyhow!("Failed to derive MPL token metadata PDA"))
}

pub fn get_minter_roles_pda(token_manager_pda: &Pubkey, minter: &Pubkey) -> (Pubkey, u8) {
    solana_axelar_its::UserRoles::find_pda(token_manager_pda, minter)
}

pub fn get_operator_pda(operator: &Pubkey) -> Result<(Pubkey, u8), anyhow::Error> {
    Pubkey::try_find_program_address(
        &[
            solana_axelar_operators::OperatorAccount::SEED_PREFIX,
            operator.key().as_ref(),
        ],
        &solana_axelar_operators::ID,
    )
    .ok_or_else(|| anyhow!("Failed to derive operator PDA"))
}

pub fn get_treasury_pda() -> Result<(Pubkey, u8), anyhow::Error> {
    Pubkey::try_find_program_address(
        &[solana_axelar_gas_service::state::Treasury::SEED_PREFIX],
        &solana_axelar_gas_service::ID,
    )
    .ok_or_else(|| anyhow!("Failed to derive treasury PDA"))
}

pub fn get_gas_service_event_authority_pda() -> Result<(Pubkey, u8), anyhow::Error> {
    Pubkey::try_find_program_address(&[b"__event_authority"], &solana_axelar_gas_service::ID)
        .ok_or_else(|| anyhow!("Failed to derive gas service event authority PDA"))
}

pub fn get_destination_ata(
    destination_pubkey: &Pubkey,
    token_mint_pda: &Pubkey,
) -> Result<(Pubkey, u8), anyhow::Error> {
    Pubkey::try_find_program_address(
        &[
            destination_pubkey.as_ref(),
            spl_token_2022::id().as_ref(),
            token_mint_pda.as_ref(),
        ],
        &spl_associated_token_account::id(),
    )
    .ok_or_else(|| anyhow!("Failed to derive destination ATA PDA"))
}

pub fn get_initialize_verification_session_pda(
    payload_merkle_root: &[u8; 32],
    signing_verifier_set_hash: &[u8; 32],
) -> Result<(Pubkey, u8), anyhow::Error> {
    Pubkey::try_find_program_address(
        &[
            seed_prefixes::SIGNATURE_VERIFICATION_SEED,
            payload_merkle_root,
            signing_verifier_set_hash,
        ],
        &solana_axelar_gateway::ID,
    )
    .ok_or_else(|| anyhow!("Failed to derive verification session PDA"))
}

pub fn calculate_total_cost_lamports(
    tx: &SolanaTransactionType,
    units: u64,
) -> Result<u64, anyhow::Error> {
    const LAMPORTS_PER_SIGNATURE: u64 = 5_000;
    const MICRO_PER_LAMPORT: u128 = 1_000_000;

    let micro_price = tx.get_potential_micro_priority_price();

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

    let sigs = tx.get_num_required_signatures();
    let base_fee = LAMPORTS_PER_SIGNATURE.saturating_mul(sigs);

    Ok(base_fee.saturating_add(priority_lamports))
}

#[allow(clippy::too_many_arguments)]
pub async fn create_transaction(
    mut instructions: Vec<Instruction>,
    alt_info: Option<ALTInfo>,
    alt_addresses: Vec<Pubkey>,
    unit_price: u64,
    compute_budget: u64,
    payer: &Keypair,
    signing_keypairs: Vec<&Keypair>,
    recent_hash: solana_sdk::hash::Hash,
) -> Result<SolanaTransactionType, anyhow::Error> {
    let set_compute_unit_price_ix = ComputeBudgetInstruction::set_compute_unit_price(unit_price);
    let set_compute_budget_ix = ComputeBudgetInstruction::set_compute_unit_limit(
        compute_budget
            .try_into()
            .map_err(|e: std::num::TryFromIntError| anyhow::anyhow!(e.to_string()))?,
    );

    instructions.splice(0..0, [set_compute_unit_price_ix, set_compute_budget_ix]);

    match alt_info {
        Some(alt_info) => {
            let alt_pubkey = alt_info
                .alt_pubkey
                .ok_or_else(|| anyhow::anyhow!("ALTInfo provided without pubkey"))?;

            let alt_ref = AddressLookupTableAccount {
                key: alt_pubkey,
                addresses: alt_addresses,
            };
            let v0_msg =
                v0::Message::try_compile(&payer.pubkey(), &instructions, &[alt_ref], recent_hash)
                    .map_err(|e| anyhow::anyhow!(e.to_string()))?;
            let message = VersionedMessage::V0(v0_msg);
            let versioned_tx = VersionedTransaction::try_new(message, &signing_keypairs)
                .map_err(|e| anyhow::anyhow!(e.to_string()))?;
            Ok(SolanaTransactionType::Versioned(versioned_tx))
        }
        None => Ok(SolanaTransactionType::Legacy(
            Transaction::new_signed_with_payer(
                &instructions,
                Some(&payer.pubkey()),
                &signing_keypairs,
                recent_hash,
            ),
        )),
    }
}

pub fn not_enough_gas_event<G: GmpApiTrait>(
    available_gas_balance: i64,
    required_gas: u64,
    task: ExecuteTask,
    gmp_api: Arc<G>,
) -> Vec<Event> {
    let error_message = format!(
        "Not enough gas to execute message. Available gas: {}, required gas: {}",
        available_gas_balance, required_gas
    );
    let event = gmp_api.cannot_execute_message(
        task.common.id.clone(),
        task.task.message.message_id.clone(),
        task.task.message.source_chain,
        error_message,
        CannotExecuteMessageReason::InsufficientGas,
    );
    vec![event]
}

pub fn is_recoverable(transaction_error: &TransactionError) -> bool {
    !matches!(
        transaction_error,
        TransactionError::InstructionError(_, _)
            | TransactionError::InvalidProgramForExecution
            | TransactionError::InvalidWritableAccount
            | TransactionError::TooManyAccountLocks
            | TransactionError::ProgramCacheHitMaxLimit
    )
}

/// Checks if a string contains the "account already in use" error pattern.
/// Matches: "Allocate: account Address { address: <address>, base: None } already in use"
/// Returns true only if the extracted address matches the expected_address exactly.
pub fn is_addr_in_use(s: &str, expected_address: &str) -> bool {
    let pattern = r"Allocate: account Address \{ address: ([1-9A-HJ-NP-Za-km-z]{32,44}), base: None \} already in use";
    Regex::new(pattern)
        .ok()
        .and_then(|re| re.captures(s))
        .and_then(|caps| caps.get(1))
        .map(|m| m.as_str() == expected_address)
        .unwrap_or(false)
}

/// Checks if a string contains the "SlotAlreadyVerified" error pattern.
/// Matches: "Error Code: SlotAlreadyVerified"
pub fn is_slot_already_verified(s: &str) -> bool {
    let pattern = r"Error Code: SlotAlreadyVerified";
    Regex::new(pattern)
        .map(|re| re.is_match(s))
        .unwrap_or(false)
}

pub fn keypair_to_base58_string(keypair: &Keypair) -> String {
    keypair.to_base58_string()
}

pub fn keypair_from_base58_string(s: &str) -> Result<Keypair, anyhow::Error> {
    let mut buf = [0u8; 64]; // Ed25519 keypair length
    bs58::decode(s)
        .onto(&mut buf)
        .map_err(|e| anyhow!("Failed to decode base58 keypair: {}", e))?;
    Keypair::from_bytes(&buf).map_err(|e| anyhow!("Failed to create keypair from bytes: {}", e))
}

pub fn check_if_error_includes_an_expected_account(
    error_message: &str,
    execute_data: &ExecuteData,
) -> Result<bool, anyhow::Error> {
    let (initialize_verification_session_pda, _) = get_initialize_verification_session_pda(
        &execute_data.payload_merkle_root,
        &execute_data.signing_verifier_set_merkle_root,
    )?;
    if is_addr_in_use(
        error_message,
        &initialize_verification_session_pda.to_string(),
    ) {
        return Ok(true);
    }
    let command_ids: Option<Vec<[u8; 32]>> = match &execute_data.payload_items {
        MerklizedPayload::NewMessages { messages } => {
            let ids: Vec<[u8; 32]> = messages
                .iter()
                .map(|message| message.leaf.message.command_id())
                .collect();
            if ids.is_empty() {
                None
            } else {
                Some(ids)
            }
        }
        MerklizedPayload::VerifierSetRotation {
            new_verifier_set_merkle_root: _,
        } => None,
    };
    if let Some(command_ids) = command_ids {
        let incoming_message_pdas: Vec<Pubkey> = command_ids
            .iter()
            .map(|command_id| {
                let (pda, _) = get_incoming_message_pda(command_id)
                    .map_err(|e| anyhow::anyhow!("Failed to get incoming message PDA: {}", e))?;
                Ok(pda)
            })
            .collect::<Result<Vec<_>, anyhow::Error>>()?;
        for incoming_message_pda in incoming_message_pdas {
            if is_addr_in_use(error_message, &incoming_message_pda.to_string()) {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

pub fn extract_proposal_hash_from_payload(payload: &[u8]) -> Result<[u8; 32], anyhow::Error> {
    let cmd_payload =
        <governance_gmp::GovernanceCommandPayload as alloy_sol_types::SolType>::abi_decode(
            payload, true,
        )
        .map_err(|e| anyhow::anyhow!(format!("Failed to decode governance payload: {}", e)))?;

    let target_bytes: [u8; 32] = cmd_payload
        .target
        .as_ref()
        .try_into()
        .map_err(|_| anyhow::anyhow!("Invalid target length in governance payload"))?;
    let target = Pubkey::from(target_bytes);

    let call_data: solana_axelar_governance::ExecuteProposalCallData =
        anchor_lang::AnchorDeserialize::try_from_slice(&cmd_payload.call_data)
            .map_err(|e| anyhow::anyhow!("Failed to decode call_data: {}", e))?;

    let proposal_hash =
        solana_axelar_governance::state::proposal::ExecutableProposal::calculate_hash(
            &target,
            &call_data,
            &cmd_payload.native_value.to_le_bytes::<32>(),
        );

    Ok(proposal_hash)
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_sol_types::SolValue;
    use anchor_lang::InstructionData;
    use governance_gmp::alloy_primitives::U256;
    use solana_axelar_governance::state::proposal::ExecutableProposal;
    use solana_axelar_governance::{ExecuteProposalCallData, SolanaAccountMetadata};
    use solana_axelar_std::execute_data::PayloadType;
    use solana_axelar_std::message::MessageLeaf;
    use solana_axelar_std::verifier_set::{SigningVerifierSetInfo, VerifierSetLeaf};
    use solana_axelar_std::{
        CrossChainId, MerklizedMessage, Message, PublicKey, Signature as AxelarSignature,
    };
    use solana_sdk::signature::Keypair;

    fn create_test_execute_data_single_message(
        payload_merkle_root: [u8; 32],
        signing_verifier_set_merkle_root: [u8; 32],
    ) -> ExecuteData {
        let verifier_info = SigningVerifierSetInfo {
            payload_type: PayloadType::ApproveMessages,
            leaf: VerifierSetLeaf {
                nonce: 0,
                quorum: 0,
                signer_pubkey: PublicKey([0; 33]),
                signer_weight: 0,
                position: 0,
                set_size: 0,
                domain_separator: [0; 32],
            },
            signature: AxelarSignature([0; 65]),
            merkle_proof: vec![],
        };

        ExecuteData {
            payload_merkle_root,
            signing_verifier_set_merkle_root,
            signing_verifier_set_leaves: vec![verifier_info],
            payload_items: MerklizedPayload::NewMessages {
                messages: vec![MerklizedMessage {
                    leaf: MessageLeaf {
                        message: Message {
                            cc_id: CrossChainId {
                                chain: "test-chain".to_string(),
                                id: "test-message-id".to_string(),
                            },
                            source_address: "test-source-address".to_string(),
                            destination_chain: "test-destination-chain".to_string(),
                            destination_address: "test-destination-address".to_string(),
                            payload_hash: [0; 32],
                        },
                        position: 0,
                        set_size: 0,
                        domain_separator: [0; 32],
                    },
                    proof: vec![],
                }],
            },
        }
    }

    fn create_test_execute_data_multiple_messages(
        payload_merkle_root: [u8; 32],
        signing_verifier_set_merkle_root: [u8; 32],
    ) -> ExecuteData {
        let verifier_info = SigningVerifierSetInfo {
            payload_type: PayloadType::ApproveMessages,
            leaf: VerifierSetLeaf {
                nonce: 0,
                quorum: 0,
                signer_pubkey: PublicKey([0; 33]),
                signer_weight: 0,
                position: 0,
                set_size: 0,
                domain_separator: [0; 32],
            },
            signature: AxelarSignature([0; 65]),
            merkle_proof: vec![],
        };

        ExecuteData {
            payload_merkle_root,
            signing_verifier_set_merkle_root,
            signing_verifier_set_leaves: vec![verifier_info],
            payload_items: MerklizedPayload::NewMessages {
                messages: vec![
                    MerklizedMessage {
                        leaf: MessageLeaf {
                            message: Message {
                                cc_id: CrossChainId {
                                    chain: "test-chain-1".to_string(),
                                    id: "test-message-id-1".to_string(),
                                },
                                source_address: "test-source-address-1".to_string(),
                                destination_chain: "test-destination-chain-1".to_string(),
                                destination_address: "test-destination-address-1".to_string(),
                                payload_hash: [1; 32],
                            },
                            position: 0,
                            set_size: 2,
                            domain_separator: [0; 32],
                        },
                        proof: vec![],
                    },
                    MerklizedMessage {
                        leaf: MessageLeaf {
                            message: Message {
                                cc_id: CrossChainId {
                                    chain: "test-chain-2".to_string(),
                                    id: "test-message-id-2".to_string(),
                                },
                                source_address: "test-source-address-2".to_string(),
                                destination_chain: "test-destination-chain-2".to_string(),
                                destination_address: "test-destination-address-2".to_string(),
                                payload_hash: [2; 32],
                            },
                            position: 1,
                            set_size: 2,
                            domain_separator: [0; 32],
                        },
                        proof: vec![],
                    },
                    MerklizedMessage {
                        leaf: MessageLeaf {
                            message: Message {
                                cc_id: CrossChainId {
                                    chain: "test-chain-3".to_string(),
                                    id: "test-message-id-3".to_string(),
                                },
                                source_address: "test-source-address-3".to_string(),
                                destination_chain: "test-destination-chain-3".to_string(),
                                destination_address: "test-destination-address-3".to_string(),
                                payload_hash: [3; 32],
                            },
                            position: 2,
                            set_size: 2,
                            domain_separator: [0; 32],
                        },
                        proof: vec![],
                    },
                ],
            },
        }
    }

    fn create_test_execute_data_verifier_rotation(
        payload_merkle_root: [u8; 32],
        signing_verifier_set_merkle_root: [u8; 32],
    ) -> ExecuteData {
        let verifier_info = SigningVerifierSetInfo {
            payload_type: PayloadType::RotateSigners,
            leaf: VerifierSetLeaf {
                nonce: 0,
                quorum: 0,
                signer_pubkey: PublicKey([0; 33]),
                signer_weight: 0,
                position: 0,
                set_size: 0,
                domain_separator: [0; 32],
            },
            signature: AxelarSignature([0; 65]),
            merkle_proof: vec![],
        };

        ExecuteData {
            payload_merkle_root,
            signing_verifier_set_merkle_root,
            signing_verifier_set_leaves: vec![verifier_info],
            payload_items: MerklizedPayload::VerifierSetRotation {
                new_verifier_set_merkle_root: [99u8; 32],
            },
        }
    }

    // Tests for check_if_error_includes_an_expected_account
    #[test]
    fn test_check_error_returns_true_for_init_verification_session_pda() {
        let payload_merkle_root = [1u8; 32];
        let signing_verifier_set_merkle_root = [2u8; 32];
        let execute_data = create_test_execute_data_single_message(
            payload_merkle_root,
            signing_verifier_set_merkle_root,
        );

        let (init_pda, _) = get_initialize_verification_session_pda(
            &payload_merkle_root,
            &signing_verifier_set_merkle_root,
        )
        .unwrap();

        let error_message = format!(
            "Allocate: account Address {{ address: {}, base: None }} already in use",
            init_pda
        );

        assert!(
            check_if_error_includes_an_expected_account(&error_message, &execute_data).unwrap()
        );
    }

    #[test]
    fn test_check_error_returns_true_for_incoming_message_pda_single_message() {
        let payload_merkle_root = [1u8; 32];
        let signing_verifier_set_merkle_root = [2u8; 32];
        let execute_data = create_test_execute_data_single_message(
            payload_merkle_root,
            signing_verifier_set_merkle_root,
        );

        let command_id = match &execute_data.payload_items {
            MerklizedPayload::NewMessages { messages } => messages[0].leaf.message.command_id(),
            _ => panic!("Expected NewMessages"),
        };
        let (incoming_msg_pda, _) = get_incoming_message_pda(&command_id).unwrap();

        let error_message = format!(
            "Allocate: account Address {{ address: {}, base: None }} already in use",
            incoming_msg_pda
        );

        assert!(
            check_if_error_includes_an_expected_account(&error_message, &execute_data).unwrap()
        );
    }

    #[test]
    fn test_check_error_returns_true_for_first_message_pda_multiple_messages() {
        let payload_merkle_root = [1u8; 32];
        let signing_verifier_set_merkle_root = [2u8; 32];
        let execute_data = create_test_execute_data_multiple_messages(
            payload_merkle_root,
            signing_verifier_set_merkle_root,
        );

        let command_id = match &execute_data.payload_items {
            MerklizedPayload::NewMessages { messages } => messages[0].leaf.message.command_id(),
            _ => panic!("Expected NewMessages"),
        };
        let (incoming_msg_pda, _) = get_incoming_message_pda(&command_id).unwrap();

        let error_message = format!(
            "Allocate: account Address {{ address: {}, base: None }} already in use",
            incoming_msg_pda
        );

        assert!(
            check_if_error_includes_an_expected_account(&error_message, &execute_data).unwrap()
        );
    }

    #[test]
    fn test_check_error_returns_true_for_second_message_pda_multiple_messages() {
        let payload_merkle_root = [1u8; 32];
        let signing_verifier_set_merkle_root = [2u8; 32];
        let execute_data = create_test_execute_data_multiple_messages(
            payload_merkle_root,
            signing_verifier_set_merkle_root,
        );

        let command_id = match &execute_data.payload_items {
            MerklizedPayload::NewMessages { messages } => messages[1].leaf.message.command_id(),
            _ => panic!("Expected NewMessages"),
        };
        let (incoming_msg_pda, _) = get_incoming_message_pda(&command_id).unwrap();

        let error_message = format!(
            "Allocate: account Address {{ address: {}, base: None }} already in use",
            incoming_msg_pda
        );

        assert!(
            check_if_error_includes_an_expected_account(&error_message, &execute_data).unwrap()
        );
    }

    #[test]
    fn test_check_error_returns_true_for_third_message_pda_multiple_messages() {
        let payload_merkle_root = [1u8; 32];
        let signing_verifier_set_merkle_root = [2u8; 32];
        let execute_data = create_test_execute_data_multiple_messages(
            payload_merkle_root,
            signing_verifier_set_merkle_root,
        );

        let command_id = match &execute_data.payload_items {
            MerklizedPayload::NewMessages { messages } => messages[2].leaf.message.command_id(),
            _ => panic!("Expected NewMessages"),
        };
        let (incoming_msg_pda, _) = get_incoming_message_pda(&command_id).unwrap();

        let error_message = format!(
            "Allocate: account Address {{ address: {}, base: None }} already in use",
            incoming_msg_pda
        );

        assert!(
            check_if_error_includes_an_expected_account(&error_message, &execute_data).unwrap()
        );
    }

    #[test]
    fn test_check_error_returns_false_for_different_address() {
        let payload_merkle_root = [1u8; 32];
        let signing_verifier_set_merkle_root = [2u8; 32];
        let execute_data = create_test_execute_data_single_message(
            payload_merkle_root,
            signing_verifier_set_merkle_root,
        );

        let different_address = "11111111111111111111111111111111";
        let error_message = format!(
            "Allocate: account Address {{ address: {}, base: None }} already in use",
            different_address
        );

        assert!(
            !check_if_error_includes_an_expected_account(&error_message, &execute_data).unwrap()
        );
    }

    #[test]
    fn test_check_error_returns_false_for_no_pattern_match() {
        let payload_merkle_root = [1u8; 32];
        let signing_verifier_set_merkle_root = [2u8; 32];
        let execute_data = create_test_execute_data_single_message(
            payload_merkle_root,
            signing_verifier_set_merkle_root,
        );

        let error_message = "Some other error occurred";

        assert!(
            !check_if_error_includes_an_expected_account(error_message, &execute_data).unwrap()
        );
    }

    #[test]
    fn test_check_error_returns_false_for_verifier_set_rotation() {
        let payload_merkle_root = [1u8; 32];
        let signing_verifier_set_merkle_root = [2u8; 32];
        let execute_data = create_test_execute_data_verifier_rotation(
            payload_merkle_root,
            signing_verifier_set_merkle_root,
        );

        // Use a random address that isn't the init_pda
        let different_address = "11111111111111111111111111111111";
        let error_message = format!(
            "Allocate: account Address {{ address: {}, base: None }} already in use",
            different_address
        );

        // Should return false because VerifierSetRotation has no incoming message PDAs
        // and the address doesn't match init_pda
        assert!(
            !check_if_error_includes_an_expected_account(&error_message, &execute_data).unwrap()
        );
    }

    #[test]
    fn test_check_error_returns_true_for_init_pda_with_verifier_set_rotation() {
        let payload_merkle_root = [1u8; 32];
        let signing_verifier_set_merkle_root = [2u8; 32];
        let execute_data = create_test_execute_data_verifier_rotation(
            payload_merkle_root,
            signing_verifier_set_merkle_root,
        );

        let (init_pda, _) = get_initialize_verification_session_pda(
            &payload_merkle_root,
            &signing_verifier_set_merkle_root,
        )
        .unwrap();

        let error_message = format!(
            "Allocate: account Address {{ address: {}, base: None }} already in use",
            init_pda
        );

        // Should return true because init_pda matches (even for VerifierSetRotation)
        assert!(
            check_if_error_includes_an_expected_account(&error_message, &execute_data).unwrap()
        );
    }

    /// Helper to create memo instruction call data for governance tests
    fn create_memo_call_data(
        memo: &str,
        value_receiver: Pubkey,
        governance_config_pda: Pubkey,
    ) -> ExecuteProposalCallData {
        let memo_instruction_data = solana_axelar_memo::instruction::EmitMemo {
            message: memo.to_string(),
        }
        .data();

        let governance_config_pda_metadata = SolanaAccountMetadata {
            pubkey: governance_config_pda.to_bytes(),
            is_signer: true,
            is_writable: false,
        };

        let value_receiver_metadata = SolanaAccountMetadata {
            pubkey: value_receiver.to_bytes(),
            is_signer: false,
            is_writable: true,
        };

        ExecuteProposalCallData {
            solana_accounts: vec![
                value_receiver_metadata.clone(),
                governance_config_pda_metadata,
            ],
            solana_native_value_receiver_account: Some(value_receiver_metadata),
            call_data: memo_instruction_data,
        }
    }

    #[test]
    fn test_extract_proposal_hash_schedule_timelock() {
        // Setup test data similar to governance program tests
        let memo_program_id = solana_axelar_memo::ID;
        let value_receiver = Pubkey::new_unique();
        let governance_config_pda = Pubkey::new_unique();

        let call_data = create_memo_call_data(
            "Test governance memo",
            value_receiver,
            governance_config_pda,
        );

        let target_bytes: [u8; 32] = memo_program_id.to_bytes();
        let native_value = U256::from(0u64);
        let eta = U256::from(1800000000u64);

        // Create the governance payload
        let gmp_payload = governance_gmp::GovernanceCommandPayload {
            command: governance_gmp::GovernanceCommand::ScheduleTimeLockProposal,
            target: target_bytes.to_vec().into(),
            call_data: borsh::to_vec(&call_data).unwrap().into(),
            native_value,
            eta,
        };
        let encoded_payload = gmp_payload.abi_encode();

        // Extract the proposal hash
        let result = extract_proposal_hash_from_payload(&encoded_payload);
        assert!(result.is_ok(), "Should successfully extract proposal hash");

        let proposal_hash = result.unwrap();

        // Verify by computing expected hash
        let expected_hash = ExecutableProposal::calculate_hash(
            &memo_program_id,
            &call_data,
            &native_value.to_le_bytes::<32>(),
        );

        assert_eq!(
            proposal_hash, expected_hash,
            "Extracted hash should match expected"
        );
    }

    #[test]
    fn test_extract_proposal_hash_with_native_value() {
        let memo_program_id = solana_axelar_memo::ID;
        let value_receiver = Pubkey::new_unique();
        let governance_config_pda = Pubkey::new_unique();

        let call_data =
            create_memo_call_data("Proposal with value", value_receiver, governance_config_pda);

        let target_bytes: [u8; 32] = memo_program_id.to_bytes();
        let native_value_u64 = 1_000_000u64; // 0.001 SOL
        let native_value = U256::from(native_value_u64);
        let eta = U256::from(1800000000u64);

        let gmp_payload = governance_gmp::GovernanceCommandPayload {
            command: governance_gmp::GovernanceCommand::ScheduleTimeLockProposal,
            target: target_bytes.to_vec().into(),
            call_data: borsh::to_vec(&call_data).unwrap().into(),
            native_value,
            eta,
        };
        let encoded_payload = gmp_payload.abi_encode();

        let result = extract_proposal_hash_from_payload(&encoded_payload);
        assert!(result.is_ok());

        let proposal_hash = result.unwrap();
        let expected_hash = ExecutableProposal::calculate_hash(
            &memo_program_id,
            &call_data,
            &native_value.to_le_bytes::<32>(),
        );

        assert_eq!(proposal_hash, expected_hash);
    }

    #[test]
    fn test_extract_proposal_hash_approve_operator() {
        let memo_program_id = solana_axelar_memo::ID;
        let value_receiver = Pubkey::new_unique();
        let governance_config_pda = Pubkey::new_unique();

        let call_data =
            create_memo_call_data("Operator proposal", value_receiver, governance_config_pda);

        let target_bytes: [u8; 32] = memo_program_id.to_bytes();
        let native_value = U256::from(0u64);
        let eta = U256::from(1800000000u64);

        // Different command type
        let gmp_payload = governance_gmp::GovernanceCommandPayload {
            command: governance_gmp::GovernanceCommand::ApproveOperatorProposal,
            target: target_bytes.to_vec().into(),
            call_data: borsh::to_vec(&call_data).unwrap().into(),
            native_value,
            eta,
        };
        let encoded_payload = gmp_payload.abi_encode();

        let result = extract_proposal_hash_from_payload(&encoded_payload);
        assert!(result.is_ok());

        // Hash should be the same regardless of command type
        let proposal_hash = result.unwrap();
        let expected_hash = ExecutableProposal::calculate_hash(
            &memo_program_id,
            &call_data,
            &native_value.to_le_bytes::<32>(),
        );

        assert_eq!(proposal_hash, expected_hash);
    }

    #[test]
    fn test_extract_proposal_hash_invalid_payload() {
        // Invalid/malformed payload
        let invalid_payload = vec![0u8; 32];
        let result = extract_proposal_hash_from_payload(&invalid_payload);
        assert!(result.is_err(), "Should fail for invalid payload");
    }

    #[test]
    fn test_extract_proposal_hash_invalid_target_length() {
        // Create payload with invalid target length (not 32 bytes)
        let short_target = vec![1u8; 16]; // Only 16 bytes instead of 32
        let call_data = ExecuteProposalCallData {
            solana_accounts: vec![],
            solana_native_value_receiver_account: None,
            call_data: vec![],
        };

        let gmp_payload = governance_gmp::GovernanceCommandPayload {
            command: governance_gmp::GovernanceCommand::ScheduleTimeLockProposal,
            target: short_target.into(),
            call_data: borsh::to_vec(&call_data).unwrap().into(),
            native_value: U256::from(0u64),
            eta: U256::from(0u64),
        };
        let encoded_payload = gmp_payload.abi_encode();

        let result = extract_proposal_hash_from_payload(&encoded_payload);
        assert!(
            result.is_err(),
            "Should fail for invalid target length: {:?}",
            result
        );
    }

    #[test]
    fn test_keypair_roundtrip() {
        let original_keypair = Keypair::new();
        let base58_string = keypair_to_base58_string(&original_keypair);
        let reconstructed_keypair = keypair_from_base58_string(&base58_string).unwrap();

        // Verify the public keys match (since we can't compare private keys directly)
        assert_eq!(original_keypair.pubkey(), reconstructed_keypair.pubkey());
    }

    #[test]
    fn test_keypair_from_invalid_base58() {
        let result = keypair_from_base58_string("invalid-base58-string");
        assert!(result.is_err());
    }

    #[test]
    fn test_keypair_from_wrong_length() {
        // Create a valid base58 string but for a shorter key
        let short_keypair = Keypair::new();
        let short_pubkey_base58 = short_keypair.pubkey().to_string();

        let result = keypair_from_base58_string(&short_pubkey_base58);
        assert!(result.is_err());
    }

    #[test]
    fn test_is_recoverable() {
        let transaction_error = TransactionError::InvalidWritableAccount;
        assert!(!is_recoverable(&transaction_error));

        let transaction_error = TransactionError::TooManyAccountLocks;
        assert!(!is_recoverable(&transaction_error));

        let transaction_error = TransactionError::ProgramCacheHitMaxLimit;
        assert!(!is_recoverable(&transaction_error));

        let transaction_error = TransactionError::InvalidProgramForExecution;
        assert!(!is_recoverable(&transaction_error));

        let transaction_error = TransactionError::AccountNotFound;
        assert!(is_recoverable(&transaction_error));
    }

    #[test]
    fn test_is_addr_in_use() {
        let error_msg = "Allocate: account Address { address: FAVDxWyV1GcvRxaDjv12jo1foDbU7uDYfrbuky68JGHK, base: None } already in use";
        assert!(is_addr_in_use(
            error_msg,
            "FAVDxWyV1GcvRxaDjv12jo1foDbU7uDYfrbuky68JGHK"
        ));
        assert!(!is_addr_in_use(
            error_msg,
            "DifferentAddress1111111111111111111111111"
        ));

        let error_msg2 = "Allocate: account Address { address: 11111111111111111111111111111111, base: None } already in use";
        assert!(is_addr_in_use(
            error_msg2,
            "11111111111111111111111111111111"
        ));
        assert!(!is_addr_in_use(
            error_msg2,
            "FAVDxWyV1GcvRxaDjv12jo1foDbU7uDYfrbuky68JGHK"
        ));

        let normal_msg = "Some other error message";
        assert!(!is_addr_in_use(
            normal_msg,
            "FAVDxWyV1GcvRxaDjv12jo1foDbU7uDYfrbuky68JGHK"
        ));

        let partial_msg =
            "Allocate: account Address { address: FAVDxWyV1GcvRxaDjv12jo1foDbU7uDYfrbuky68JGHK";
        assert!(!is_addr_in_use(
            partial_msg,
            "FAVDxWyV1GcvRxaDjv12jo1foDbU7uDYfrbuky68JGHK"
        ));

        let embedded_msg = "Error: Allocate: account Address { address: FAVDxWyV1GcvRxaDjv12jo1foDbU7uDYfrbuky68JGHK, base: None } already in use - transaction failed";
        assert!(is_addr_in_use(
            embedded_msg,
            "FAVDxWyV1GcvRxaDjv12jo1foDbU7uDYfrbuky68JGHK"
        ));

        let full_message = r#"2025-11-25T18:02:30.415586Z DEBUG solana_rpc_client::nonblocking::rpc_client: -32002 Transaction simulation failed: Error processing Instruction 2: custom program error: 0x0
2025-11-25T18:02:30.415641Z DEBUG solana_rpc_client::nonblocking::rpc_client:   1: Program ComputeBudget111111111111111111111111111111 invoke [1]
2025-11-25T18:02:30.415672Z DEBUG solana_rpc_client::nonblocking::rpc_client:   2: Program ComputeBudget111111111111111111111111111111 success
2025-11-25T18:02:30.415700Z DEBUG solana_rpc_client::nonblocking::rpc_client:   3: Program ComputeBudget111111111111111111111111111111 invoke [1]
2025-11-25T18:02:30.415727Z DEBUG solana_rpc_client::nonblocking::rpc_client:   4: Program ComputeBudget111111111111111111111111111111 success
2025-11-25T18:02:30.415754Z DEBUG solana_rpc_client::nonblocking::rpc_client:   5: Program gtw3LYHmSe3y1cRqCeBuTpyB4KDQHfaqqHQs6Rw19DX invoke [1]
2025-11-25T18:02:30.415781Z DEBUG solana_rpc_client::nonblocking::rpc_client:   6: Program log: Instruction: InitializePayloadVerificationSession
2025-11-25T18:02:30.415808Z DEBUG solana_rpc_client::nonblocking::rpc_client:   7: Program 11111111111111111111111111111111 invoke [2]
2025-11-25T18:02:30.415835Z DEBUG solana_rpc_client::nonblocking::rpc_client:   8: Allocate: account Address { address: FAVDxWyV1GcvRxaDjv12jo1foDbU7uDYfrbuky68JGHK, base: None } already in use
2025-11-25T18:02:30.415864Z DEBUG solana_rpc_client::nonblocking::rpc_client:   9: Program 11111111111111111111111111111111 failed: custom program error: 0x0
2025-11-25T18:02:30.415891Z DEBUG solana_rpc_client::nonblocking::rpc_client:  10: Program gtw3LYHmSe3y1cRqCeBuTpyB4KDQHfaqqHQs6Rw19DX consumed 5909 of 7150 compute units
2025-11-25T18:02:30.415919Z DEBUG solana_rpc_client::nonblocking::rpc_client:  11: Program gtw3LYHmSe3y1cRqCeBuTpyB4KDQHfaqqHQs6Rw19DX failed: custom program error: 0x0
2025-11-25T18:02:30.415947Z DEBUG solana_rpc_client::nonblocking::rpc_client: 
2025-11-25T18:02:30.416016Z ERROR relayer_core::includer: Failed to consume delivery: GatewayTxTaskError("Generic error: Generic error: Generic error: TransactionError: Error processing Instruction 2: custom program error: 0x0")"#;
        assert!(is_addr_in_use(
            full_message,
            "FAVDxWyV1GcvRxaDjv12jo1foDbU7uDYfrbuky68JGHK"
        ));
        assert!(!is_addr_in_use(
            full_message,
            "DifferentAddress1111111111111111111111111"
        ));
    }

    #[test]
    fn test_is_slot_already_verified() {
        let error_msg = "Error Code: SlotAlreadyVerified";
        assert!(is_slot_already_verified(error_msg));

        let error_msg2 = "Program log: AnchorError thrown in programs/solana-axelar-gateway/src/state/verification_session.rs:164. Error Code: SlotAlreadyVerified. Error Number: 6006. Error Message: SlotAlreadyVerified.";
        assert!(is_slot_already_verified(error_msg2));

        let normal_msg = "Some other error message";
        assert!(!is_slot_already_verified(normal_msg));

        let partial_msg = "Error Code: SlotAlready";
        assert!(!is_slot_already_verified(partial_msg));

        let embedded_msg = "Error: Program log: AnchorError thrown in programs/solana-axelar-gateway/src/state/verification_session.rs:164. Error Code: SlotAlreadyVerified. Error Number: 6006.";
        assert!(is_slot_already_verified(embedded_msg));

        let full_message = r#"2025-11-26T16:50:25.617503Z DEBUG solana_rpc_client::nonblocking::rpc_client: -32002 Transaction simulation failed: Error processing Instruction 2: custom program error: 0x1776
2025-11-26T16:50:25.617579Z DEBUG solana_rpc_client::nonblocking::rpc_client:   1: Program ComputeBudget111111111111111111111111111111 invoke [1]
2025-11-26T16:50:25.617714Z DEBUG solana_rpc_client::nonblocking::rpc_client:   2: Program ComputeBudget111111111111111111111111111111 success
2025-11-26T16:50:25.617786Z DEBUG solana_rpc_client::nonblocking::rpc_client:   3: Program ComputeBudget111111111111111111111111111111 invoke [1]
2025-11-26T16:50:25.617824Z DEBUG solana_rpc_client::nonblocking::rpc_client:   4: Program ComputeBudget111111111111111111111111111111 success
2025-11-26T16:50:25.617928Z DEBUG solana_rpc_client::nonblocking::rpc_client:   5: Program gtw3LYHmSe3y1cRqCeBuTpyB4KDQHfaqqHQs6Rw19DX invoke [1]
2025-11-26T16:50:25.618014Z DEBUG solana_rpc_client::nonblocking::rpc_client:   6: Program log: Instruction: VerifySignature
2025-11-26T16:50:25.618080Z DEBUG solana_rpc_client::nonblocking::rpc_client:   7: Program log: AnchorError thrown in programs/solana-axelar-gateway/src/state/verification_session.rs:164. Error Code: SlotAlreadyVerified. Error Number: 6006. Error Message: SlotAlreadyVerified.
2025-11-26T16:50:25.618115Z DEBUG solana_rpc_client::nonblocking::rpc_client:   8: Program gtw3LYHmSe3y1cRqCeBuTpyB4KDQHfaqqHQs6Rw19DX consumed 9333 of 11259 compute units
2025-11-26T16:50:25.618140Z DEBUG solana_rpc_client::nonblocking::rpc_client:   9: Program gtw3LYHmSe3y1cRqCeBuTpyB4KDQHfaqqHQs6Rw19DX failed: custom program error: 0x1776
2025-11-26T16:50:25.618165Z DEBUG solana_rpc_client::nonblocking::rpc_client: 
2025-11-26T16:50:25.618307Z ERROR relayer_core::includer: Failed to consume delivery: GatewayTxTaskError("Generic error: TransactionError: Error processing Instruction 2: custom program error: 0x1776")"#;
        assert!(is_slot_already_verified(full_message));
    }
}
