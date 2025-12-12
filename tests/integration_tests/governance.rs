//! Governance integration tests
//!
//! Tests for the governance program's ProcessGmp instruction.

use std::sync::Arc;

use alloy_sol_types::SolValue as _;
use anchor_lang::{InstructionData, ToAccountMetas};
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use borsh::BorshSerialize;
use governance_gmp::alloy_primitives::U256;
use relayer_core::gmp_api::MockGmpApiTrait;
use relayer_core::includer_worker::IncluderTrait;
use solana::includer::SolanaIncluder;
use solana::mocks::MockRefundsModel;
use solana_axelar_gateway_test_fixtures::create_verifier_info;
use solana_axelar_governance::SolanaAccountMetadata;
use solana_axelar_std::execute_data::{ExecuteData, MerklizedPayload};
use solana_axelar_std::{
    hasher::LeafHash, CrossChainId, MerkleTree, MerklizedMessage, Message, MessageLeaf, PayloadType,
};
use solana_sdk::instruction::Instruction;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signer;
use solana_sdk::transaction::Transaction;
use solana_transaction_parser::gmp_types::{
    Amount, CommonTaskFields, ExecuteTask, ExecuteTaskFields, GatewayTxTask, GatewayTxTaskFields,
    GatewayV2Message,
};

use super::common::*;

fn get_governance_config_pda() -> (Pubkey, u8) {
    Pubkey::find_program_address(
        &[solana_axelar_governance::seed_prefixes::GOVERNANCE_CONFIG],
        &solana_axelar_governance::ID,
    )
}

fn get_memo_instruction_data(
    memo: String,
    value_receiver: SolanaAccountMetadata,
) -> solana_axelar_governance::ExecuteProposalCallData {
    let memo_instruction_data = solana_axelar_memo::instruction::EmitMemo { message: memo }.data();

    let (governance_config_pda, _) = get_governance_config_pda();

    let governance_config_pda_metadata = SolanaAccountMetadata {
        pubkey: governance_config_pda.to_bytes(),
        is_signer: true,
        is_writable: false,
    };

    let solana_accounts = vec![value_receiver.clone(), governance_config_pda_metadata];

    solana_axelar_governance::ExecuteProposalCallData {
        solana_accounts,
        solana_native_value_receiver_account: Some(value_receiver),
        call_data: memo_instruction_data,
    }
}

/// Test ProcessGmp with ScheduleTimeLockProposal command.
/// This tests the full flow: initialize governance, approve message, execute via includer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_governance_schedule_timelock_proposal() {
    let env = TestEnvironment::new().await;
    let test_queue = Arc::new(TestQueue::new());

    let (governance_config_pda, _governance_config_bump) = get_governance_config_pda();
    let program_data =
        solana_sdk::bpf_loader_upgradeable::get_program_data_address(&solana_axelar_governance::ID);

    let authorized_chain = "ethereum";
    let authorized_address = "0xSourceAddress";
    let chain_hash = solana_program::keccak::hashv(&[authorized_chain.as_bytes()]).to_bytes();
    let address_hash = solana_program::keccak::hashv(&[authorized_address.as_bytes()]).to_bytes();

    let init_params = solana_axelar_governance::GovernanceConfigInit {
        chain_hash,
        address_hash,
        minimum_proposal_eta_delay: 3600, // Minimum allowed: 1 hour
        operator: env.operator.pubkey().to_bytes(),
    };

    let init_ix = Instruction {
        program_id: solana_axelar_governance::ID,
        accounts: solana_axelar_governance::accounts::InitializeConfig {
            payer: env.payer.pubkey(),
            upgrade_authority: env.upgrade_authority.pubkey(),
            program_data,
            governance_config: governance_config_pda,
            system_program: solana_sdk::system_program::ID,
        }
        .to_account_metas(None),
        data: solana_axelar_governance::instruction::InitializeConfig {
            params: init_params,
        }
        .data(),
    };

    let recent_blockhash = env.rpc_client.get_latest_blockhash().await.unwrap();
    let init_tx = Transaction::new_signed_with_payer(
        &[init_ix],
        Some(&env.payer.pubkey()),
        &[&env.payer, &env.upgrade_authority],
        recent_blockhash,
    );

    env.rpc_client
        .send_and_confirm_transaction(&init_tx)
        .await
        .expect("Failed to initialize governance config");
    println!("Governance config initialized successfully");

    let memo = String::from("This is a governance proposal memo");
    let native_value_u64: u64 = 0;
    let eta: u64 = 1800000000;

    let value_receiver_pubkey = Pubkey::new_unique();
    let value_receiver = SolanaAccountMetadata {
        pubkey: value_receiver_pubkey.to_bytes(),
        is_signer: false,
        is_writable: true,
    };

    let target_bytes: [u8; 32] = env.memo_program_id.to_bytes();
    let call_data = get_memo_instruction_data(memo, value_receiver);
    let native_value = U256::from(native_value_u64);
    let eta_u256 = U256::from(eta);

    let gmp_payload = governance_gmp::GovernanceCommandPayload {
        command: governance_gmp::GovernanceCommand::ScheduleTimeLockProposal,
        target: target_bytes.to_vec().into(),
        call_data: call_data.try_to_vec().unwrap().into(),
        native_value,
        eta: eta_u256,
    };
    let governance_payload = gmp_payload.abi_encode();
    let payload_hash: [u8; 32] = solana_program::keccak::hashv(&[&governance_payload]).to_bytes();

    let message_id = "governance-schedule-001";

    let message = Message {
        cc_id: CrossChainId {
            chain: authorized_chain.to_string(),
            id: message_id.to_string(),
        },
        source_address: authorized_address.to_string(),
        destination_chain: "solana-devnet".to_string(),
        destination_address: solana_axelar_governance::ID.to_string(),
        payload_hash,
    };

    let message_leaf = MessageLeaf {
        message: message.clone(),
        position: 0,
        set_size: 1,
        domain_separator: env.domain_separator,
    };
    let message_leaf_hash = message_leaf.hash();
    let message_merkle_tree = MerkleTree::from_leaves(&[message_leaf_hash]);
    let payload_merkle_root = message_merkle_tree.root().expect("merkle root");

    let signing_verifier_set_merkle_root = env.verifier_set_hash;

    let verifier_info_1 = create_verifier_info(
        &env.verifier_secret_keys[0],
        payload_merkle_root,
        &env.verifier_leaves[0],
        0,
        &env.verifier_merkle_tree,
        PayloadType::ApproveMessages,
    );
    let verifier_info_2 = create_verifier_info(
        &env.verifier_secret_keys[1],
        payload_merkle_root,
        &env.verifier_leaves[1],
        1,
        &env.verifier_merkle_tree,
        PayloadType::ApproveMessages,
    );

    let execute_data = ExecuteData {
        payload_merkle_root,
        signing_verifier_set_merkle_root,
        signing_verifier_set_leaves: vec![verifier_info_1, verifier_info_2],
        payload_items: MerklizedPayload::NewMessages {
            messages: vec![MerklizedMessage {
                leaf: message_leaf.clone(),
                proof: vec![], // no proof needed for a single message
            }],
        },
    };

    let execute_data_b64 = BASE64_STANDARD.encode(execute_data.try_to_vec().unwrap());

    let gateway_task = GatewayTxTask {
        common: CommonTaskFields {
            id: "gov-approve-task".to_string(),
            chain: "solana-devnet".to_string(),
            timestamp: chrono::Utc::now().to_string(),
            r#type: "gateway_tx".to_string(),
            meta: None,
        },
        task: GatewayTxTaskFields {
            execute_data: execute_data_b64,
        },
    };

    let components = create_includer_components(&env.rpc_url, &env.payer);
    let mock_redis = create_mock_redis();
    let mock_gmp_api = MockGmpApiTrait::new();
    let mock_refunds_model = MockRefundsModel::new();

    let includer = SolanaIncluder::new(
        Arc::new(components.includer_client.clone()),
        components.keypair,
        "solana-devnet".to_string(),
        components.transaction_builder.clone(),
        Arc::new(mock_gmp_api),
        mock_redis,
        Arc::new(mock_refunds_model),
    );

    let approve_result = includer.handle_gateway_tx_task(gateway_task).await;
    match approve_result {
        Ok(_) => println!("Message approved successfully"),
        Err(e) => {
            println!("Message approval failed: {:?}", e);
        }
    }

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let execute_task = ExecuteTask {
        common: CommonTaskFields {
            id: "gov-execute-task".to_string(),
            chain: "solana-devnet".to_string(),
            timestamp: chrono::Utc::now().to_string(),
            r#type: "execute".to_string(),
            meta: None,
        },
        task: ExecuteTaskFields {
            message: GatewayV2Message {
                message_id: message_id.to_string(),
                source_chain: authorized_chain.to_string(),
                source_address: authorized_address.to_string(),
                destination_address: solana_axelar_governance::ID.to_string(),
                payload_hash: BASE64_STANDARD.encode(payload_hash),
            },
            payload: BASE64_STANDARD.encode(&governance_payload),
            available_gas_balance: Amount {
                token_id: None,
                amount: "1000000000".to_string(), // 1 SOL
            },
        },
    };

    let components_exec = create_includer_components(&env.rpc_url, &env.payer);
    let mock_redis_exec = create_mock_redis();
    let mock_gmp_api_exec = create_mock_gmp_api_for_execute();
    let mock_refunds_model_exec = MockRefundsModel::new();

    let includer_exec = SolanaIncluder::new(
        Arc::new(components_exec.includer_client),
        components_exec.keypair,
        "solana-devnet".to_string(),
        components_exec.transaction_builder,
        Arc::new(mock_gmp_api_exec),
        mock_redis_exec,
        Arc::new(mock_refunds_model_exec),
    );

    let execute_result = includer_exec.handle_execute_task(execute_task).await;

    match execute_result {
        Ok(events) => {
            println!("ProcessGmp executed! Events: {:?}", events.len());
            for event in &events {
                println!("Event: {:?}", event);
            }

            let has_success = events.iter().any(|e| {
                let event_str = format!("{:?}", e);
                !event_str.contains("REVERTED")
            });

            if events.is_empty() || has_success {
                println!("Governance test passed!");
            } else {
                panic!("Governance test failed, events: {:?}", events);
            }
        }
        Err(e) => {
            let error_str = format!("{:?}", e);
            panic!("ProcessGmp execution error: {}", error_str);
        }
    }

    println!("Governance Integration Test Completed");
    test_queue.clear().await;
    env.cleanup().await;
}
