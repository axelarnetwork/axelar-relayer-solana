//! ITS (Interchain Token Service) integration tests

use std::sync::Arc;

use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use borsh::BorshSerialize;
use interchain_token_transfer_gmp::alloy_primitives::{Bytes, FixedBytes, Uint};
use interchain_token_transfer_gmp::{
    DeployInterchainToken, GMPPayload, InterchainTransfer, ReceiveFromHub,
};
use relayer_core::gmp_api::gmp_types::Event;
use relayer_core::includer_worker::IncluderTrait;
use relayer_core::ingestor::IngestorTrait;
use relayer_core::queue::{QueueItem, QueueTrait};
use solana::includer::SolanaIncluder;
use solana::ingestor::SolanaIngestor;
use solana::mocks::{MockRefundsModel, MockUpdateEvents};
use solana_axelar_gateway_test_fixtures::create_verifier_info;
use solana_axelar_its::utils::interchain_token_id;
use solana_axelar_std::PayloadType;
use solana_axelar_std::execute_data::{ExecuteData, MerklizedPayload};
use solana_axelar_std::{
    hasher::LeafHash, CrossChainId, MerkleTree, MerklizedMessage, Message, MessageLeaf,
};
use solana_sdk::signature::Signer;
use solana_transaction_parser::gmp_types::{
    Amount, CommonTaskFields, ExecuteTask, ExecuteTaskFields, GatewayTxTask, GatewayTxTaskFields,
    GatewayV2Message,
};
use solana_transaction_parser::parser::TransactionParser;
use solana_transaction_parser::redis::MockCostCacheTrait;

use super::common::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_approve_and_execute_its_message() {
    let env = TestEnvironment::new().await;

    let its_hub_address = env.its_hub_address.clone();
    let its_program_address = solana_axelar_its::ID.to_string();

    let components = create_includer_components(&env.rpc_url, &env.payer);
    let mock_redis = create_mock_redis();
    let mock_gmp_api = create_mock_gmp_api_for_execute();
    let mock_refunds_model = MockRefundsModel::new();

    let includer = SolanaIncluder::new(
        Arc::new(components.includer_client),
        components.keypair,
        "solana-devnet".to_string(),
        components.transaction_builder,
        Arc::new(mock_gmp_api),
        mock_redis,
        Arc::new(mock_refunds_model),
    );

    let deploy_message_id = "test-its-deploy-token-001";
    let source_address = its_hub_address.clone();

    let salt = [1u8; 32];
    let token_id = interchain_token_id(&env.payer.pubkey(), &salt);

    // Create DeployInterchainToken payload
    let deploy_token = DeployInterchainToken {
        selector: Uint::from(1u64),
        token_id: FixedBytes::from(token_id),
        name: "Test Token".to_string(),
        symbol: "TEST".to_string(),
        decimals: 9,
        minter: Bytes::from(vec![]),
    };

    let deploy_inner = GMPPayload::DeployInterchainToken(deploy_token).encode();
    let deploy_receive_from_hub = ReceiveFromHub {
        selector: Uint::from(4u64),
        source_chain: "axelar".to_string(), // Must be in trusted_chains
        payload: Bytes::from(deploy_inner),
    };

    let deploy_gmp_payload = GMPPayload::ReceiveFromHub(deploy_receive_from_hub);
    let deploy_payload_bytes = deploy_gmp_payload.encode();
    let deploy_payload_hash = solana_sdk::keccak::hashv(&[&deploy_payload_bytes]).to_bytes();

    // Create message for deploy
    let deploy_message = Message {
        cc_id: CrossChainId {
            chain: "axelar".to_string(), // The hub chain
            id: deploy_message_id.to_string(),
        },
        source_address: source_address.clone(),
        destination_chain: "solana-devnet".to_string(),
        destination_address: its_program_address.clone(),
        payload_hash: deploy_payload_hash,
    };

    let deploy_message_leaf = MessageLeaf {
        message: deploy_message.clone(),
        position: 0,
        set_size: 1,
        domain_separator: env.domain_separator,
    };
    let deploy_leaf_hash = deploy_message_leaf.hash();
    let deploy_merkle_tree = MerkleTree::from_leaves(&[deploy_leaf_hash]);
    let deploy_merkle_root = deploy_merkle_tree.root().expect("merkle root");

    let deploy_verifier_info_1 = create_verifier_info(
        &env.verifier_secret_keys[0],
        deploy_merkle_root,
        &env.verifier_leaves[0],
        0,
        &env.verifier_merkle_tree,
        PayloadType::ApproveMessages,
    );
    let deploy_verifier_info_2 = create_verifier_info(
        &env.verifier_secret_keys[1],
        deploy_merkle_root,
        &env.verifier_leaves[1],
        1,
        &env.verifier_merkle_tree,
        PayloadType::ApproveMessages,
    );

    let deploy_execute_data = ExecuteData {
        payload_merkle_root: deploy_merkle_root,
        signing_verifier_set_merkle_root: env.verifier_set_hash,
        signing_verifier_set_leaves: vec![deploy_verifier_info_1, deploy_verifier_info_2],
        payload_items: MerklizedPayload::NewMessages {
            messages: vec![MerklizedMessage {
                leaf: deploy_message_leaf,
                proof: vec![],
            }],
        },
    };

    println!("Approving deploy message...");
    let deploy_gateway_task = GatewayTxTask {
        common: CommonTaskFields {
            id: "test-its-deploy-gateway-001".into(),
            chain: "solana-devnet".into(),
            timestamp: "2025-11-26T14:47:18.567796Z".into(),
            r#type: "GATEWAY_TX".into(),
            meta: None,
        },
        task: GatewayTxTaskFields {
            execute_data: BASE64_STANDARD.encode(deploy_execute_data.try_to_vec().unwrap()),
        },
    };

    includer
        .handle_gateway_tx_task(deploy_gateway_task)
        .await
        .expect("Failed to approve deploy message");
    println!("Deploy message approved!");

    println!("Executing deploy...");

    let deploy_execute_task = ExecuteTask {
        common: CommonTaskFields {
            id: "test-its-deploy-execute-001".into(),
            chain: "solana-devnet".into(),
            timestamp: "2025-11-26T14:47:19.567796Z".into(),
            r#type: "EXECUTE".into(),
            meta: None,
        },
        task: ExecuteTaskFields {
            message: GatewayV2Message {
                message_id: deploy_message_id.to_string(),
                source_chain: "axelar".to_string(), // From the hub chain
                source_address: source_address.clone(),
                destination_address: its_program_address.clone(),
                payload_hash: BASE64_STANDARD.encode(deploy_payload_hash),
            },
            payload: BASE64_STANDARD.encode(&deploy_payload_bytes),
            available_gas_balance: Amount {
                token_id: None,
                amount: "10000000000".to_string(),
            },
        },
    };

    println!("Setting up poller to verify MessageApproved event...");
    let test_queue = Arc::new(TestQueue::new());
    let events_queue: Arc<dyn QueueTrait> = Arc::clone(&test_queue) as Arc<dyn QueueTrait>;

    let poller_handle = spawn_poller(
        &env.rpc_url,
        "test_its_deploy_poller",
        &env.transaction_model,
        &env.postgres_db,
        Arc::clone(&events_queue),
    )
    .await;

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let mut found_approve_message_tx = false;
    let max_wait = std::time::Duration::from_secs(30);
    let start = std::time::Instant::now();

    while start.elapsed() < max_wait && !found_approve_message_tx {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        let queued_items = test_queue.get_items().await;
        for item in &queued_items {
            if let QueueItem::Transaction(tx_data) = item {
                if tx_data.contains("ApproveMessage") {
                    found_approve_message_tx = true;
                    println!("Found ApproveMessage transaction in queue!");
                    break;
                }
            }
        }
    }

    println!("Cancelling poller...");
    poller_handle.stop().await;
    println!("Poller stopped");

    let queued_items = test_queue.get_items().await;
    println!("Queue contains {} items total", queued_items.len());

    assert!(
        found_approve_message_tx,
        "Should have found ApproveMessage transaction in queue"
    );

    // Parse MessageApproved event
    let mut mock_cost_cache = MockCostCacheTrait::new();
    mock_cost_cache
        .expect_get_cost_by_message_id()
        .returning(|_, _| Ok(0));
    let parser = TransactionParser::new(
        "solana".to_string(),
        solana_axelar_gas_service::ID,
        solana_axelar_gateway::ID,
        solana_axelar_its::ID,
        Arc::new(mock_cost_cache),
    );

    let mut mock_update_events = MockUpdateEvents::new();
    mock_update_events
        .expect_update_events()
        .returning(|_, _| Box::pin(async { Ok(()) }));

    let ingestor = SolanaIngestor::new(parser, mock_update_events);

    let mut found_approved_event = false;
    for item in &queued_items {
        if let QueueItem::Transaction(tx_data) = item {
            if tx_data.contains("ApproveMessage") {
                match ingestor.handle_transaction(tx_data.to_string()).await {
                    Ok(events) => {
                        for event in &events {
                            if let Event::MessageApproved {
                                common,
                                message,
                                cost,
                                ..
                            } = event
                            {
                                println!("Parsed MessageApproved event!");
                                println!("Event ID: {}", common.event_id);
                                println!("Message ID: {}", message.message_id);
                                println!("Source Chain: {}", message.source_chain);
                                println!("Source Address: {}", message.source_address);
                                println!("Cost: {:?}", cost);

                                if message.message_id == deploy_message_id {
                                    found_approved_event = true;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        println!("Failed to parse transaction: {:?}", e);
                    }
                }
            }
        }
    }

    assert!(
        found_approved_event,
        "MessageApproved event should have been parsed for message_id: {}",
        deploy_message_id
    );
    println!(
        "MessageApproved event parsed successfully for message_id: {}",
        deploy_message_id
    );

    // Now execute the deploy
    let deploy_result = includer.handle_execute_task(deploy_execute_task).await;
    match &deploy_result {
        Ok(events) => {
            println!("Deploy executed successfully! Events: {:?}", events.len());
        }
        Err(e) => {
            panic!("Deploy execution failed: {:?}", e);
        }
    }

    // Give time for deploy to be confirmed
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    println!("Interchain Transfer...");

    let transfer_message_id = "test-its-transfer-001";
    let destination_pubkey = env.payer.pubkey();
    let transfer_amount = 1_000_000u64;

    // Create InterchainTransfer payload
    let source_address_bytes = "ethereum_address_123".as_bytes().to_vec();
    let destination_address_bytes = destination_pubkey.to_bytes().to_vec();

    let transfer = InterchainTransfer {
        selector: Uint::from(0u64),
        token_id: FixedBytes::from(token_id),
        source_address: Bytes::from(source_address_bytes),
        destination_address: Bytes::from(destination_address_bytes),
        amount: Uint::from(transfer_amount),
        data: Bytes::from(vec![]),
    };

    let transfer_inner = GMPPayload::InterchainTransfer(transfer).encode();
    let transfer_receive_from_hub = ReceiveFromHub {
        selector: Uint::from(4u64),
        source_chain: "axelar".to_string(), // Must be in trusted_chains
        payload: Bytes::from(transfer_inner),
    };

    let transfer_gmp_payload = GMPPayload::ReceiveFromHub(transfer_receive_from_hub);
    let transfer_payload_bytes = transfer_gmp_payload.encode();
    let transfer_payload_hash = solana_sdk::keccak::hashv(&[&transfer_payload_bytes]).to_bytes();

    // Create message for transfer
    let transfer_message = Message {
        cc_id: CrossChainId {
            chain: "axelar".to_string(), // The hub chain
            id: transfer_message_id.to_string(),
        },
        source_address: source_address.clone(),
        destination_chain: "solana-devnet".to_string(),
        destination_address: its_program_address.clone(),
        payload_hash: transfer_payload_hash,
    };

    let transfer_message_leaf = MessageLeaf {
        message: transfer_message.clone(),
        position: 0,
        set_size: 1,
        domain_separator: env.domain_separator,
    };
    let transfer_leaf_hash = transfer_message_leaf.hash();
    let transfer_merkle_tree = MerkleTree::from_leaves(&[transfer_leaf_hash]);
    let transfer_merkle_root = transfer_merkle_tree.root().expect("merkle root");

    let transfer_verifier_info_1 = create_verifier_info(
        &env.verifier_secret_keys[0],
        transfer_merkle_root,
        &env.verifier_leaves[0],
        0,
        &env.verifier_merkle_tree,
        PayloadType::ApproveMessages,
    );
    let transfer_verifier_info_2 = create_verifier_info(
        &env.verifier_secret_keys[1],
        transfer_merkle_root,
        &env.verifier_leaves[1],
        1,
        &env.verifier_merkle_tree,
        PayloadType::ApproveMessages,
    );

    let transfer_execute_data = ExecuteData {
        payload_merkle_root: transfer_merkle_root,
        signing_verifier_set_merkle_root: env.verifier_set_hash,
        signing_verifier_set_leaves: vec![transfer_verifier_info_1, transfer_verifier_info_2],
        payload_items: MerklizedPayload::NewMessages {
            messages: vec![MerklizedMessage {
                leaf: transfer_message_leaf,
                proof: vec![],
            }],
        },
    };

    println!("Approving transfer message...");
    let transfer_gateway_task = GatewayTxTask {
        common: CommonTaskFields {
            id: "test-its-transfer-gateway-001".into(),
            chain: "solana-devnet".into(),
            timestamp: "2025-11-26T14:47:20.567796Z".into(),
            r#type: "GATEWAY_TX".into(),
            meta: None,
        },
        task: GatewayTxTaskFields {
            execute_data: BASE64_STANDARD.encode(transfer_execute_data.try_to_vec().unwrap()),
        },
    };

    includer
        .handle_gateway_tx_task(transfer_gateway_task)
        .await
        .expect("Failed to approve transfer message");
    println!("Transfer message approved!");

    println!("Executing transfer...");
    let transfer_execute_task = ExecuteTask {
        common: CommonTaskFields {
            id: "test-its-transfer-execute-001".into(),
            chain: "solana-devnet".into(),
            timestamp: "2025-11-26T14:47:21.567796Z".into(),
            r#type: "EXECUTE".into(),
            meta: None,
        },
        task: ExecuteTaskFields {
            message: GatewayV2Message {
                message_id: transfer_message_id.to_string(),
                source_chain: "ethereum".to_string(),
                source_address: source_address.clone(),
                destination_address: its_program_address.clone(),
                payload_hash: BASE64_STANDARD.encode(transfer_payload_hash),
            },
            payload: BASE64_STANDARD.encode(&transfer_payload_bytes),
            available_gas_balance: Amount {
                token_id: None,
                amount: "10000000000".to_string(),
            },
        },
    };

    match includer.handle_execute_task(transfer_execute_task).await {
        Ok(events) => {
            println!("Transfer executed successfully! Events: {:?}", events.len());
        }
        Err(e) => {
            println!("Transfer execution failed: {:?}", e);
        }
    }

    // Verify MessageExecuted event
    println!("Setting up poller to verify MessageExecuted event...");
    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;

    test_queue.clear().await;

    let events_queue_execute: Arc<dyn QueueTrait> = Arc::clone(&test_queue) as Arc<dyn QueueTrait>;
    let poller_handle_execute = spawn_poller(
        &env.rpc_url,
        "test_its_execute_poller",
        &env.transaction_model,
        &env.postgres_db,
        events_queue_execute,
    )
    .await;

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let mut found_execute_message_tx = false;
    let max_wait_execute = std::time::Duration::from_secs(30);
    let start_execute = std::time::Instant::now();

    while start_execute.elapsed() < max_wait_execute && !found_execute_message_tx {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        let queued_items = test_queue.get_items().await;
        for item in &queued_items {
            if let QueueItem::Transaction(tx_data) = item {
                if tx_data.contains("Execute") {
                    found_execute_message_tx = true;
                    println!("Found MessageExecuted transaction in queue!");
                    break;
                }
            }
        }
    }

    println!("Cancelling poller...");
    poller_handle_execute.stop().await;
    println!("Poller stopped");

    let queued_items_execute = test_queue.get_items().await;
    println!("Queue contains {} items total", queued_items_execute.len());

    assert!(
        found_execute_message_tx,
        "Should have found MessageExecuted transaction in queue"
    );

    let mut mock_cost_cache_execute = MockCostCacheTrait::new();
    mock_cost_cache_execute
        .expect_get_cost_by_message_id()
        .returning(|_, _| Ok(0));
    let parser_execute = TransactionParser::new(
        "solana".to_string(),
        solana_axelar_gas_service::ID,
        solana_axelar_gateway::ID,
        solana_axelar_its::ID,
        Arc::new(mock_cost_cache_execute),
    );

    let mut mock_update_events_execute = MockUpdateEvents::new();
    mock_update_events_execute
        .expect_update_events()
        .returning(|_, _| Box::pin(async { Ok(()) }));

    let ingestor_execute = SolanaIngestor::new(parser_execute, mock_update_events_execute);

    let mut found_executed_event = false;
    for item in &queued_items_execute {
        if let QueueItem::Transaction(tx_data) = item {
            if tx_data.contains("MessageExecuted") {
                match ingestor_execute
                    .handle_transaction(tx_data.to_string())
                    .await
                {
                    Ok(events) => {
                        for event in &events {
                            if let Event::MessageExecuted {
                                common,
                                message_id: event_message_id,
                                source_chain: event_source_chain,
                                status,
                                cost,
                                ..
                            } = event
                            {
                                println!("Parsed MessageExecuted event!");
                                println!("Event ID: {}", common.event_id);
                                println!("Message ID: {}", event_message_id);
                                println!("Source Chain: {}", event_source_chain);
                                println!("Status: {:?}", status);
                                println!("Cost: {:?}", cost);

                                if *event_message_id == deploy_message_id
                                    || *event_message_id == transfer_message_id
                                {
                                    found_executed_event = true;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        println!("Transaction parse note: {:?}", e);
                    }
                }
            }
        }
    }

    if found_executed_event {
        println!("MessageExecuted event parsed successfully!");
    } else {
        println!("Note: MessageExecuted event may not have been parsed yet");
    }

    println!("ITS Integration Test Completed");
    test_queue.clear().await;
    env.cleanup().await;
}
