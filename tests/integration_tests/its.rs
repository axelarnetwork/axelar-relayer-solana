//! ITS (Interchain Token Service) integration tests

use std::sync::Arc;

use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use borsh::BorshSerialize;
use interchain_token_transfer_gmp::alloy_primitives::{Bytes, FixedBytes, Uint};
use interchain_token_transfer_gmp::{
    DeployInterchainToken, GMPPayload, InterchainTransfer, LinkToken, ReceiveFromHub,
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

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    println!("Interchain Transfer...");

    let transfer_message_id = "test-its-transfer-001";
    let destination_pubkey = env.payer.pubkey();
    let transfer_amount = 1_000_000u64;

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
                source_chain: "axelar".to_string(),
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

    test_queue.clear().await;

    println!("Executing transfer...");
    match includer.handle_execute_task(transfer_execute_task).await {
        Ok(events) => {
            println!("Transfer executed successfully! Events: {:?}", events.len());
        }
        Err(e) => {
            panic!("Transfer execution failed: {:?}", e);
        }
    }

    let mut found_transfer_tx = false;
    let max_wait_transfer = std::time::Duration::from_secs(30);
    let start_transfer = std::time::Instant::now();

    while start_transfer.elapsed() < max_wait_transfer && !found_transfer_tx {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        let queued_items = test_queue.get_items().await;
        for item in &queued_items {
            if let QueueItem::Transaction(tx_data) = item {
                if tx_data.contains("InterchainTransfer") {
                    found_transfer_tx = true;
                    println!("Found InterchainTransfer transaction in queue!");
                    break;
                }
            }
        }
    }

    let queued_items_execute = test_queue.get_items().await;
    println!("Queue contains {} items total", queued_items_execute.len());

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
                    if tx_data.contains("itsmM2AJ27dSAXVhCfj34MtnFqyUmnLF7kbKbmyqRQA") {
                        println!("Parse error for ITS transaction: {:?}", e);
                    }
                    panic!("Parse error: {:?}", e);
                }
            }
        }
    }

    if found_executed_event {
        println!("MessageExecuted event parsed successfully!");
    } else {
        panic!("MessageExecuted event not found");
    }

    println!("Link Token Test");

    let link_mint_keypair = solana_sdk::signature::Keypair::new();
    let link_mint_pubkey = link_mint_keypair.pubkey();

    let rent = env
        .rpc_client
        .get_minimum_balance_for_rent_exemption(82)
        .await
        .unwrap();
    let create_mint_ix = solana_sdk::system_instruction::create_account(
        &env.payer.pubkey(),
        &link_mint_pubkey,
        rent,
        82, // Mint account size
        &anchor_spl::token::ID,
    );

    let init_mint_ix = anchor_spl::token::spl_token::instruction::initialize_mint(
        &anchor_spl::token::ID,
        &link_mint_pubkey,
        &env.payer.pubkey(),
        None,
        9,
    )
    .unwrap();

    let recent_blockhash = env.rpc_client.get_latest_blockhash().await.unwrap();
    let create_mint_tx = solana_sdk::transaction::Transaction::new_signed_with_payer(
        &[create_mint_ix, init_mint_ix],
        Some(&env.payer.pubkey()),
        &[&env.payer, &link_mint_keypair],
        recent_blockhash,
    );

    env.rpc_client
        .send_and_confirm_transaction(&create_mint_tx)
        .await
        .expect("Failed to create mint for link token test");

    println!("Created mint for link token: {}", link_mint_pubkey);

    let link_salt = [3u8; 32];
    let link_token_id = interchain_token_id(&env.payer.pubkey(), &link_salt);

    let link_message_id = "test-its-link-token-001";

    let link_token = LinkToken {
        selector: Uint::from(5u64),
        token_id: FixedBytes::from(link_token_id),
        token_manager_type: Uint::from(2u64), // LockUnlock = 2
        source_token_address: Bytes::from(link_mint_pubkey.to_bytes().to_vec()),
        destination_token_address: Bytes::from(link_mint_pubkey.to_bytes().to_vec()),
        link_params: Bytes::from(vec![]), // No operator
    };

    let link_inner = GMPPayload::LinkToken(link_token).encode();
    let link_receive_from_hub = ReceiveFromHub {
        selector: Uint::from(4u64),
        source_chain: "axelar".to_string(),
        payload: Bytes::from(link_inner),
    };

    let link_gmp_payload = GMPPayload::ReceiveFromHub(link_receive_from_hub);
    let link_payload_bytes = link_gmp_payload.encode();
    let link_payload_hash = solana_sdk::keccak::hashv(&[&link_payload_bytes]).to_bytes();

    let link_message = Message {
        cc_id: CrossChainId {
            chain: "axelar".to_string(),
            id: link_message_id.to_string(),
        },
        source_address: source_address.clone(),
        destination_chain: "solana-devnet".to_string(),
        destination_address: its_program_address.clone(),
        payload_hash: link_payload_hash,
    };

    let link_message_leaf = MessageLeaf {
        message: link_message.clone(),
        position: 0,
        set_size: 1,
        domain_separator: env.domain_separator,
    };

    let link_leaf_hash = link_message_leaf.hash();
    let link_merkle_tree = MerkleTree::from_leaves(&[link_leaf_hash]);
    let link_merkle_root = link_merkle_tree.root().expect("merkle root");

    let link_verifier_info_1 = create_verifier_info(
        &env.verifier_secret_keys[0],
        link_merkle_root,
        &env.verifier_leaves[0],
        0,
        &env.verifier_merkle_tree,
    );
    let link_verifier_info_2 = create_verifier_info(
        &env.verifier_secret_keys[1],
        link_merkle_root,
        &env.verifier_leaves[1],
        1,
        &env.verifier_merkle_tree,
    );

    let link_execute_data = ExecuteData {
        payload_merkle_root: link_merkle_root,
        signing_verifier_set_merkle_root: env.verifier_set_hash,
        signing_verifier_set_leaves: vec![link_verifier_info_1, link_verifier_info_2],
        payload_items: MerklizedPayload::NewMessages {
            messages: vec![MerklizedMessage {
                leaf: link_message_leaf,
                proof: vec![],
            }],
        },
    };

    println!("Approving link token message...");
    let link_gateway_task = GatewayTxTask {
        common: CommonTaskFields {
            id: "test-its-link-gateway-001".into(),
            chain: "solana-devnet".into(),
            timestamp: "2025-11-26T14:47:22.567796Z".into(),
            r#type: "GATEWAY_TX".into(),
            meta: None,
        },
        task: GatewayTxTaskFields {
            execute_data: BASE64_STANDARD.encode(link_execute_data.try_to_vec().unwrap()),
        },
    };

    includer
        .handle_gateway_tx_task(link_gateway_task)
        .await
        .expect("Failed to approve link token message");
    println!("Link token message approved!");

    let link_execute_task = ExecuteTask {
        common: CommonTaskFields {
            id: "test-its-link-execute-001".into(),
            chain: "solana-devnet".into(),
            timestamp: "2025-11-26T14:47:23.567796Z".into(),
            r#type: "EXECUTE".into(),
            meta: None,
        },
        task: ExecuteTaskFields {
            message: GatewayV2Message {
                message_id: link_message_id.to_string(),
                source_chain: "axelar".to_string(),
                source_address: source_address.clone(),
                destination_address: its_program_address.clone(),
                payload_hash: BASE64_STANDARD.encode(link_payload_hash),
            },
            payload: BASE64_STANDARD.encode(&link_payload_bytes),
            available_gas_balance: Amount {
                token_id: None,
                amount: "10000000000".to_string(),
            },
        },
    };

    println!("Executing link token...");
    let link_result = includer.handle_execute_task(link_execute_task).await;
    match &link_result {
        Ok(events) => {
            println!("Link token transaction sent! Events: {:?}", events.len());
            for event in events {
                println!("Event: {:?}", event);
            }
        }
        Err(e) => {
            panic!("Link token execution error: {:?}", e);
        }
    }

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    println!("Link Token (SPL Token) test completed");

    println!("Link Token Test (Token-2022)");

    let link_mint_2022_keypair = solana_sdk::signature::Keypair::new();
    let link_mint_2022_pubkey = link_mint_2022_keypair.pubkey();

    let rent_2022 = env
        .rpc_client
        .get_minimum_balance_for_rent_exemption(82)
        .await
        .unwrap();
    let create_mint_2022_ix = solana_sdk::system_instruction::create_account(
        &env.payer.pubkey(),
        &link_mint_2022_pubkey,
        rent_2022,
        82, // Mint account size
        &anchor_spl::token_2022::ID,
    );

    let init_mint_2022_ix = anchor_spl::token_2022::spl_token_2022::instruction::initialize_mint(
        &anchor_spl::token_2022::ID,
        &link_mint_2022_pubkey,
        &env.payer.pubkey(),
        None,
        9,
    )
    .unwrap();

    let recent_blockhash_2022 = env.rpc_client.get_latest_blockhash().await.unwrap();
    let create_mint_2022_tx = solana_sdk::transaction::Transaction::new_signed_with_payer(
        &[create_mint_2022_ix, init_mint_2022_ix],
        Some(&env.payer.pubkey()),
        &[&env.payer, &link_mint_2022_keypair],
        recent_blockhash_2022,
    );

    env.rpc_client
        .send_and_confirm_transaction(&create_mint_2022_tx)
        .await
        .expect("Failed to create Token-2022 mint for link token test");

    println!(
        "Created Token-2022 mint for link token: {}",
        link_mint_2022_pubkey
    );

    let link_salt_2022 = [4u8; 32];
    let link_token_id_2022 = interchain_token_id(&env.payer.pubkey(), &link_salt_2022);

    let link_message_id_2022 = "test-its-link-token-2022-001";

    let link_token_2022 = LinkToken {
        selector: Uint::from(5u64),
        token_id: FixedBytes::from(link_token_id_2022),
        token_manager_type: Uint::from(2u64), // LockUnlock = 2
        source_token_address: Bytes::from(link_mint_2022_pubkey.to_bytes().to_vec()),
        destination_token_address: Bytes::from(link_mint_2022_pubkey.to_bytes().to_vec()),
        link_params: Bytes::from(vec![]), // No operator
    };

    let link_inner_2022 = GMPPayload::LinkToken(link_token_2022).encode();
    let link_receive_from_hub_2022 = ReceiveFromHub {
        selector: Uint::from(4u64),
        source_chain: "axelar".to_string(),
        payload: Bytes::from(link_inner_2022),
    };

    let link_gmp_payload_2022 = GMPPayload::ReceiveFromHub(link_receive_from_hub_2022);
    let link_payload_bytes_2022 = link_gmp_payload_2022.encode();
    let link_payload_hash_2022 = solana_sdk::keccak::hashv(&[&link_payload_bytes_2022]).to_bytes();

    let link_message_2022 = Message {
        cc_id: CrossChainId {
            chain: "axelar".to_string(),
            id: link_message_id_2022.to_string(),
        },
        source_address: source_address.clone(),
        destination_chain: "solana-devnet".to_string(),
        destination_address: its_program_address.clone(),
        payload_hash: link_payload_hash_2022,
    };

    let link_message_leaf_2022 = MessageLeaf {
        message: link_message_2022.clone(),
        position: 0,
        set_size: 1,
        domain_separator: env.domain_separator,
    };

    let link_leaf_hash_2022 = link_message_leaf_2022.hash();
    let link_merkle_tree_2022 = MerkleTree::from_leaves(&[link_leaf_hash_2022]);
    let link_merkle_root_2022 = link_merkle_tree_2022.root().expect("merkle root");

    let link_verifier_info_2022_1 = create_verifier_info(
        &env.verifier_secret_keys[0],
        link_merkle_root_2022,
        &env.verifier_leaves[0],
        0,
        &env.verifier_merkle_tree,
    );
    let link_verifier_info_2022_2 = create_verifier_info(
        &env.verifier_secret_keys[1],
        link_merkle_root_2022,
        &env.verifier_leaves[1],
        1,
        &env.verifier_merkle_tree,
    );

    let link_execute_data_2022 = ExecuteData {
        payload_merkle_root: link_merkle_root_2022,
        signing_verifier_set_merkle_root: env.verifier_set_hash,
        signing_verifier_set_leaves: vec![link_verifier_info_2022_1, link_verifier_info_2022_2],
        payload_items: MerklizedPayload::NewMessages {
            messages: vec![MerklizedMessage {
                leaf: link_message_leaf_2022,
                proof: vec![],
            }],
        },
    };

    println!("Approving link token (Token-2022) message...");
    let link_gateway_task_2022 = GatewayTxTask {
        common: CommonTaskFields {
            id: "test-its-link-gateway-2022-001".into(),
            chain: "solana-devnet".into(),
            timestamp: "2025-11-26T14:47:24.567796Z".into(),
            r#type: "GATEWAY_TX".into(),
            meta: None,
        },
        task: GatewayTxTaskFields {
            execute_data: BASE64_STANDARD.encode(link_execute_data_2022.try_to_vec().unwrap()),
        },
    };

    includer
        .handle_gateway_tx_task(link_gateway_task_2022)
        .await
        .expect("Failed to approve link token (Token-2022) message");
    println!("Link token (Token-2022) message approved!");

    let link_execute_task_2022 = ExecuteTask {
        common: CommonTaskFields {
            id: "test-its-link-execute-2022-001".into(),
            chain: "solana-devnet".into(),
            timestamp: "2025-11-26T14:47:25.567796Z".into(),
            r#type: "EXECUTE".into(),
            meta: None,
        },
        task: ExecuteTaskFields {
            message: GatewayV2Message {
                message_id: link_message_id_2022.to_string(),
                source_chain: "axelar".to_string(),
                source_address: source_address.clone(),
                destination_address: its_program_address.clone(),
                payload_hash: BASE64_STANDARD.encode(link_payload_hash_2022),
            },
            payload: BASE64_STANDARD.encode(&link_payload_bytes_2022),
            available_gas_balance: Amount {
                token_id: None,
                amount: "10000000000".to_string(),
            },
        },
    };

    println!("Executing link token (Token-2022)...");
    let link_result_2022 = includer.handle_execute_task(link_execute_task_2022).await;
    match &link_result_2022 {
        Ok(events) => {
            println!(
                "Link token (Token-2022) transaction sent! Events: {:?}",
                events.len()
            );
            for event in events {
                println!("Event: {:?}", event);
            }
        }
        Err(e) => {
            panic!("Link token (Token-2022) execution error: {:?}", e);
        }
    }

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    println!("Link Token (Token-2022) test completed");

    println!("ITS Integration Test Completed");

    println!("Stopping poller...");
    poller_handle.stop().await;
    println!("Poller stopped");

    test_queue.clear().await;
    env.cleanup().await;
}

// DeployInterchainToken with minter
// LinkToken with operator
// Interchain transfer with execute data
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_its_messages_with_optional_fields() {
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

    println!("DeployInterchainToken with Minter");

    let deploy_message_id = "test-its-deploy-with-minter-001";
    let source_address = its_hub_address.clone();

    let salt = [10u8; 32];
    let token_id = interchain_token_id(&env.payer.pubkey(), &salt);

    let minter_pubkey = env.payer.pubkey();
    let minter_bytes = minter_pubkey.to_bytes().to_vec();

    let deploy_token = DeployInterchainToken {
        selector: Uint::from(1u64),
        token_id: FixedBytes::from(token_id),
        name: "Minted Token".to_string(),
        symbol: "MINT".to_string(),
        decimals: 6,
        minter: Bytes::from(minter_bytes),
    };

    let deploy_inner = GMPPayload::DeployInterchainToken(deploy_token).encode();
    let deploy_receive_from_hub = ReceiveFromHub {
        selector: Uint::from(4u64),
        source_chain: "axelar".to_string(),
        payload: Bytes::from(deploy_inner),
    };

    let deploy_gmp_payload = GMPPayload::ReceiveFromHub(deploy_receive_from_hub);
    let deploy_payload_bytes = deploy_gmp_payload.encode();
    let deploy_payload_hash = solana_sdk::keccak::hashv(&[&deploy_payload_bytes]).to_bytes();

    let deploy_message = Message {
        cc_id: CrossChainId {
            chain: "axelar".to_string(),
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
    );
    let deploy_verifier_info_2 = create_verifier_info(
        &env.verifier_secret_keys[1],
        deploy_merkle_root,
        &env.verifier_leaves[1],
        1,
        &env.verifier_merkle_tree,
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

    println!("Approving deploy message with minter...");
    let deploy_gateway_task = GatewayTxTask {
        common: CommonTaskFields {
            id: "test-its-deploy-minter-gateway-001".into(),
            chain: "solana-devnet".into(),
            timestamp: "2025-12-05T10:00:00.000000Z".into(),
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
        .expect("Failed to approve deploy message with minter");
    println!("Deploy message with minter approved!");

    let deploy_execute_task = ExecuteTask {
        common: CommonTaskFields {
            id: "test-its-deploy-minter-execute-001".into(),
            chain: "solana-devnet".into(),
            timestamp: "2025-12-05T10:00:01.000000Z".into(),
            r#type: "EXECUTE".into(),
            meta: None,
        },
        task: ExecuteTaskFields {
            message: GatewayV2Message {
                message_id: deploy_message_id.to_string(),
                source_chain: "axelar".to_string(),
                source_address: source_address.clone(),
                destination_address: its_program_address.clone(),
                payload_hash: BASE64_STANDARD.encode(deploy_payload_hash),
            },
            payload: BASE64_STANDARD.encode(&deploy_payload_bytes),
            available_gas_balance: Amount {
                token_id: None,
                amount: "15000000000".to_string(),
            },
        },
    };

    println!("Executing deploy with minter...");
    match includer.handle_execute_task(deploy_execute_task).await {
        Ok(events) => {
            println!(
                "Deploy with minter executed successfully! Events: {:?}",
                events.len()
            );
        }
        Err(e) => {
            panic!("Deploy with minter execution failed: {:?}", e);
        }
    }

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    println!("DeployInterchainToken with minter test completed!");

    println!("LinkToken with Operator");

    let link_mint_keypair = solana_sdk::signature::Keypair::new();
    let link_mint_pubkey = link_mint_keypair.pubkey();

    // Create SPL Token
    let rent = env
        .rpc_client
        .get_minimum_balance_for_rent_exemption(82)
        .await
        .unwrap();
    let create_mint_ix = solana_sdk::system_instruction::create_account(
        &env.payer.pubkey(),
        &link_mint_pubkey,
        rent,
        82,
        &anchor_spl::token::ID,
    );

    let init_mint_ix = anchor_spl::token::spl_token::instruction::initialize_mint(
        &anchor_spl::token::ID,
        &link_mint_pubkey,
        &env.payer.pubkey(),
        None,
        8,
    )
    .unwrap();

    let recent_blockhash = env.rpc_client.get_latest_blockhash().await.unwrap();
    let create_mint_tx = solana_sdk::transaction::Transaction::new_signed_with_payer(
        &[create_mint_ix, init_mint_ix],
        Some(&env.payer.pubkey()),
        &[&env.payer, &link_mint_keypair],
        recent_blockhash,
    );

    env.rpc_client
        .send_and_confirm_transaction(&create_mint_tx)
        .await
        .expect("Failed to create mint for link token with operator test");

    println!("Created mint for link token: {}", link_mint_pubkey);

    let link_salt = [20u8; 32];
    let link_token_id = interchain_token_id(&env.payer.pubkey(), &link_salt);

    let operator_pubkey = env.payer.pubkey();
    let operator_bytes = operator_pubkey.to_bytes().to_vec();

    let link = LinkToken {
        selector: Uint::from(5u64),
        token_id: FixedBytes::from(link_token_id),
        token_manager_type: Uint::from(2u64), // LockUnlock = 2
        source_token_address: Bytes::from(vec![0xAB; 20]), // Different source address
        destination_token_address: Bytes::from(link_mint_pubkey.to_bytes().to_vec()),
        link_params: Bytes::from(operator_bytes), // Non-empty operator!
    };

    let link_inner = GMPPayload::LinkToken(link).encode();
    let link_receive_from_hub = ReceiveFromHub {
        selector: Uint::from(4u64),
        source_chain: "axelar".to_string(),
        payload: Bytes::from(link_inner),
    };

    let link_gmp_payload = GMPPayload::ReceiveFromHub(link_receive_from_hub);
    let link_payload_bytes = link_gmp_payload.encode();
    let link_payload_hash = solana_sdk::keccak::hashv(&[&link_payload_bytes]).to_bytes();

    let link_message_id = "test-its-link-with-operator-001";
    let link_message = Message {
        cc_id: CrossChainId {
            chain: "axelar".to_string(),
            id: link_message_id.to_string(),
        },
        source_address: source_address.clone(),
        destination_chain: "solana-devnet".to_string(),
        destination_address: its_program_address.clone(),
        payload_hash: link_payload_hash,
    };

    let link_message_leaf = MessageLeaf {
        message: link_message.clone(),
        position: 0,
        set_size: 1,
        domain_separator: env.domain_separator,
    };
    let link_leaf_hash = link_message_leaf.hash();
    let link_merkle_tree = MerkleTree::from_leaves(&[link_leaf_hash]);
    let link_merkle_root = link_merkle_tree.root().expect("merkle root");

    let link_verifier_info_1 = create_verifier_info(
        &env.verifier_secret_keys[0],
        link_merkle_root,
        &env.verifier_leaves[0],
        0,
        &env.verifier_merkle_tree,
    );
    let link_verifier_info_2 = create_verifier_info(
        &env.verifier_secret_keys[1],
        link_merkle_root,
        &env.verifier_leaves[1],
        1,
        &env.verifier_merkle_tree,
    );

    let link_execute_data = ExecuteData {
        payload_merkle_root: link_merkle_root,
        signing_verifier_set_merkle_root: env.verifier_set_hash,
        signing_verifier_set_leaves: vec![link_verifier_info_1, link_verifier_info_2],
        payload_items: MerklizedPayload::NewMessages {
            messages: vec![MerklizedMessage {
                leaf: link_message_leaf,
                proof: vec![],
            }],
        },
    };

    println!("Approving link token message with operator...");
    let link_gateway_task = GatewayTxTask {
        common: CommonTaskFields {
            id: "test-its-link-operator-gateway-001".into(),
            chain: "solana-devnet".into(),
            timestamp: "2025-12-05T10:01:00.000000Z".into(),
            r#type: "GATEWAY_TX".into(),
            meta: None,
        },
        task: GatewayTxTaskFields {
            execute_data: BASE64_STANDARD.encode(link_execute_data.try_to_vec().unwrap()),
        },
    };

    includer
        .handle_gateway_tx_task(link_gateway_task)
        .await
        .expect("Failed to approve link token message with operator");
    println!("Link token message with operator approved!");

    let link_execute_task = ExecuteTask {
        common: CommonTaskFields {
            id: "test-its-link-operator-execute-001".into(),
            chain: "solana-devnet".into(),
            timestamp: "2025-12-05T10:01:01.000000Z".into(),
            r#type: "EXECUTE".into(),
            meta: None,
        },
        task: ExecuteTaskFields {
            message: GatewayV2Message {
                message_id: link_message_id.to_string(),
                source_chain: "axelar".to_string(),
                source_address: source_address.clone(),
                destination_address: its_program_address.clone(),
                payload_hash: BASE64_STANDARD.encode(link_payload_hash),
            },
            payload: BASE64_STANDARD.encode(&link_payload_bytes),
            available_gas_balance: Amount {
                token_id: None,
                amount: "12000000000".to_string(),
            },
        },
    };

    println!("Executing link token with operator...");
    let link_result = includer.handle_execute_task(link_execute_task).await;
    match &link_result {
        Ok(events) => {
            println!(
                "Link token with operator executed! Events: {:?}",
                events.len()
            );
            for event in events {
                println!("Event: {:?}", event);
            }
        }
        Err(e) => {
            panic!("Link token with operator execution error: {:?}", e);
        }
    }

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    println!("LinkToken with operator test completed!");

    println!("InterchainTransfer with Executable Data (Memo Program)");

    println!("Initializing memo counter PDA...");
    let (counter_pda, _counter_bump) =
        solana_sdk::pubkey::Pubkey::find_program_address(&[b"counter"], &env.memo_program_id);

    use anchor_lang::InstructionData as AnchorInstructionData;
    use anchor_lang::ToAccountMetas;

    let init_ix_data = solana_axelar_memo::instruction::Init {}.data();
    let init_accounts = solana_axelar_memo::accounts::Init {
        counter: counter_pda,
        payer: env.payer.pubkey(),
        system_program: solana_sdk::system_program::ID,
    }
    .to_account_metas(None);

    let init_instruction = solana_sdk::instruction::Instruction {
        program_id: env.memo_program_id,
        accounts: init_accounts,
        data: init_ix_data,
    };

    let init_blockhash = env.rpc_client.get_latest_blockhash().await.unwrap();
    let init_tx = solana_sdk::transaction::Transaction::new_signed_with_payer(
        &[init_instruction],
        Some(&env.payer.pubkey()),
        &[&env.payer],
        init_blockhash,
    );

    env.rpc_client
        .send_and_confirm_transaction(&init_tx)
        .await
        .expect("Failed to initialize memo counter PDA");
    println!("Memo counter PDA initialized!");

    println!("Minting tokens to token manager for transfer...");
    let (its_root_pda, _) = solana_axelar_its::InterchainTokenService::find_pda();
    let (token_manager_pda, _) = solana_axelar_its::TokenManager::find_pda(token_id, its_root_pda);
    let (token_mint_pda, _) =
        solana_axelar_its::TokenManager::find_token_mint(token_id, its_root_pda);

    let token_manager_ata =
        anchor_spl::associated_token::get_associated_token_address_with_program_id(
            &token_manager_pda,
            &token_mint_pda,
            &anchor_spl::token_2022::ID,
        );

    let (minter_roles_pda, _) =
        solana_axelar_its::UserRoles::find_pda(&token_manager_pda, &minter_pubkey);

    let mint_amount = 10_000_000u64;
    let mint_ix_data = solana_axelar_its::instruction::MintInterchainToken {
        amount: mint_amount,
    }
    .data();
    let mint_accounts = solana_axelar_its::accounts::MintInterchainToken {
        mint: token_mint_pda,
        destination_account: token_manager_ata,
        its_root_pda,
        token_manager_pda,
        minter: minter_pubkey,
        minter_roles_pda,
        token_program: anchor_spl::token_2022::ID,
    }
    .to_account_metas(None);

    let mint_ix = solana_sdk::instruction::Instruction {
        program_id: solana_axelar_its::ID,
        accounts: mint_accounts,
        data: mint_ix_data,
    };

    let mint_blockhash = env.rpc_client.get_latest_blockhash().await.unwrap();
    let mint_tx = solana_sdk::transaction::Transaction::new_signed_with_payer(
        &[mint_ix],
        Some(&env.payer.pubkey()),
        &[&env.payer],
        mint_blockhash,
    );

    env.rpc_client
        .send_and_confirm_transaction(&mint_tx)
        .await
        .expect("Failed to mint tokens to token manager");
    println!("Minted {} tokens to token manager", mint_amount);

    let transfer_message_id = "test-its-transfer-with-data-001";
    let transfer_amount = 500_000u64;

    let memo_program_pubkey = env.memo_program_id;

    use solana_axelar_gateway::executable::{ExecutablePayload, ExecutablePayloadEncodingScheme};

    let memo_string = "Hello from ITS transfer!";
    let encoding_scheme = ExecutablePayloadEncodingScheme::Borsh;

    let executable_payload = ExecutablePayload::new(
        memo_string.as_bytes(),
        &[solana_sdk::instruction::AccountMeta::new(
            counter_pda,
            false,
        )],
        encoding_scheme,
    );

    let executable_data = executable_payload
        .encode()
        .expect("Failed to encode executable payload");

    let source_address_bytes = "eth_sender_with_data".as_bytes().to_vec();
    let destination_address_bytes = memo_program_pubkey.to_bytes().to_vec();

    let transfer = InterchainTransfer {
        selector: Uint::from(0u64),
        token_id: FixedBytes::from(token_id),
        source_address: Bytes::from(source_address_bytes),
        destination_address: Bytes::from(destination_address_bytes), // Memo program
        amount: Uint::from(transfer_amount),
        data: Bytes::from(executable_data),
    };

    let transfer_inner = GMPPayload::InterchainTransfer(transfer).encode();
    let transfer_receive_from_hub = ReceiveFromHub {
        selector: Uint::from(4u64),
        source_chain: "axelar".to_string(),
        payload: Bytes::from(transfer_inner),
    };

    let transfer_gmp_payload = GMPPayload::ReceiveFromHub(transfer_receive_from_hub);
    let transfer_payload_bytes = transfer_gmp_payload.encode();
    let transfer_payload_hash = solana_sdk::keccak::hashv(&[&transfer_payload_bytes]).to_bytes();

    let transfer_message = Message {
        cc_id: CrossChainId {
            chain: "axelar".to_string(),
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
    );
    let transfer_verifier_info_2 = create_verifier_info(
        &env.verifier_secret_keys[1],
        transfer_merkle_root,
        &env.verifier_leaves[1],
        1,
        &env.verifier_merkle_tree,
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

    println!("Approving transfer message with executable data...");
    let transfer_gateway_task = GatewayTxTask {
        common: CommonTaskFields {
            id: "test-its-transfer-data-gateway-001".into(),
            chain: "solana-devnet".into(),
            timestamp: "2025-12-05T10:02:00.000000Z".into(),
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
        .expect("Failed to approve transfer message with data");
    println!("Transfer message with data approved!");

    let transfer_execute_task = ExecuteTask {
        common: CommonTaskFields {
            id: "test-its-transfer-data-execute-001".into(),
            chain: "solana-devnet".into(),
            timestamp: "2025-12-05T10:02:01.000000Z".into(),
            r#type: "EXECUTE".into(),
            meta: None,
        },
        task: ExecuteTaskFields {
            message: GatewayV2Message {
                message_id: transfer_message_id.to_string(),
                source_chain: "axelar".to_string(),
                source_address: source_address.clone(),
                destination_address: its_program_address.clone(),
                payload_hash: BASE64_STANDARD.encode(transfer_payload_hash),
            },
            payload: BASE64_STANDARD.encode(&transfer_payload_bytes),
            available_gas_balance: Amount {
                token_id: None,
                amount: "20000000000".to_string(),
            },
        },
    };

    println!("Executing transfer with executable data (calls memo program)...");
    match includer.handle_execute_task(transfer_execute_task).await {
        Ok(events) => {
            println!(
                "Transfer with data executed successfully! Events: {:?}",
                events.len()
            );
            for event in events {
                println!("Event: {:?}", event);
            }
        }
        Err(e) => {
            panic!("Transfer with data execution error: {:?}", e);
        }
    }

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    println!("InterchainTransfer with executable data (memo callback) test completed!");

    println!("ITS Messages with Optional Fields Test Completed");
    env.cleanup().await;
}

// Test concurrent ITS task processing to verify ALT and transaction handling under load
#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
async fn test_its_concurrent_task_processing() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    const NUM_CONCURRENT_TASKS: usize = 10;

    let env = TestEnvironment::new().await;

    let its_hub_address = env.its_hub_address.clone();
    let its_program_address = solana_axelar_its::ID.to_string();

    #[allow(clippy::too_many_arguments)]
    fn create_transfer_task(
        index: usize,
        token_id: [u8; 32],
        destination_pubkey: solana_sdk::pubkey::Pubkey,
        source_address: &str,
        its_program_address: &str,
        domain_separator: [u8; 32],
        verifier_secret_keys: &[libsecp256k1::SecretKey],
        verifier_leaves: &[solana_axelar_std::VerifierSetLeaf],
        verifier_merkle_tree: &MerkleTree,
        verifier_set_hash: [u8; 32],
    ) -> (GatewayTxTask, ExecuteTask) {
        let transfer_message_id = format!("test-concurrent-transfer-{:03}", index);
        let transfer_amount = 1_000u64 + (index as u64 * 100); // to simulate different transactions

        let source_address_bytes = format!("eth_sender_{}", index).as_bytes().to_vec();
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
            source_chain: "axelar".to_string(),
            payload: Bytes::from(transfer_inner),
        };

        let transfer_gmp_payload = GMPPayload::ReceiveFromHub(transfer_receive_from_hub);
        let transfer_payload_bytes = transfer_gmp_payload.encode();
        let transfer_payload_hash =
            solana_sdk::keccak::hashv(&[&transfer_payload_bytes]).to_bytes();

        let transfer_message = Message {
            cc_id: CrossChainId {
                chain: "axelar".to_string(),
                id: transfer_message_id.clone(),
            },
            source_address: source_address.to_string(),
            destination_chain: "solana-devnet".to_string(),
            destination_address: its_program_address.to_string(),
            payload_hash: transfer_payload_hash,
        };

        let transfer_message_leaf = MessageLeaf {
            message: transfer_message,
            position: 0,
            set_size: 1,
            domain_separator,
        };
        let transfer_leaf_hash = transfer_message_leaf.hash();
        let transfer_merkle_tree = MerkleTree::from_leaves(&[transfer_leaf_hash]);
        let transfer_merkle_root = transfer_merkle_tree.root().expect("merkle root");

        let transfer_verifier_info_1 = create_verifier_info(
            &verifier_secret_keys[0],
            transfer_merkle_root,
            &verifier_leaves[0],
            0,
            verifier_merkle_tree,
        );
        let transfer_verifier_info_2 = create_verifier_info(
            &verifier_secret_keys[1],
            transfer_merkle_root,
            &verifier_leaves[1],
            1,
            verifier_merkle_tree,
        );

        let transfer_execute_data = ExecuteData {
            payload_merkle_root: transfer_merkle_root,
            signing_verifier_set_merkle_root: verifier_set_hash,
            signing_verifier_set_leaves: vec![transfer_verifier_info_1, transfer_verifier_info_2],
            payload_items: MerklizedPayload::NewMessages {
                messages: vec![MerklizedMessage {
                    leaf: transfer_message_leaf,
                    proof: vec![],
                }],
            },
        };

        let gateway_task = GatewayTxTask {
            common: CommonTaskFields {
                id: format!("test-concurrent-gateway-{:03}", index),
                chain: "solana-devnet".into(),
                timestamp: format!("2025-12-05T11:{:02}:00.000000Z", index % 60),
                r#type: "GATEWAY_TX".into(),
                meta: None,
            },
            task: GatewayTxTaskFields {
                execute_data: BASE64_STANDARD.encode(transfer_execute_data.try_to_vec().unwrap()),
            },
        };

        let execute_task = ExecuteTask {
            common: CommonTaskFields {
                id: format!("test-concurrent-execute-{:03}", index),
                chain: "solana-devnet".into(),
                timestamp: format!("2025-12-05T11:{:02}:01.000000Z", index % 60),
                r#type: "EXECUTE".into(),
                meta: None,
            },
            task: ExecuteTaskFields {
                message: GatewayV2Message {
                    message_id: transfer_message_id,
                    source_chain: "axelar".to_string(),
                    source_address: source_address.to_string(),
                    destination_address: its_program_address.to_string(),
                    payload_hash: BASE64_STANDARD.encode(transfer_payload_hash),
                },
                payload: BASE64_STANDARD.encode(&transfer_payload_bytes),
                available_gas_balance: Amount {
                    token_id: None,
                    amount: "10000000000".to_string(),
                },
            },
        };

        (gateway_task, execute_task)
    }

    println!("Concurrent ITS Task Processing Test");
    println!("Testing {} concurrent tasks", NUM_CONCURRENT_TASKS);

    println!("Deploying token for concurrent transfers...");

    let salt = [99u8; 32];
    let token_id = interchain_token_id(&env.payer.pubkey(), &salt);
    let source_address = its_hub_address.clone();

    let deploy_token = DeployInterchainToken {
        selector: Uint::from(1u64),
        token_id: FixedBytes::from(token_id),
        name: "Concurrent Test Token".to_string(),
        symbol: "CONC".to_string(),
        decimals: 9,
        minter: Bytes::from(env.payer.pubkey().to_bytes().to_vec()),
    };

    let deploy_inner = GMPPayload::DeployInterchainToken(deploy_token).encode();
    let deploy_receive_from_hub = ReceiveFromHub {
        selector: Uint::from(4u64),
        source_chain: "axelar".to_string(),
        payload: Bytes::from(deploy_inner),
    };

    let deploy_gmp_payload = GMPPayload::ReceiveFromHub(deploy_receive_from_hub);
    let deploy_payload_bytes = deploy_gmp_payload.encode();
    let deploy_payload_hash = solana_sdk::keccak::hashv(&[&deploy_payload_bytes]).to_bytes();

    let deploy_message = Message {
        cc_id: CrossChainId {
            chain: "axelar".to_string(),
            id: "test-concurrent-deploy-001".to_string(),
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
    );
    let deploy_verifier_info_2 = create_verifier_info(
        &env.verifier_secret_keys[1],
        deploy_merkle_root,
        &env.verifier_leaves[1],
        1,
        &env.verifier_merkle_tree,
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

    let components = create_includer_components(&env.rpc_url, &env.payer);
    let mock_redis = create_mock_redis();
    let mock_gmp_api = create_mock_gmp_api_for_execute();
    let mock_refunds_model = MockRefundsModel::new();

    let includer = Arc::new(SolanaIncluder::new(
        Arc::new(components.includer_client),
        components.keypair,
        "solana-devnet".to_string(),
        components.transaction_builder,
        Arc::new(mock_gmp_api),
        mock_redis,
        Arc::new(mock_refunds_model),
    ));

    let deploy_gateway_task = GatewayTxTask {
        common: CommonTaskFields {
            id: "test-concurrent-deploy-gateway".into(),
            chain: "solana-devnet".into(),
            timestamp: "2025-12-05T11:00:00.000000Z".into(),
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
        .expect("Failed to approve deploy");

    let deploy_execute_task = ExecuteTask {
        common: CommonTaskFields {
            id: "test-concurrent-deploy-execute".into(),
            chain: "solana-devnet".into(),
            timestamp: "2025-12-05T11:00:01.000000Z".into(),
            r#type: "EXECUTE".into(),
            meta: None,
        },
        task: ExecuteTaskFields {
            message: GatewayV2Message {
                message_id: "test-concurrent-deploy-001".to_string(),
                source_chain: "axelar".to_string(),
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

    includer
        .handle_execute_task(deploy_execute_task)
        .await
        .expect("Failed to execute deploy");

    println!("Token deployed successfully!");
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    println!("Generating {} transfer tasks...", NUM_CONCURRENT_TASKS);
    let destination_pubkey = env.payer.pubkey();

    let tasks: Vec<(GatewayTxTask, ExecuteTask)> = (0..NUM_CONCURRENT_TASKS)
        .map(|i| {
            create_transfer_task(
                i,
                token_id,
                destination_pubkey,
                &source_address,
                &its_program_address,
                env.domain_separator,
                &env.verifier_secret_keys,
                &env.verifier_leaves,
                &env.verifier_merkle_tree,
                env.verifier_set_hash,
            )
        })
        .collect();

    println!("Approving all {} messages...", NUM_CONCURRENT_TASKS);
    for (i, (gateway_task, _)) in tasks.iter().enumerate() {
        includer
            .handle_gateway_tx_task(gateway_task.clone())
            .await
            .unwrap_or_else(|e| panic!("Failed to approve message {}: {:?}", i, e));
    }
    println!("All messages approved!");

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    println!(
        "Executing {} transfers in parallel...",
        NUM_CONCURRENT_TASKS
    );

    let success_count = Arc::new(AtomicUsize::new(0));
    let failure_count = Arc::new(AtomicUsize::new(0));

    let mut handles = vec![];

    for (i, (_, execute_task)) in tasks.into_iter().enumerate() {
        let includer = Arc::clone(&includer);
        let success_count = Arc::clone(&success_count);
        let failure_count = Arc::clone(&failure_count);

        let handle = tokio::spawn(async move {
            match includer.handle_execute_task(execute_task).await {
                Ok(_) => {
                    success_count.fetch_add(1, Ordering::SeqCst);
                    println!("Task {} completed successfully", i);
                }
                Err(e) => {
                    failure_count.fetch_add(1, Ordering::SeqCst);
                    println!("Task {} failed: {:?}", i, e);
                }
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.await.expect("Task panicked");
    }

    let successes = success_count.load(Ordering::SeqCst);
    let failures = failure_count.load(Ordering::SeqCst);

    println!("Total tasks: {}", NUM_CONCURRENT_TASKS);
    println!("Successes: {}", successes);
    println!("Failures: {}", failures);

    assert_eq!(
        successes, NUM_CONCURRENT_TASKS,
        "Expected all {} tasks to succeed, but {} failed",
        NUM_CONCURRENT_TASKS, failures
    );

    println!("Concurrent ITS task processing test completed successfully!");
    env.cleanup().await;
}
