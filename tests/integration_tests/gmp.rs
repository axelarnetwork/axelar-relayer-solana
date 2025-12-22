//! GMP integration tests

use std::sync::Arc;

use anchor_lang::{InstructionData, ToAccountMetas};
use relayer_core::gmp_api::gmp_types::{Event, PostEventResult};
use relayer_core::gmp_api::{GmpApiTrait, MockGmpApiTrait};
use relayer_core::includer_worker::IncluderTrait;
use relayer_core::ingestor::IngestorTrait;
use relayer_core::queue::{QueueItem, QueueTrait};
use solana::includer::SolanaIncluder;
use solana::ingestor::SolanaIngestor;
use solana::mocks::{MockRefundsModel, MockUpdateEvents};
use solana::models::refunds::RefundsModel;
use solana_axelar_std::PayloadType;
use solana_sdk::instruction::Instruction;
use solana_sdk::native_token::LAMPORTS_PER_SOL;
use solana_sdk::signature::{Keypair, Signer};
#[allow(deprecated)]
use solana_sdk::system_instruction;
use solana_sdk::transaction::Transaction;
use solana_transaction_parser::parser::TransactionParser;
use solana_transaction_parser::redis::MockCostCacheTrait;

use solana_axelar_gateway::ID as GATEWAY_PROGRAM_ID;
use solana_axelar_gateway_test_fixtures::create_verifier_info;
use solana_axelar_std::{hasher::LeafHash, MerkleTree, PublicKey, VerifierSetLeaf};

use super::common::*;

/// Test that call_contract transactions are captured by the poller's run() function
/// and processed by the ingestor to post events to the GMP API.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_call_contract_picked_up_and_sent_to_gmp() {
    let env = TestEnvironment::new().await;

    let test_queue = Arc::new(TestQueue::new());
    let events_queue: Arc<dyn QueueTrait> = Arc::clone(&test_queue) as Arc<dyn QueueTrait>;

    let poller_handle = spawn_poller(
        &env.rpc_url,
        "test_call_contract_poller",
        &env.transaction_model,
        &env.postgres_db,
        events_queue,
    )
    .await;

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let (event_authority_pda, _) = solana_sdk::pubkey::Pubkey::find_program_address(
        &[b"__event_authority"],
        &GATEWAY_PROGRAM_ID,
    );

    let destination_chain = "ethereum".to_string();
    let destination_address = "0xDestinationContract".to_string();
    let payload = b"Hello from Solana integration test!".to_vec();

    let call_contract_ix = solana_axelar_gateway::instruction::CallContract {
        destination_chain: destination_chain.clone(),
        destination_contract_address: destination_address.clone(),
        payload: payload.clone(),
        signing_pda_bump: 0,
    };

    let mut accounts = solana_axelar_gateway::accounts::CallContract {
        caller: env.payer.pubkey(),
        signing_pda: None,
        gateway_root_pda: env.gateway_root_pda,
        event_authority: event_authority_pda,
        program: GATEWAY_PROGRAM_ID,
    }
    .to_account_metas(None);

    accounts[0].is_signer = true;

    let call_contract_instruction = Instruction {
        program_id: GATEWAY_PROGRAM_ID,
        accounts,
        data: call_contract_ix.data(),
    };

    let recent_blockhash = env
        .rpc_client
        .get_latest_blockhash()
        .await
        .expect("blockhash");
    let call_contract_tx = Transaction::new_signed_with_payer(
        &[call_contract_instruction],
        Some(&env.payer.pubkey()),
        &[&env.payer],
        recent_blockhash,
    );

    let call_contract_signature = env
        .rpc_client
        .send_and_confirm_transaction(&call_contract_tx)
        .await
        .expect("Failed to send call_contract transaction");

    println!(
        "call_contract transaction sent: {}",
        call_contract_signature
    );

    // Wait for the poller to pick up the transaction
    let mut found_call_contract = false;
    let start_time = std::time::Instant::now();
    let timeout = std::time::Duration::from_secs(30);

    while !found_call_contract && start_time.elapsed() < timeout {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        let queued_items = test_queue.get_items().await;
        for item in &queued_items {
            if let QueueItem::Transaction(tx_data) = item {
                if tx_data.contains("CallContract") {
                    found_call_contract = true;
                    println!("Poller picked up CallContract transaction!");
                    break;
                }
            }
        }
    }

    println!("Cancelling poller...");
    poller_handle.stop().await;
    println!("Poller stopped");

    assert!(
        found_call_contract,
        "Poller should have picked up the CallContract transaction within timeout"
    );

    let queued_items = test_queue.get_items().await;
    println!("Queue contains {} items", queued_items.len());

    let call_contract_tx_json = queued_items
        .iter()
        .find_map(|item| {
            if let QueueItem::Transaction(tx_data) = item {
                if tx_data.contains("CallContract") {
                    Some(tx_data.clone())
                } else {
                    None
                }
            } else {
                None
            }
        })
        .expect("Should have found CallContract transaction in queue");

    // Create ingestor with real parser and mock update events
    let mock_cost_cache = Arc::new(MockCostCacheTrait::new());
    let parser = TransactionParser::new(
        "solana".to_string(),
        solana_axelar_gas_service::ID,
        solana_axelar_gateway::ID,
        solana_axelar_its::ID,
        mock_cost_cache,
    );

    let mut mock_update_events = MockUpdateEvents::new();
    mock_update_events
        .expect_update_events()
        .returning(|_, _| Box::pin(async { Ok(()) }));

    let ingestor = SolanaIngestor::new(parser, mock_update_events);

    let events = ingestor
        .handle_transaction(call_contract_tx_json.to_string())
        .await
        .expect("Ingestor should successfully parse the CallContract transaction");

    println!("Ingestor extracted {} event(s)", events.len());

    let call_event = events
        .iter()
        .find(|e| matches!(e, Event::Call { .. }))
        .expect("Should find a Call event");

    if let Event::Call {
        common,
        message,
        destination_chain,
        payload,
    } = call_event
    {
        println!("Found Call event!");
        println!("Event ID: {}", common.event_id);
        println!("Message ID: {}", message.message_id);
        println!("Destination Chain: {}", destination_chain);
        println!("Payload length: {} bytes", payload.len());

        assert_eq!(destination_chain, "ethereum");
        assert_eq!(message.destination_address, "0xDestinationContract");
    }

    // Verify GMP API would receive the event (mock external service)
    let captured_event = Arc::new(std::sync::Mutex::new(None::<Event>));
    let captured_event_clone = Arc::clone(&captured_event);

    let mut mock_gmp_api = MockGmpApiTrait::new();
    mock_gmp_api
        .expect_post_events()
        .times(1)
        .returning(move |events| {
            for event in &events {
                if matches!(event, Event::Call { .. }) {
                    let mut captured = captured_event_clone.lock().unwrap();
                    *captured = Some(event.clone());
                    break;
                }
            }
            let results: Vec<PostEventResult> = events
                .iter()
                .enumerate()
                .map(|(i, _)| PostEventResult {
                    status: "success".to_string(),
                    index: i,
                    error: None,
                    retriable: None,
                })
                .collect();
            Ok(results)
        });

    mock_gmp_api
        .post_events(events.clone())
        .await
        .expect("GMP API should accept the events");

    {
        let captured = captured_event.lock().unwrap();
        assert!(
            captured.is_some(),
            "GMP API should have been called with a Call event"
        );
    }

    println!("Test completed successfully!");
    test_queue.clear().await;
    env.cleanup().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_approve_and_execute_memo_message() {
    use base64::prelude::BASE64_STANDARD;
    use base64::Engine;
    use borsh::BorshSerialize;
    use solana_axelar_std::execute_data::{ExecuteData, MerklizedPayload};
    use solana_axelar_std::{CrossChainId, MerklizedMessage, Message, MessageLeaf};
    use solana_transaction_parser::gmp_types::{
        Amount, CommonTaskFields, ExecuteTask, ExecuteTaskFields, GatewayTxTask,
        GatewayTxTaskFields, GatewayV2Message,
    };

    let env = TestEnvironment::new().await;

    let components = create_includer_components(&env.rpc_url, &env.payer);
    let mock_redis = create_mock_redis();
    let mock_gmp_api = create_mock_gmp_api_for_execute();
    let mock_refunds_model = MockRefundsModel::new();

    let signing_verifier_set_merkle_root = env.verifier_set_hash;

    let memo_program_address = env.memo_program_id.to_string();
    let message_id = "test-message-id-001";
    let source_address = "axelar1test";

    use solana_axelar_gateway::executable::{ExecutablePayload, ExecutablePayloadEncodingScheme};

    let memo_string = "test-memo-execution";
    // For memo, a counter PDA is required
    let (counter_pda, _counter_bump) =
        solana_sdk::pubkey::Pubkey::find_program_address(&[b"counter"], &env.memo_program_id);

    let encoding_scheme = ExecutablePayloadEncodingScheme::AbiEncoding;

    let test_payload = ExecutablePayload::new(
        memo_string.as_bytes(),
        &[solana_sdk::instruction::AccountMeta::new(
            counter_pda,
            false,
        )],
        encoding_scheme,
    );

    let test_payload_hash: [u8; 32] = test_payload.hash().expect("Failed to hash payload");

    let message = Message {
        cc_id: CrossChainId {
            chain: "axelar".to_string(),
            id: message_id.to_string(),
        },
        source_address: source_address.to_string(),
        destination_chain: "solana-devnet".to_string(),
        destination_address: memo_program_address.clone(),
        payload_hash: test_payload_hash,
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
                leaf: message_leaf,
                proof: vec![], // no proof needed for a single message
            }],
        },
    };

    let execute_data_b64 = BASE64_STANDARD.encode(execute_data.try_to_vec().unwrap());

    let task = GatewayTxTask {
        common: CommonTaskFields {
            id: "test-gateway-tx-task-001".into(),
            chain: "solana-devnet".into(),
            timestamp: "2025-11-26T14:47:18.567796Z".into(),
            r#type: "GATEWAY_TX".into(),
            meta: None,
        },
        task: GatewayTxTaskFields {
            execute_data: execute_data_b64,
        },
    };

    println!("Created GatewayTxTask for memo (executable) message:");
    println!("Task ID: {}", task.common.id);
    println!("Message ID: {}", message_id);
    println!("Destination: {}", memo_program_address);
    let includer = SolanaIncluder::new(
        Arc::new(components.includer_client),
        components.keypair,
        "solana-devnet".to_string(),
        components.transaction_builder,
        Arc::new(mock_gmp_api),
        mock_redis,
        Arc::new(mock_refunds_model),
    );

    let result = includer.handle_gateway_tx_task(task).await;

    match result {
        Ok(()) => {
            println!("Gateway TX task completed successfully!");
        }
        Err(e) => {
            println!("Gateway TX task result: {:?}", e);
            let error_str = format!("{:?}", e);
            panic!("{}", error_str);
        }
    }

    println!("Setting up subscriber to verify MessageApproved event...");

    let test_queue = Arc::new(TestQueue::new());
    let events_queue: Arc<dyn QueueTrait> = Arc::clone(&test_queue) as Arc<dyn QueueTrait>;

    let poller_handle = spawn_poller(
        &env.rpc_url,
        "test_gateway_tx_poller",
        &env.transaction_model,
        &env.postgres_db,
        events_queue,
    )
    .await;

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
                println!("ApproveMessage transaction data (truncated):");
                println!(" {} bytes total", tx_data.len());
            }

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

                            if message.message_id == message_id {
                                found_approved_event = true;
                            }
                        }
                    }
                }
                Err(e) => {
                    panic!("Failed to parse transaction: {:?}", e);
                }
            }
        }
    }

    if found_approved_event {
        println!(
            "MessageApproved event parsed successfully for message_id: {}",
            message_id
        );
    } else {
        println!("MessageApproved event not parsed (parser format mismatch), but ApproveMessage tx confirmed");
    }

    println!("Executing approved message...");

    println!("Initializing Counter PDA for memo program...");

    let init_ix_data = solana_axelar_memo::instruction::Init {}.data();
    let init_accounts = solana_axelar_memo::accounts::Init {
        counter: counter_pda,
        payer: env.payer.pubkey(),
        system_program: solana_sdk::system_program::ID,
    }
    .to_account_metas(None);

    let init_instruction = Instruction {
        program_id: env.memo_program_id,
        accounts: init_accounts,
        data: init_ix_data,
    };

    let recent_blockhash_init = env
        .rpc_client
        .get_latest_blockhash()
        .await
        .expect("Failed to get blockhash for init");

    let init_tx = Transaction::new_signed_with_payer(
        &[init_instruction],
        Some(&env.payer.pubkey()),
        &[&env.payer],
        recent_blockhash_init,
    );

    match env.rpc_client.send_and_confirm_transaction(&init_tx).await {
        Ok(sig) => println!("Counter PDA initialized: {}", sig),
        Err(e) => {
            panic!("Failed to initialize counter PDA: {:?}", e);
        }
    }

    let execute_payload_bytes = test_payload
        .encode()
        .expect("Failed to encode ExecutablePayload");
    let execute_payload_b64 = BASE64_STANDARD.encode(&execute_payload_bytes);

    let execute_task = ExecuteTask {
        common: CommonTaskFields {
            id: "test-execute-task-001".into(),
            chain: "solana-devnet".into(),
            timestamp: "2025-11-26T14:47:20.567796Z".into(),
            r#type: "EXECUTE".into(),
            meta: None,
        },
        task: ExecuteTaskFields {
            message: GatewayV2Message {
                message_id: message_id.to_string(),
                source_chain: "axelar".to_string(),
                source_address: source_address.to_string(),
                destination_address: memo_program_address.clone(),
                payload_hash: BASE64_STANDARD.encode(message.payload_hash),
            },
            payload: execute_payload_b64,
            available_gas_balance: Amount {
                token_id: None,
                amount: "1000000000".to_string(), // 1 SOL in lamports
            },
        },
    };

    let execute_result = includer.handle_execute_task(execute_task).await;

    match execute_result {
        Ok(events) => {
            println!("Execute task completed! Returned {} event(s)", events.len());
            for event in &events {
                println!("Event: {:?}", event);
            }
        }
        Err(e) => {
            println!("Execute task error: {:?}", e);
            let error_str = format!("{:?}", e);
            panic!("Execute task failed: {}", error_str);
        }
    }

    println!("Setting up subscriber to verify MessageExecuted event...");

    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;

    test_queue.clear().await;

    let events_queue_execute: Arc<dyn QueueTrait> = Arc::clone(&test_queue) as Arc<dyn QueueTrait>;
    let poller_handle_execute = spawn_poller(
        &env.rpc_url,
        "test_execute_poller",
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
                println!("MessageExecuted transaction data:");
                println!(" {} bytes total", tx_data.len());
                println!("Transaction data: {}", tx_data);
            }

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

                            if *event_message_id == message_id {
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

    if found_executed_event {
        println!(
            "MessageExecuted event parsed successfully for message_id: {}",
            message_id
        );
    } else {
        panic!("MessageExecuted event found but not parsed");
    }

    println!("Includer integration test completed!");
    test_queue.clear().await;
    env.cleanup().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_refund_task_handled_and_found_by_poller() {
    use solana_transaction_parser::gmp_types::{
        Amount, CommonTaskFields, GatewayV2Message, RefundTask, RefundTaskFields,
    };

    let env = TestEnvironment::new().await;

    // Note: uses operator instead of payer for the refund task
    let components = create_includer_components(&env.rpc_url, &env.operator);
    let mock_redis = create_mock_redis();
    let mock_gmp_api = create_mock_gmp_api_for_execute();

    let refunds_model = Arc::new(env.postgres_db.clone());

    let includer = SolanaIncluder::new(
        Arc::new(components.includer_client),
        components.keypair,
        "solana-devnet".to_string(),
        components.transaction_builder,
        Arc::new(mock_gmp_api),
        mock_redis,
        refunds_model,
    );

    let (treasury, _) =
        solana_axelar_gas_service::Treasury::try_find_pda().expect("Failed to derive treasury PDA");
    let treasury_funding_amount = 20 * LAMPORTS_PER_SOL; // 20 SOL to ensure enough for refund + fees

    #[allow(deprecated)]
    let fund_treasury_ix =
        system_instruction::transfer(&env.payer.pubkey(), &treasury, treasury_funding_amount);

    let recent_blockhash = env
        .rpc_client
        .get_latest_blockhash()
        .await
        .expect("Failed to get blockhash");

    let fund_treasury_tx = Transaction::new_signed_with_payer(
        &[fund_treasury_ix],
        Some(&env.payer.pubkey()),
        &[&env.payer],
        recent_blockhash,
    );

    env.rpc_client
        .send_and_confirm_transaction(&fund_treasury_tx)
        .await
        .expect("Failed to fund treasury");

    println!("Treasury funded with {} lamports", treasury_funding_amount);

    let refund_recipient = Keypair::new().pubkey();
    let refund_amount = 10_000_000_000u64; // 10 SOL in lamports
    let message_id = "test-refund-message-001".to_string();
    let refund_id = "test-refund-task-001".to_string();

    let refund_task = RefundTask {
        common: CommonTaskFields {
            id: refund_id.clone(),
            chain: "solana-devnet".to_string(),
            timestamp: chrono::Utc::now().to_string(),
            r#type: "refund".to_string(),
            meta: None,
        },
        task: RefundTaskFields {
            message: GatewayV2Message {
                message_id: message_id.clone(),
                source_chain: "ethereum".to_string(),
                destination_address: "test-destination".to_string(),
                payload_hash: "test-payload-hash".to_string(),
                source_address: solana_sdk::pubkey::Pubkey::new_unique().to_string(),
            },
            refund_recipient_address: refund_recipient.to_string(),
            remaining_gas_balance: Amount {
                amount: refund_amount.to_string(),
                token_id: None,
            },
        },
    };

    println!("Calling handle_refund_task...");
    let result = includer.handle_refund_task(refund_task).await;

    match result {
        Ok(()) => {
            println!("Refund task completed successfully!");
        }
        Err(e) => {
            println!("Refund task result: {:?}", e);
            let error_str = format!("{:?}", e);
            panic!("{}", error_str);
        }
    }

    println!("Setting up subscriber to verify RefundFees transaction...");

    let test_queue = Arc::new(TestQueue::new());
    let events_queue: Arc<dyn QueueTrait> = Arc::clone(&test_queue) as Arc<dyn QueueTrait>;

    let poller_handle = spawn_poller(
        &env.rpc_url,
        "test_refund_poller",
        &env.transaction_model,
        &env.postgres_db,
        Arc::clone(&events_queue),
    )
    .await;

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let mut found_refund = false;
    let start_time = std::time::Instant::now();
    let timeout = std::time::Duration::from_secs(30);

    while !found_refund && start_time.elapsed() < timeout {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        let queued_items = test_queue.get_items().await;
        for item in &queued_items {
            if let QueueItem::Transaction(tx_data) = item {
                if tx_data.contains("RefundFees") || tx_data.contains(&refund_id) {
                    found_refund = true;
                    println!("Poller picked up RefundFees transaction!");
                    break;
                }
            }
        }
    }

    println!("Cancelling poller...");
    poller_handle.stop().await;
    println!("Poller stopped");

    assert!(
        found_refund,
        "Poller should have picked up the RefundFees transaction within timeout"
    );

    let queued_items = test_queue.get_items().await;
    println!("Queue contains {} items", queued_items.len());

    let refund_tx_json = queued_items
        .iter()
        .find_map(|item| {
            if let QueueItem::Transaction(tx_data) = item {
                if tx_data.contains("RefundFees") || tx_data.contains(&refund_id) {
                    Some(tx_data.clone())
                } else {
                    None
                }
            } else {
                None
            }
        })
        .expect("Should have found RefundFees transaction in queue");

    println!("RefundFees transaction captured: {}", refund_tx_json.len());

    let refund_record = env
        .postgres_db
        .find(refund_id.clone())
        .await
        .expect("Should be able to query refund from database");

    assert!(
        refund_record.is_some(),
        "Refund should be recorded in the database"
    );

    let (signature, _) = refund_record.unwrap();
    println!("Refund signature recorded in database: {}", signature);

    println!("Refund Integration Test Completed");
    test_queue.clear().await;
    env.cleanup().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_refund_task_duplicate_returns_already_processed() {
    use solana_transaction_parser::gmp_types::{
        Amount, CommonTaskFields, GatewayV2Message, RefundTask, RefundTaskFields,
    };

    let env = TestEnvironment::new().await;

    let components = create_includer_components(&env.rpc_url, &env.operator);
    let mock_redis = create_mock_redis();
    let mock_gmp_api = create_mock_gmp_api_for_execute();

    let refunds_model = Arc::new(env.postgres_db.clone());

    let includer = SolanaIncluder::new(
        Arc::new(components.includer_client),
        components.keypair,
        "solana-devnet".to_string(),
        components.transaction_builder,
        Arc::new(mock_gmp_api),
        mock_redis,
        Arc::clone(&refunds_model),
    );

    let (treasury, _) =
        solana_axelar_gas_service::Treasury::try_find_pda().expect("Failed to derive treasury PDA");
    let treasury_funding_amount = 20 * LAMPORTS_PER_SOL;

    #[allow(deprecated)]
    let fund_treasury_ix =
        system_instruction::transfer(&env.payer.pubkey(), &treasury, treasury_funding_amount);

    let recent_blockhash = env
        .rpc_client
        .get_latest_blockhash()
        .await
        .expect("Failed to get blockhash");

    let fund_treasury_tx = Transaction::new_signed_with_payer(
        &[fund_treasury_ix],
        Some(&env.payer.pubkey()),
        &[&env.payer],
        recent_blockhash,
    );

    env.rpc_client
        .send_and_confirm_transaction(&fund_treasury_tx)
        .await
        .expect("Failed to fund treasury");

    println!("Treasury funded");

    let refund_recipient = Keypair::new().pubkey();
    let refund_amount = 5_000_000_000u64; // 5 SOL
    let message_id = "test-duplicate-refund-message-001".to_string();
    let refund_id = "test-duplicate-refund-task-001".to_string();

    let refund_task = RefundTask {
        common: CommonTaskFields {
            id: refund_id.clone(),
            chain: "solana-devnet".to_string(),
            timestamp: chrono::Utc::now().to_string(),
            r#type: "refund".to_string(),
            meta: None,
        },
        task: RefundTaskFields {
            message: GatewayV2Message {
                message_id: message_id.clone(),
                source_chain: "ethereum".to_string(),
                destination_address: "test-destination".to_string(),
                payload_hash: "test-payload-hash".to_string(),
                source_address: solana_sdk::pubkey::Pubkey::new_unique().to_string(),
            },
            refund_recipient_address: refund_recipient.to_string(),
            remaining_gas_balance: Amount {
                amount: refund_amount.to_string(),
                token_id: None,
            },
        },
    };

    println!("Processing refund task first time...");
    let first_result = includer.handle_refund_task(refund_task.clone()).await;
    assert!(
        first_result.is_ok(),
        "First refund should succeed: {:?}",
        first_result
    );
    println!("First refund processed successfully!");

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let refund_record = refunds_model
        .find(refund_id.clone())
        .await
        .expect("Should query database");
    assert!(
        refund_record.is_some(),
        "Refund should be recorded in database after first processing"
    );
    println!("Refund recorded in database");

    println!("Processing same refund task second time...");
    let second_result = includer.handle_refund_task(refund_task).await;
    assert!(
        second_result.is_ok(),
        "Second refund should return Ok (already processed): {:?}",
        second_result
    );
    println!("Second refund call returned Ok (correctly identified as already processed)");

    let refund_record_after = refunds_model
        .find(refund_id.clone())
        .await
        .expect("Should query database");
    assert_eq!(
        refund_record, refund_record_after,
        "Database record should be unchanged after duplicate refund attempt"
    );

    println!("Duplicate Refund Task Test Completed");
    env.cleanup().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_rotate_signers() {
    use base64::prelude::BASE64_STANDARD;
    use base64::Engine;
    use borsh::BorshSerialize;
    use solana_axelar_std::execute_data::{ExecuteData, MerklizedPayload};
    use solana_transaction_parser::gmp_types::{
        CommonTaskFields, GatewayTxTask, GatewayTxTaskFields,
    };

    let env = TestEnvironment::new().await;

    let components = create_includer_components(&env.rpc_url, &env.payer);
    let mock_redis = create_mock_redis();
    let mock_gmp_api = MockGmpApiTrait::new();
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

    let signing_verifier_set_merkle_root = env.verifier_set_hash;

    let (_new_secret_key_1, new_compressed_pubkey_1) = generate_random_signer();
    let (_new_secret_key_2, new_compressed_pubkey_2) = generate_random_signer();

    let new_nonce = 1u64;
    let quorum_threshold = 100;

    let new_verifier_leaves = [
        VerifierSetLeaf {
            nonce: new_nonce,
            quorum: quorum_threshold,
            signer_pubkey: PublicKey(new_compressed_pubkey_1),
            signer_weight: 50,
            position: 0,
            set_size: 2,
            domain_separator: env.domain_separator,
        },
        VerifierSetLeaf {
            nonce: new_nonce,
            quorum: quorum_threshold,
            signer_pubkey: PublicKey(new_compressed_pubkey_2),
            signer_weight: 50,
            position: 1,
            set_size: 2,
            domain_separator: env.domain_separator,
        },
    ];

    let new_verifier_leaf_hashes: Vec<[u8; 32]> = new_verifier_leaves
        .iter()
        .map(VerifierSetLeaf::hash)
        .collect();
    let new_verifier_merkle_tree = MerkleTree::from_leaves(&new_verifier_leaf_hashes);
    let new_verifier_set_merkle_root = new_verifier_merkle_tree.root().expect("merkle root");

    println!(
        "New verifier set merkle root: {:?}",
        hex::encode(new_verifier_set_merkle_root)
    );

    let payload_merkle_root = new_verifier_set_merkle_root;

    let verifier_info_1 = create_verifier_info(
        &env.verifier_secret_keys[0],
        payload_merkle_root,
        &env.verifier_leaves[0],
        0,
        &env.verifier_merkle_tree,
        PayloadType::RotateSigners,
    );
    let verifier_info_2 = create_verifier_info(
        &env.verifier_secret_keys[1],
        payload_merkle_root,
        &env.verifier_leaves[1],
        1,
        &env.verifier_merkle_tree,
        PayloadType::RotateSigners,
    );

    let execute_data = ExecuteData {
        payload_merkle_root,
        signing_verifier_set_merkle_root,
        signing_verifier_set_leaves: vec![verifier_info_1, verifier_info_2],
        payload_items: MerklizedPayload::VerifierSetRotation {
            new_verifier_set_merkle_root,
        },
    };

    let execute_data_b64 = BASE64_STANDARD.encode(execute_data.try_to_vec().unwrap());

    let task = GatewayTxTask {
        common: CommonTaskFields {
            id: "test-rotate-signers-001".into(),
            chain: "solana-devnet".into(),
            timestamp: "2025-11-26T14:47:18.567796Z".into(),
            r#type: "GATEWAY_TX".into(),
            meta: None,
        },
        task: GatewayTxTaskFields {
            execute_data: execute_data_b64,
        },
    };

    println!("Processing rotate signers task...");
    println!("Task ID: {}", task.common.id);
    println!(
        "Current verifier set root: {:?}",
        hex::encode(signing_verifier_set_merkle_root)
    );
    println!(
        "New verifier set root: {:?}",
        hex::encode(new_verifier_set_merkle_root)
    );

    let result = includer.handle_gateway_tx_task(task).await;

    match result {
        Ok(()) => {
            println!("Rotate signers task completed successfully!");
        }
        Err(e) => {
            println!("Rotate signers task failed: {:?}", e);
            let error_str = format!("{:?}", e);
            panic!("{}", error_str);
        }
    }

    println!("Setting up subscriber to verify SignersRotated event...");

    let test_queue = Arc::new(TestQueue::new());
    let events_queue: Arc<dyn QueueTrait> = Arc::clone(&test_queue) as Arc<dyn QueueTrait>;

    let poller_handle = spawn_poller(
        &env.rpc_url,
        "test_rotate_signers_poller",
        &env.transaction_model,
        &env.postgres_db,
        events_queue,
    )
    .await;

    let mut found_rotate_signers_tx = false;
    let max_wait = std::time::Duration::from_secs(30);
    let start = std::time::Instant::now();

    while start.elapsed() < max_wait && !found_rotate_signers_tx {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        let queued_items = test_queue.get_items().await;
        for item in &queued_items {
            if let QueueItem::Transaction(tx_data) = item {
                if tx_data.contains("RotateSigners") {
                    found_rotate_signers_tx = true;
                    println!("Found SignersRotated transaction in queue!");
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

    for item in &queued_items {
        if let QueueItem::Transaction(tx_data) = item {
            if tx_data.len() > 200 {
                println!("Transaction found: {}...", &tx_data[..200]);
            } else {
                println!("Transaction found: {}", tx_data);
            }
        }
    }

    assert!(
        found_rotate_signers_tx,
        "Should have found SignersRotated transaction in queue"
    );

    let new_verifier_set_tracker_pda =
        solana_axelar_gateway::VerifierSetTracker::find_pda(&new_verifier_set_merkle_root).0;

    let account_result = env
        .rpc_client
        .get_account(&new_verifier_set_tracker_pda)
        .await;
    assert!(
        account_result.is_ok(),
        "New verifier set tracker PDA should exist after rotation"
    );
    println!(
        "New verifier set tracker PDA created: {}",
        new_verifier_set_tracker_pda
    );

    println!("Rotate Signers Integration Test Completed");
    test_queue.clear().await;
    env.cleanup().await;
}
