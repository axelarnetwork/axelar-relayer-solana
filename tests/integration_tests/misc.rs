//! Miscellaneous integration tests for event parsing
//!
//! Tests for gas service events (GasPaidEvent, GasAddedEvent, GasRefundedEvent)

use std::sync::Arc;

use anchor_lang::{InstructionData, ToAccountMetas};
use relayer_core::gmp_api::gmp_types::Event;
use relayer_core::gmp_api::MockGmpApiTrait;
use relayer_core::includer_worker::IncluderTrait;
use relayer_core::ingestor::IngestorTrait;
use relayer_core::queue::{QueueItem, QueueTrait};
use solana::includer::SolanaIncluder;
use solana::ingestor::SolanaIngestor;
use solana::mocks::MockUpdateEvents;
use solana_sdk::instruction::Instruction;
use solana_sdk::native_token::LAMPORTS_PER_SOL;
use solana_sdk::signature::{Keypair, Signer};
#[allow(deprecated)]
use solana_sdk::system_instruction;
use solana_sdk::transaction::Transaction;
use solana_transaction_parser::gmp_types::{
    Amount, CommonTaskFields, GatewayV2Message, RefundTask, RefundTaskFields,
};
use solana_transaction_parser::parser::TransactionParser;
use solana_transaction_parser::redis::MockCostCacheTrait;

use solana_axelar_gateway::ID as GATEWAY_PROGRAM_ID;

use super::common::*;

/// Test that PayGas emits a GasCredit event that can be parsed
/// This requires a CallContract event in the same transaction
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_gas_paid_event_parsing() {
    let env = TestEnvironment::new().await;

    let test_queue = Arc::new(TestQueue::new());
    let events_queue: Arc<dyn QueueTrait> = Arc::clone(&test_queue) as Arc<dyn QueueTrait>;

    let poller_handle = spawn_poller(
        &env.rpc_url,
        "test_gas_paid_poller",
        &env.transaction_model,
        &env.postgres_db,
        Arc::clone(&events_queue),
    )
    .await;

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let treasury = solana_axelar_gas_service::Treasury::find_pda().0;
    let (gas_service_event_authority, _) = solana_sdk::pubkey::Pubkey::find_program_address(
        &[b"__event_authority"],
        &solana_axelar_gas_service::ID,
    );

    let (gateway_event_authority, _) = solana_sdk::pubkey::Pubkey::find_program_address(
        &[b"__event_authority"],
        &GATEWAY_PROGRAM_ID,
    );

    let destination_chain = "ethereum".to_string();
    let destination_address = "0xGasTestContract".to_string();
    let payload = b"Gas payment test payload".to_vec();

    let call_contract_ix_data = solana_axelar_gateway::instruction::CallContract {
        destination_chain: destination_chain.clone(),
        destination_contract_address: destination_address.clone(),
        payload: payload.clone(),
        signing_pda_bump: 0,
    };

    let mut call_contract_accounts = solana_axelar_gateway::accounts::CallContract {
        caller: env.payer.pubkey(),
        signing_pda: None,
        gateway_root_pda: env.gateway_root_pda,
        event_authority: gateway_event_authority,
        program: GATEWAY_PROGRAM_ID,
    }
    .to_account_metas(None);

    call_contract_accounts[0].is_signer = true;

    let call_contract_instruction = Instruction {
        program_id: GATEWAY_PROGRAM_ID,
        accounts: call_contract_accounts,
        data: call_contract_ix_data.data(),
    };

    let gas_amount = LAMPORTS_PER_SOL / 10; // 0.1 SOL
    let refund_address = env.payer.pubkey();

    let payload_hash = solana_sdk::keccak::hashv(&[&payload]).to_bytes();

    let pay_gas_ix_data = solana_axelar_gas_service::instruction::PayGas {
        destination_chain: destination_chain.clone(),
        destination_address: destination_address.clone(),
        payload_hash,
        refund_address,
        amount: gas_amount,
    };

    let pay_gas_accounts = solana_axelar_gas_service::accounts::PayGas {
        sender: env.payer.pubkey(),
        treasury,
        event_authority: gas_service_event_authority,
        system_program: solana_sdk::system_program::ID,
        program: solana_axelar_gas_service::ID,
    }
    .to_account_metas(None);

    let pay_gas_instruction = Instruction {
        program_id: solana_axelar_gas_service::ID,
        accounts: pay_gas_accounts,
        data: pay_gas_ix_data.data(),
    };

    let recent_blockhash = env
        .rpc_client
        .get_latest_blockhash()
        .await
        .expect("Failed to get blockhash");

    let tx = Transaction::new_signed_with_payer(
        &[call_contract_instruction, pay_gas_instruction],
        Some(&env.payer.pubkey()),
        &[&env.payer],
        recent_blockhash,
    );

    let signature = env
        .rpc_client
        .send_and_confirm_transaction(&tx)
        .await
        .expect("Failed to send CallContract + PayGas transaction");

    println!("CallContract + PayGas transaction sent: {}", signature);

    let mut found_gas_paid_tx = false;
    let start_time = std::time::Instant::now();
    let timeout = std::time::Duration::from_secs(30);

    while !found_gas_paid_tx && start_time.elapsed() < timeout {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        let queued_items = test_queue.get_items().await;
        for item in &queued_items {
            if let QueueItem::Transaction(tx_data) = item {
                if tx_data.contains("CallContract") {
                    found_gas_paid_tx = true;
                    println!("Found CallContract + GasPaid transaction in queue!");
                    break;
                }
            }
        }
    }

    poller_handle.stop().await;

    let queued_items = test_queue.get_items().await;
    println!("Queue contains {} items", queued_items.len());

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

    let mut found_gas_credit_event = false;
    let mut found_call_event = false;

    for item in &queued_items {
        if let QueueItem::Transaction(tx_data) = item {
            match ingestor.handle_transaction(tx_data.to_string()).await {
                Ok(events) => {
                    for event in &events {
                        match event {
                            Event::Call {
                                common,
                                message,
                                destination_chain: dest_chain,
                                ..
                            } => {
                                println!("Found Call event!");
                                println!("Event ID: {}", common.event_id);
                                println!("Message ID: {}", message.message_id);
                                println!("Destination: {}", dest_chain);
                                found_call_event = true;
                            }
                            Event::GasCredit {
                                common,
                                message_id,
                                payment,
                                refund_address: refund_addr,
                                ..
                            } => {
                                println!("Found GasCredit event (from GasPaid)!");
                                println!("Event ID: {}", common.event_id);
                                println!("Message ID: {}", message_id);
                                println!("Payment: {:?}", payment);
                                println!("Refund Address: {}", refund_addr);
                                found_gas_credit_event = true;
                            }
                            _ => {}
                        }
                    }
                }
                Err(e) => {
                    println!("Transaction parse error: {:?}", e);
                }
            }
        }
    }

    assert!(
        found_call_event,
        "Should have parsed the CallContract event"
    );
    assert!(
        found_gas_credit_event,
        "Should have parsed the GasCredit event (from GasPaid)"
    );

    println!("GasPaid event parsing test completed successfully!");
    test_queue.clear().await;
    env.cleanup().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_gas_added_event_parsing() {
    let env = TestEnvironment::new().await;

    let test_queue = Arc::new(TestQueue::new());
    let events_queue: Arc<dyn QueueTrait> = Arc::clone(&test_queue) as Arc<dyn QueueTrait>;

    let poller_handle = spawn_poller(
        &env.rpc_url,
        "test_gas_added_poller",
        &env.transaction_model,
        &env.postgres_db,
        Arc::clone(&events_queue),
    )
    .await;

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let treasury = solana_axelar_gas_service::Treasury::find_pda().0;
    let (gas_service_event_authority, _) = solana_sdk::pubkey::Pubkey::find_program_address(
        &[b"__event_authority"],
        &solana_axelar_gas_service::ID,
    );

    let message_id =
        "FakeSignatureForAddGasTest1234567890123456789012345678901234567890-2.1".to_string();
    let gas_amount = LAMPORTS_PER_SOL / 20; // 0.05 SOL
    let refund_address = env.payer.pubkey();

    let add_gas_ix_data = solana_axelar_gas_service::instruction::AddGas {
        message_id: message_id.clone(),
        refund_address,
        amount: gas_amount,
    };

    let add_gas_accounts = solana_axelar_gas_service::accounts::AddGas {
        sender: env.payer.pubkey(),
        treasury,
        event_authority: gas_service_event_authority,
        system_program: solana_sdk::system_program::ID,
        program: solana_axelar_gas_service::ID,
    }
    .to_account_metas(None);

    let add_gas_instruction = Instruction {
        program_id: solana_axelar_gas_service::ID,
        accounts: add_gas_accounts,
        data: add_gas_ix_data.data(),
    };

    let recent_blockhash = env
        .rpc_client
        .get_latest_blockhash()
        .await
        .expect("Failed to get blockhash");

    let tx = Transaction::new_signed_with_payer(
        &[add_gas_instruction],
        Some(&env.payer.pubkey()),
        &[&env.payer],
        recent_blockhash,
    );

    let signature = env
        .rpc_client
        .send_and_confirm_transaction(&tx)
        .await
        .expect("Failed to send AddGas transaction");

    println!("AddGas transaction sent: {}", signature);

    let mut found_gas_added_tx = false;
    let start_time = std::time::Instant::now();
    let timeout = std::time::Duration::from_secs(30);

    while !found_gas_added_tx && start_time.elapsed() < timeout {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        let queued_items = test_queue.get_items().await;
        for item in &queued_items {
            if let QueueItem::Transaction(tx_data) = item {
                if tx_data.contains("AddGas") || tx_data.contains(&signature.to_string()) {
                    found_gas_added_tx = true;
                    println!("Found AddGas transaction in queue!");
                    break;
                }
            }
        }
    }

    poller_handle.stop().await;

    let queued_items = test_queue.get_items().await;
    println!("Queue contains {} items", queued_items.len());

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

    let mut found_gas_credit_event = false;

    for item in &queued_items {
        if let QueueItem::Transaction(tx_data) = item {
            match ingestor.handle_transaction(tx_data.to_string()).await {
                Ok(events) => {
                    for event in &events {
                        if let Event::GasCredit {
                            common,
                            message_id: event_msg_id,
                            payment,
                            refund_address: refund_addr,
                            ..
                        } = event
                        {
                            println!("Found GasCredit event (from GasAdded)!");
                            println!("Event ID: {}", common.event_id);
                            println!("Message ID: {}", event_msg_id);
                            println!("Payment: {:?}", payment);
                            println!("Refund Address: {}", refund_addr);

                            if *event_msg_id == message_id {
                                found_gas_credit_event = true;
                            }
                        }
                    }
                }
                Err(e) => {
                    println!("Transaction parse error: {:?}", e);
                }
            }
        }
    }

    assert!(
        found_gas_credit_event,
        "Should have parsed the GasCredit event (from GasAdded) for message_id: {}",
        message_id
    );

    println!("GasAdded event parsing test completed successfully!");
    test_queue.clear().await;
    env.cleanup().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_gas_refunded_event_parsing() {
    let env = TestEnvironment::new().await;

    let components = create_includer_components(&env.rpc_url, &env.operator);
    let mock_redis = create_mock_redis();
    let mock_gmp_api = MockGmpApiTrait::new();
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

    let (treasury, _) = solana::utils::get_treasury_pda().expect("Failed to derive treasury PDA");
    let treasury_funding_amount = 5 * LAMPORTS_PER_SOL;

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
    let refund_amount = 1_000_000_000u64; // 1 SOL
    let message_id =
        "FakeSignatureForRefundTest1234567890123456789012345678901234567890-3.1".to_string();
    let refund_id = "test-refund-parsing-task-001".to_string();

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

    let test_queue = Arc::new(TestQueue::new());
    let events_queue: Arc<dyn QueueTrait> = Arc::clone(&test_queue) as Arc<dyn QueueTrait>;

    let poller_handle = spawn_poller(
        &env.rpc_url,
        "test_refund_parsing_poller",
        &env.transaction_model,
        &env.postgres_db,
        Arc::clone(&events_queue),
    )
    .await;

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    println!("Executing refund task...");
    includer
        .handle_refund_task(refund_task)
        .await
        .expect("Refund task should succeed");
    println!("Refund task completed!");

    let mut found_refund_tx = false;
    let start_time = std::time::Instant::now();
    let timeout = std::time::Duration::from_secs(30);

    while !found_refund_tx && start_time.elapsed() < timeout {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        let queued_items = test_queue.get_items().await;
        for item in &queued_items {
            if let QueueItem::Transaction(tx_data) = item {
                if tx_data.contains(&message_id) {
                    found_refund_tx = true;
                    println!("Found Refund transaction in queue!");
                    break;
                }
            }
        }
    }

    poller_handle.stop().await;

    let queued_items = test_queue.get_items().await;
    println!("Queue contains {} items", queued_items.len());

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

    let mut found_gas_refunded_event = false;

    for item in &queued_items {
        if let QueueItem::Transaction(tx_data) = item {
            match ingestor.handle_transaction(tx_data.to_string()).await {
                Ok(events) => {
                    for event in &events {
                        if let Event::GasRefunded {
                            common,
                            message_id: event_msg_id,
                            cost,
                            ..
                        } = event
                        {
                            println!("Found GasRefunded event!");
                            println!("Event ID: {}", common.event_id);
                            println!("Message ID: {}", event_msg_id);
                            println!("Cost: {:?}", cost);

                            if *event_msg_id == message_id {
                                found_gas_refunded_event = true;
                            }
                        }
                    }
                }
                Err(e) => {
                    println!("Transaction parse error: {:?}", e);
                }
            }
        }
    }

    assert!(
        found_gas_refunded_event,
        "Should have parsed the GasRefunded event for message_id: {}",
        message_id
    );

    println!("GasRefunded event parsing test completed successfully!");
    test_queue.clear().await;
    env.cleanup().await;
}
