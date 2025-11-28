//! Integration tests for Axelar Solana programs
//! To run:
//! cargo test --test test

#![allow(clippy::indexing_slicing)]

use std::path::PathBuf;
use std::sync::Arc;

use anchor_lang::{InstructionData, ToAccountMetas};
use async_trait::async_trait;
use relayer_core::gmp_api::gmp_types::{Event, PostEventResult};
use relayer_core::gmp_api::{GmpApiTrait, MockGmpApiTrait};
use relayer_core::ingestor::IngestorTrait;
use relayer_core::queue::{QueueItem, QueueTrait};
use solana::ingestor::SolanaIngestor;
use solana::mocks::MockUpdateEvents;
use solana::models::solana_subscriber_cursor::{AccountPollerEnum, PostgresDB};
use solana::models::solana_transaction::PgSolanaTransactionModel;
use solana::poll_client::SolanaRpcClient;
use solana::subscriber_poller::{SolanaPoller, TransactionPoller};
use solana_rpc::rpc::JsonRpcConfig;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::account::AccountSharedData;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::instruction::Instruction;
use solana_sdk::native_token::LAMPORTS_PER_SOL;
use solana_sdk::signature::{Keypair, Signer};
use solana_sdk::transaction::Transaction;
use solana_sdk_ids::bpf_loader_upgradeable;
use solana_system_interface::program::ID as SYSTEM_PROGRAM_ID;
use solana_test_validator::{TestValidatorGenesis, UpgradeableProgramInfo};
use solana_transaction_parser::parser::TransactionParser;
use solana_transaction_parser::redis::MockCostCacheTrait;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres;
use tokio::sync::Mutex;

use solana_axelar_gateway::state::config::{InitialVerifierSet, InitializeConfigParams};
use solana_axelar_gateway::ID as GATEWAY_PROGRAM_ID;
use solana_axelar_std::{hasher::LeafHash, MerkleTree, PublicKey, VerifierSetLeaf, U256};

/// A simple in-memory queue for testing that collects published items
/// without starting any background workers or requiring Redis.
struct TestQueue {
    items: Mutex<Vec<QueueItem>>,
}

impl TestQueue {
    fn new() -> Self {
        Self {
            items: Mutex::new(Vec::new()),
        }
    }

    async fn get_items(&self) -> Vec<QueueItem> {
        self.items.lock().await.clone()
    }
}

#[async_trait]
impl QueueTrait for TestQueue {
    async fn publish(&self, item: QueueItem) {
        self.items.lock().await.push(item);
    }

    async fn republish(
        &self,
        _delivery: lapin::message::Delivery,
        _requeue: bool,
    ) -> Result<(), anyhow::Error> {
        Ok(())
    }

    async fn consumer(&self) -> Result<lapin::Consumer, anyhow::Error> {
        Err(anyhow::anyhow!("TestQueue does not support consuming"))
    }

    async fn close(&self) {
        // No-op for test queue
    }
}

fn programs_dir() -> PathBuf {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")
        .unwrap_or_else(|_| env!("CARGO_MANIFEST_DIR").to_owned());
    PathBuf::from(manifest_dir)
        .join("tests")
        .join("testdata")
        .join("programs")
}

fn cleanup_leftover_validators() {
    use std::process::Command;
    let _ = Command::new("pkill")
        .args(["-9", "-f", "solana-test-validator"])
        .output();
    std::thread::sleep(std::time::Duration::from_millis(500));
}

/// Generate a random secp256k1 signer for gateway verification
fn generate_random_signer() -> (libsecp256k1::SecretKey, [u8; 33]) {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let secret_key_bytes: [u8; 32] = rng.gen();
    let secret_key = libsecp256k1::SecretKey::parse(&secret_key_bytes).expect("valid secret key");
    let public_key = libsecp256k1::PublicKey::from_secret_key(&secret_key);
    let compressed_pubkey = public_key.serialize_compressed();
    (secret_key, compressed_pubkey)
}

/// Test that call_contract transactions are captured by the poller and published to the queue
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_call_contract_picked_up_and_sent_to_gmp() {
    cleanup_leftover_validators();

    let init_sql = format!(
        "{}\n{}\n",
        include_str!("../migrations/0006_solana_transactions.sql"),
        include_str!("../migrations/0007_solana_subscriber_cursors.sql"),
    );
    let db_container = postgres::Postgres::default()
        .with_init_sql(init_sql.into_bytes())
        .start()
        .await
        .expect("Failed to start postgres container");

    let connection_string = format!(
        "postgres://postgres:postgres@{}:{}/postgres",
        db_container.get_host().await.expect("get host"),
        db_container
            .get_host_port_ipv4(5432)
            .await
            .expect("get port")
    );

    let postgres_db = PostgresDB::new(&connection_string)
        .await
        .expect("Failed to connect to test database");
    let pg_pool = sqlx::PgPool::connect(&connection_string)
        .await
        .expect("Failed to create pool");
    let transaction_model = PgSolanaTransactionModel::new(pg_pool);

    let upgrade_authority = Keypair::new();
    let operator = Keypair::new();

    let mut validator = TestValidatorGenesis::default();

    let mut rpc_config = JsonRpcConfig::default_for_test();
    rpc_config.enable_rpc_transaction_history = true;
    rpc_config.enable_extended_tx_metadata_storage = true;
    validator.rpc_config(rpc_config);

    validator.add_account(
        upgrade_authority.pubkey(),
        AccountSharedData::new(100 * LAMPORTS_PER_SOL, 0, &SYSTEM_PROGRAM_ID),
    );
    validator.add_account(
        operator.pubkey(),
        AccountSharedData::new(100 * LAMPORTS_PER_SOL, 0, &SYSTEM_PROGRAM_ID),
    );

    let programs_path = programs_dir();

    let program_files = [
        "solana_axelar_gateway.so",
        "solana_axelar_gas_service.so",
        "solana_axelar_its.so",
        "solana_axelar_governance.so",
        "solana_axelar_operators.so",
        "solana_axelar_memo.so",
    ];
    for file in &program_files {
        let path = programs_path.join(file);
        if !path.exists() {
            panic!("Program file not found: {:?}", path);
        }
    }

    // Add memo program ID
    let memo_program_id = solana_sdk::pubkey::Pubkey::new_unique();

    validator.add_upgradeable_programs_with_path(&[
        UpgradeableProgramInfo {
            program_id: solana_axelar_gateway::ID,
            loader: bpf_loader_upgradeable::id(),
            upgrade_authority: upgrade_authority.pubkey(),
            program_path: programs_path.join("solana_axelar_gateway.so"),
        },
        UpgradeableProgramInfo {
            program_id: solana_axelar_gas_service::ID,
            loader: bpf_loader_upgradeable::id(),
            upgrade_authority: upgrade_authority.pubkey(),
            program_path: programs_path.join("solana_axelar_gas_service.so"),
        },
        UpgradeableProgramInfo {
            program_id: solana_axelar_its::ID,
            loader: bpf_loader_upgradeable::id(),
            upgrade_authority: operator.pubkey(),
            program_path: programs_path.join("solana_axelar_its.so"),
        },
        UpgradeableProgramInfo {
            program_id: solana_axelar_governance::ID,
            loader: bpf_loader_upgradeable::id(),
            upgrade_authority: upgrade_authority.pubkey(),
            program_path: programs_path.join("solana_axelar_governance.so"),
        },
        UpgradeableProgramInfo {
            program_id: solana_axelar_operators::ID,
            loader: bpf_loader_upgradeable::id(),
            upgrade_authority: upgrade_authority.pubkey(),
            program_path: programs_path.join("solana_axelar_operators.so"),
        },
        UpgradeableProgramInfo {
            program_id: memo_program_id,
            loader: bpf_loader_upgradeable::id(),
            upgrade_authority: upgrade_authority.pubkey(),
            program_path: programs_path.join("solana_axelar_memo.so"),
        },
    ]);

    let (test_validator, payer) = validator.start_async().await;
    let rpc_url = test_validator.rpc_url();

    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;

    let rpc_client = RpcClient::new_with_commitment(rpc_url.clone(), CommitmentConfig::confirmed());

    // Initialize Operators
    let registry = solana_axelar_operators::OperatorRegistry::find_pda().0;
    let operator_account_pda =
        solana_axelar_operators::OperatorAccount::find_pda(&operator.pubkey()).0;

    let init_operators_ix = Instruction {
        program_id: solana_axelar_operators::ID,
        accounts: solana_axelar_operators::accounts::Initialize {
            payer: payer.pubkey(),
            owner: operator.pubkey(),
            registry,
            system_program: SYSTEM_PROGRAM_ID,
        }
        .to_account_metas(None),
        data: solana_axelar_operators::instruction::Initialize {}.data(),
    };

    let add_operator_ix = Instruction {
        program_id: solana_axelar_operators::ID,
        accounts: solana_axelar_operators::accounts::AddOperator {
            owner: operator.pubkey(),
            operator_to_add: operator.pubkey(),
            registry,
            operator_account: operator_account_pda,
            system_program: SYSTEM_PROGRAM_ID,
        }
        .to_account_metas(None),
        data: solana_axelar_operators::instruction::AddOperator {}.data(),
    };

    let recent_blockhash = rpc_client.get_latest_blockhash().await.expect("blockhash");
    let tx = Transaction::new_signed_with_payer(
        &[init_operators_ix, add_operator_ix],
        Some(&payer.pubkey()),
        &[&payer, &operator],
        recent_blockhash,
    );
    rpc_client
        .send_and_confirm_transaction(&tx)
        .await
        .expect("Failed to initialize operators");

    // Initialize Gas Service
    let treasury = solana_axelar_gas_service::Treasury::find_pda().0;

    let init_gas_service_ix = Instruction {
        program_id: solana_axelar_gas_service::ID,
        accounts: solana_axelar_gas_service::accounts::Initialize {
            payer: payer.pubkey(),
            operator: operator.pubkey(),
            operator_pda: operator_account_pda,
            system_program: SYSTEM_PROGRAM_ID,
            treasury,
        }
        .to_account_metas(None),
        data: solana_axelar_gas_service::instruction::Initialize {}.data(),
    };

    let recent_blockhash = rpc_client.get_latest_blockhash().await.expect("blockhash");
    let tx = Transaction::new_signed_with_payer(
        &[init_gas_service_ix],
        Some(&payer.pubkey()),
        &[&payer, &operator],
        recent_blockhash,
    );
    rpc_client
        .send_and_confirm_transaction(&tx)
        .await
        .expect("Failed to initialize gas service");

    // Initialize Gateway
    let gateway_root_pda = solana_axelar_gateway::GatewayConfig::find_pda().0;
    let program_data =
        solana_sdk::bpf_loader_upgradeable::get_program_data_address(&solana_axelar_gateway::ID);

    let (_secret_key_1, compressed_pubkey_1) = generate_random_signer();
    let (_secret_key_2, compressed_pubkey_2) = generate_random_signer();

    let domain_separator = [2u8; 32];
    let quorum_threshold = 100;

    let verifier_leaves = [
        VerifierSetLeaf {
            nonce: 0,
            quorum: quorum_threshold,
            signer_pubkey: PublicKey(compressed_pubkey_1),
            signer_weight: 50,
            position: 0,
            set_size: 2,
            domain_separator,
        },
        VerifierSetLeaf {
            nonce: 0,
            quorum: quorum_threshold,
            signer_pubkey: PublicKey(compressed_pubkey_2),
            signer_weight: 50,
            position: 1,
            set_size: 2,
            domain_separator,
        },
    ];

    let verifier_leaf_hashes: Vec<[u8; 32]> =
        verifier_leaves.iter().map(VerifierSetLeaf::hash).collect();
    let verifier_merkle_tree = MerkleTree::from_leaves(&verifier_leaf_hashes);
    let verifier_set_hash = verifier_merkle_tree.root().expect("merkle root");
    let verifier_set_tracker_pda =
        solana_axelar_gateway::VerifierSetTracker::find_pda(&verifier_set_hash).0;

    let params = InitializeConfigParams {
        domain_separator,
        initial_verifier_set: InitialVerifierSet {
            hash: verifier_set_hash,
            pda: verifier_set_tracker_pda,
        },
        minimum_rotation_delay: 3600,
        operator: operator.pubkey(),
        previous_verifier_retention: U256::from(5u64),
    };

    let init_gateway_ix = Instruction {
        program_id: solana_axelar_gateway::ID,
        accounts: solana_axelar_gateway::accounts::InitializeConfig {
            payer: payer.pubkey(),
            upgrade_authority: upgrade_authority.pubkey(),
            system_program: SYSTEM_PROGRAM_ID,
            program_data,
            gateway_root_pda,
            verifier_set_tracker_pda,
        }
        .to_account_metas(None),
        data: solana_axelar_gateway::instruction::InitializeConfig { params }.data(),
    };

    let recent_blockhash = rpc_client.get_latest_blockhash().await.expect("blockhash");
    let tx = Transaction::new_signed_with_payer(
        &[init_gateway_ix],
        Some(&payer.pubkey()),
        &[&payer, &upgrade_authority],
        recent_blockhash,
    );
    rpc_client
        .send_and_confirm_transaction(&tx)
        .await
        .expect("Failed to initialize gateway");

    println!("Gateway initialized successfully");

    tokio::time::sleep(std::time::Duration::from_millis(2000)).await;

    let test_queue = Arc::new(TestQueue::new());
    let events_queue: Arc<dyn QueueTrait> = Arc::clone(&test_queue) as Arc<dyn QueueTrait>;

    let solana_rpc_client = SolanaRpcClient::new(&rpc_url, CommitmentConfig::confirmed(), 3)
        .expect("create rpc client");

    let poller = SolanaPoller::new(
        solana_rpc_client,
        "test_call_contract_poller".to_string(),
        Arc::new(transaction_model),
        Arc::new(postgres_db),
        events_queue,
    )
    .await
    .expect("Failed to create poller");

    let gateway_txs = poller
        .poll_account(solana_axelar_gateway::ID, AccountPollerEnum::Gateway)
        .await
        .expect("Failed to poll gateway");

    println!("Found {} gateway transaction(s)", gateway_txs.len());
    assert!(
        !gateway_txs.is_empty(),
        "Should find gateway initialization transaction"
    );

    // Verify the init transaction was found
    let has_gateway_init = gateway_txs.iter().any(|tx| {
        tx.logs
            .iter()
            .any(|log| log.contains("Instruction: InitializeConfig"))
    });
    assert!(
        has_gateway_init,
        "Gateway transactions should include InitializeConfig instruction"
    );

    // send call contract tx
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
        signing_pda_bump: 0, // Not used for direct signers
    };

    let mut accounts = solana_axelar_gateway::accounts::CallContract {
        caller: payer.pubkey(),
        signing_pda: None,
        gateway_root_pda,
        event_authority: event_authority_pda,
        program: GATEWAY_PROGRAM_ID,
    }
    .to_account_metas(None);

    // Mark the caller as a signer
    accounts[0].is_signer = true;

    let call_contract_instruction = Instruction {
        program_id: GATEWAY_PROGRAM_ID,
        accounts,
        data: call_contract_ix.data(),
    };

    let recent_blockhash = rpc_client.get_latest_blockhash().await.expect("blockhash");
    let call_contract_tx = Transaction::new_signed_with_payer(
        &[call_contract_instruction],
        Some(&payer.pubkey()),
        &[&payer],
        recent_blockhash,
    );

    let call_contract_signature = rpc_client
        .send_and_confirm_transaction(&call_contract_tx)
        .await
        .expect("Failed to send call_contract transaction");

    println!(
        "call_contract transaction sent: {}",
        call_contract_signature
    );

    tokio::time::sleep(std::time::Duration::from_millis(2000)).await;

    let gateway_txs_after_call = poller
        .poll_account(solana_axelar_gateway::ID, AccountPollerEnum::Gateway)
        .await
        .expect("Failed to poll gateway after call_contract");

    println!(
        "Found {} gateway transaction(s) after call_contract",
        gateway_txs_after_call.len()
    );

    // Check that we found the call_contract transaction in the polled transactions
    let has_call_contract = gateway_txs_after_call.iter().any(|tx| {
        tx.logs
            .iter()
            .any(|log| log.contains("Instruction: CallContract"))
    });
    assert!(
        has_call_contract,
        "Gateway transactions should include CallContract instruction after call"
    );

    let signatures: Vec<solana_sdk::signature::Signature> = gateway_txs_after_call
        .iter()
        .map(|tx| tx.signature)
        .collect();
    println!(
        "Recovering {} transaction(s) to publish to queue...",
        signatures.len()
    );

    poller
        .recover_txs(signatures)
        .await
        .expect("Failed to recover transactions");

    let queued_items = test_queue.get_items().await;
    println!("Queue contains {} items", queued_items.len());

    assert!(
        !queued_items.is_empty(),
        "Queue should have at least one entry after recovering transactions"
    );

    // Check if the CallContract transaction is in the queue
    let mut found_call_contract = false;
    for item in &queued_items {
        if let QueueItem::Transaction(tx_data) = item {
            let preview = if tx_data.len() > 200 {
                format!("{}...", &tx_data[..200])
            } else {
                tx_data.to_string()
            };
            println!("Queue item: {}", preview);
            if tx_data.contains("CallContract") {
                found_call_contract = true;
                println!("✓ Found CallContract in queue!");
            }
        }
    }

    assert!(
        found_call_contract,
        "CallContract transaction should be published to the queue"
    );

    println!("PgSolanaTransactionModelarsing transaction and extracting CallContract event");

    // Find the CallContract transaction in the queue
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

    // Create a TransactionParser with mock cost cache
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

    // Verify that we got a Call event
    let mut found_call_event = false;
    for event in &events {
        match event {
            Event::Call {
                common,
                message,
                destination_chain,
                payload,
            } => {
                println!("Found Call event!");
                println!("Event ID: {}", common.event_id);
                println!("Message ID: {}", message.message_id);
                println!("Source Chain: {}", message.source_chain);
                println!("Destination Chain: {}", destination_chain);
                println!("Destination Address: {}", message.destination_address);
                println!("Payload length: {} bytes", payload.len());
                found_call_event = true;

                assert_eq!(
                    destination_chain, "ethereum",
                    "Destination chain should be ethereum"
                );
            }
            _ => {
                println!("Other event type: {:?}", event);
            }
        }
    }

    assert!(
        found_call_event,
        "Ingestor should have extracted a Call event from the CallContract transaction"
    );

    // Create a mock GMP API that expects to be called with our event
    let mut mock_gmp_api = MockGmpApiTrait::new();

    let captured_event = std::sync::Arc::new(std::sync::Mutex::new(None::<Event>));
    let captured_event_clone = std::sync::Arc::clone(&captured_event);

    mock_gmp_api
        .expect_post_events()
        .times(1)
        .returning(move |events| {
            // Capture the first Call event
            for event in &events {
                if matches!(event, Event::Call { .. }) {
                    let mut captured = captured_event_clone.lock().unwrap();
                    *captured = Some(event.clone());
                    break;
                }
            }
            // Return success for each event
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

    // Verify the captured event is the CallContract we expected
    // Extract the event data in a block scope to drop the lock before await
    {
        let captured = captured_event.lock().unwrap();
        assert!(
            captured.is_some(),
            "GMP API should have been called with a Call event"
        );

        if let Some(Event::Call {
            common,
            message,
            destination_chain,
            ..
        }) = captured.as_ref()
        {
            println!("GMP API was called with CallContract event!");
            println!("Event ID: {}", common.event_id);
            println!("Message ID: {}", message.message_id);
            println!("Destination Chain: {}", destination_chain);
            assert_eq!(destination_chain, "ethereum");
            assert_eq!(message.destination_address, "0xDestinationContract");
        }
    }

    println!("Test completed successfully!");
    println!("call_contract transaction was sent to Solana");
    println!("Poller captured the transaction");
    println!("Transaction was published to the queue");
    println!("Ingestor parsed the transaction and extracted CallContract event");
    println!("CallContract event has correct destination chain (ethereum)");
    println!("Mock GMP API was called with the CallContract event");

    drop(test_validator);
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    cleanup_leftover_validators();
}
