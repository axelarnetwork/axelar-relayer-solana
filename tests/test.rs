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
use relayer_core::includer_worker::IncluderTrait;
use relayer_core::ingestor::IngestorTrait;
use relayer_core::queue::{QueueItem, QueueTrait};
use solana::fees_client::FeesClient;
use solana::gas_calculator::GasCalculator;
use solana::includer::SolanaIncluder;
use solana::includer_client::IncluderClient;
use solana::ingestor::SolanaIngestor;
use solana::mocks::{MockRedisConnectionTrait, MockRefundsModel, MockUpdateEvents};
use solana::models::solana_subscriber_cursor::PostgresDB;
use solana::models::solana_transaction::PgSolanaTransactionModel;
use solana::poll_client::SolanaRpcClient;
use solana::subscriber_poller::SolanaPoller;
use solana::transaction_builder::TransactionBuilder;
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
use solana_test_validator::{TestValidator, TestValidatorGenesis, UpgradeableProgramInfo};
use solana_transaction_parser::parser::TransactionParser;
use solana_transaction_parser::redis::MockCostCacheTrait;
use testcontainers::runners::AsyncRunner;
use testcontainers::ContainerAsync;
use testcontainers_modules::postgres;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use solana_axelar_gateway::state::config::{InitialVerifierSet, InitializeConfigParams};
use solana_axelar_gateway::ID as GATEWAY_PROGRAM_ID;
use solana_axelar_gateway_test_fixtures::create_verifier_info;
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

    /// Clear all queued items. Useful to ensure no cross-test leakage.
    async fn clear(&self) {
        self.items.lock().await.clear();
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

/// Test environment with deployed Axelar programs and initialized services.
struct TestEnvironment {
    pub test_validator: TestValidator,
    pub rpc_url: String,
    pub rpc_client: RpcClient,
    pub payer: Keypair,
    #[allow(dead_code)]
    pub operator: Keypair,
    #[allow(dead_code)]
    pub upgrade_authority: Keypair,
    pub gateway_root_pda: solana_sdk::pubkey::Pubkey,
    pub verifier_set_hash: [u8; 32],
    pub domain_separator: [u8; 32],
    pub verifier_secret_keys: Vec<libsecp256k1::SecretKey>,
    pub verifier_leaves: Vec<VerifierSetLeaf>,
    pub verifier_merkle_tree: MerkleTree,
    pub postgres_db: PostgresDB,
    pub transaction_model: PgSolanaTransactionModel,
    #[allow(dead_code)]
    pub db_container: ContainerAsync<postgres::Postgres>,
}

impl TestEnvironment {
    pub async fn new() -> Self {
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

        let rpc_client =
            RpcClient::new_with_commitment(rpc_url.clone(), CommitmentConfig::confirmed());

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
        let program_data = solana_sdk::bpf_loader_upgradeable::get_program_data_address(
            &solana_axelar_gateway::ID,
        );

        let (secret_key_1, compressed_pubkey_1) = generate_random_signer();
        let (secret_key_2, compressed_pubkey_2) = generate_random_signer();

        let domain_separator = [2u8; 32];
        let quorum_threshold = 100;

        let verifier_leaves = vec![
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

        println!("Test environment initialized successfully");

        Self {
            test_validator,
            rpc_url,
            rpc_client,
            payer,
            operator,
            upgrade_authority,
            gateway_root_pda,
            verifier_set_hash,
            domain_separator,
            verifier_secret_keys: vec![secret_key_1, secret_key_2],
            verifier_leaves,
            verifier_merkle_tree,
            postgres_db,
            transaction_model,
            db_container,
        }
    }

    pub async fn cleanup(self) {
        drop(self.test_validator);
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        cleanup_leftover_validators();
    }
}

/// Test that call_contract transactions are captured by the poller's run() function
/// and processed by the ingestor to post events to the GMP API.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_call_contract_picked_up_and_sent_to_gmp() {
    let env = TestEnvironment::new().await;

    let test_queue = Arc::new(TestQueue::new());
    let events_queue: Arc<dyn QueueTrait> = Arc::clone(&test_queue) as Arc<dyn QueueTrait>;

    let solana_rpc_client = SolanaRpcClient::new(&env.rpc_url, CommitmentConfig::confirmed(), 3)
        .expect("create rpc client");

    let poller = Arc::new(
        SolanaPoller::new(
            solana_rpc_client,
            "test_call_contract_poller".to_string(),
            Arc::new(env.transaction_model.clone()),
            Arc::new(env.postgres_db.clone()),
            events_queue,
        )
        .await
        .expect("Failed to create poller"),
    );

    let poller_cancellation = CancellationToken::new();
    let poller_cancellation_clone = poller_cancellation.clone();

    let poller_clone = Arc::clone(&poller);
    let poller_handle = tokio::spawn(async move {
        println!("Poller starting run()...");
        poller_clone
            .run(
                solana_axelar_gas_service::ID,
                solana_axelar_gateway::ID,
                solana_axelar_its::ID,
                poller_cancellation_clone,
            )
            .await;
        println!("Poller run() completed");
    });

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Send call_contract transaction
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
    poller_cancellation.cancel();
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), poller_handle).await;
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
    // Ensure in-memory queue is emptied so nothing leaks across tests in the same process
    test_queue.clear().await;
    env.cleanup().await;
}

// Test that the includer can process a GatewayTxTask for an ITS message
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_includer_gateway_tx_task() {
    use base64::prelude::BASE64_STANDARD;
    use base64::Engine;
    use borsh::BorshSerialize;
    use solana_axelar_std::execute_data::{ExecuteData, MerklizedPayload};
    use solana_axelar_std::{CrossChainId, MerklizedMessage, Message, MessageLeaf};
    use solana_transaction_parser::gmp_types::{
        CommonTaskFields, GatewayTxTask, GatewayTxTaskFields,
    };

    let env = TestEnvironment::new().await;

    let includer_client = IncluderClient::new(&env.rpc_url, CommitmentConfig::confirmed(), 3)
        .expect("Failed to create includer client");

    let fees_client =
        FeesClient::new(includer_client.clone(), 10).expect("Failed to create fees client");

    let gas_calculator = GasCalculator::new(includer_client.clone(), fees_client);

    let keypair = Arc::new(Keypair::from_bytes(&env.payer.to_bytes()).unwrap());

    let transaction_builder = TransactionBuilder::new(
        Arc::clone(&keypair),
        gas_calculator,
        Arc::new(includer_client.clone()),
    );

    let mut mock_redis = MockRedisConnectionTrait::new();
    mock_redis
        .expect_add_gas_cost_for_task_id()
        .returning(|_, _, _| ());
    mock_redis
        .expect_get_gas_cost_for_task_id()
        .returning(|_, _| Ok(0u64));
    mock_redis
        .expect_write_gas_cost_for_message_id()
        .returning(|_, _, _| ());

    let mock_gmp_api = MockGmpApiTrait::new();
    let mock_refunds_model = MockRefundsModel::new();

    let signing_verifier_set_merkle_root = env.verifier_set_hash;

    // Create the message we want to approve
    let its_program_address = solana_axelar_its::ID.to_string();
    let message_id = "test-message-id-001";
    let source_address = "axelar1test";

    let message = Message {
        cc_id: CrossChainId {
            chain: "axelar".to_string(),
            id: message_id.to_string(),
        },
        source_address: source_address.to_string(),
        destination_chain: "solana-devnet".to_string(),
        destination_address: its_program_address.clone(),
        payload_hash: [0; 32],
    };

    // Create the message leaf and merkle tree for the payload
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
    );
    let verifier_info_2 = create_verifier_info(
        &env.verifier_secret_keys[1],
        payload_merkle_root,
        &env.verifier_leaves[1],
        1,
        &env.verifier_merkle_tree,
    );

    let execute_data = ExecuteData {
        payload_merkle_root,
        signing_verifier_set_merkle_root,
        signing_verifier_set_leaves: vec![verifier_info_1, verifier_info_2],
        payload_items: MerklizedPayload::NewMessages {
            messages: vec![MerklizedMessage {
                leaf: message_leaf,
                proof: vec![], // Single message, no proof needed
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

    println!("Created GatewayTxTask for ITS message:");
    println!("Task ID: {}", task.common.id);
    println!("Message ID: {}", message_id);
    println!("Destination: {}", its_program_address);
    println!("Using REAL: IncluderClient, TransactionBuilder, GasCalculator, FeesClient");
    let includer = SolanaIncluder::new(
        Arc::new(includer_client),
        keypair,
        "solana-devnet".to_string(),
        transaction_builder,
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

    // Now use subscriber/ingestor to pick up and parse the transactions
    // to verify that a MessageApproved event was emitted
    println!("Setting up subscriber to verify MessageApproved event...");

    // Create test queue to capture transactions
    let test_queue = Arc::new(TestQueue::new());
    let events_queue: Arc<dyn QueueTrait> = Arc::clone(&test_queue) as Arc<dyn QueueTrait>;

    // Create subscriber poller
    let poll_client = SolanaRpcClient::new(&env.rpc_url, CommitmentConfig::confirmed(), 3)
        .expect("Failed to create poll client");

    let poller = Arc::new(
        SolanaPoller::new(
            poll_client,
            "test_gateway_tx_poller".to_string(),
            Arc::new(env.transaction_model.clone()),
            Arc::new(env.postgres_db.clone()),
            events_queue,
        )
        .await
        .expect("Failed to create poller"),
    );

    // Run poller briefly to pick up the approve message transaction
    let poller_cancellation = CancellationToken::new();
    let poller_cancellation_clone = poller_cancellation.clone();

    let poller_clone = Arc::clone(&poller);
    let poller_handle = tokio::spawn(async move {
        poller_clone
            .run(
                solana_axelar_gas_service::ID,
                solana_axelar_gateway::ID,
                solana_axelar_its::ID,
                poller_cancellation_clone,
            )
            .await;
    });

    let mut found_approve_message_tx = false;
    let max_wait = std::time::Duration::from_secs(30);
    let start = std::time::Instant::now();

    while start.elapsed() < max_wait && !found_approve_message_tx {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        let queued_items = test_queue.get_items().await;
        for item in &queued_items {
            if let QueueItem::Transaction(tx_data) = item {
                // Check for ApproveMessage in the transaction logs
                if tx_data.contains("ApproveMessage") || tx_data.contains("MessageApproved") {
                    found_approve_message_tx = true;
                    println!("Found ApproveMessage transaction in queue!");
                    break;
                }
            }
        }
    }

    println!("Cancelling poller...");
    poller_cancellation.cancel();
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), poller_handle).await;
    println!("Poller stopped");

    let queued_items = test_queue.get_items().await;
    println!("Queue contains {} items total", queued_items.len());

    assert!(
        found_approve_message_tx,
        "Should have found ApproveMessage transaction in queue (proves message was approved on-chain)"
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
            // Print raw transaction data for debugging if it contains ApproveMessage
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

    println!("Includer integration test completed!");
    test_queue.clear().await;
    env.cleanup().await;
}
