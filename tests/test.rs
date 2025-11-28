//! Integration tests for Axelar Solana programs
//! To run:
//! cargo test --test test

#![allow(clippy::indexing_slicing)]

use std::path::PathBuf;
use std::sync::Arc;

use anchor_lang::{InstructionData, ToAccountMetas};
use relayer_core::queue::MockQueueTrait;
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
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres;

use solana_axelar_gateway::state::config::{InitialVerifierSet, InitializeConfigParams};
use solana_axelar_std::{hasher::LeafHash, MerkleTree, PublicKey, VerifierSetLeaf, U256};

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_happy_path() {
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
    ];
    for file in &program_files {
        let path = programs_path.join(file);
        if !path.exists() {
            panic!("Program file not found: {:?}", path);
        }
    }

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
    ]);

    let (test_validator, payer) = validator.start_async().await;
    let rpc_url = test_validator.rpc_url();

    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;

    let rpc_client = RpcClient::new_with_commitment(rpc_url.clone(), CommitmentConfig::confirmed());

    // Verify programs deployed
    let programs_to_check = [
        ("Gateway", solana_axelar_gateway::ID),
        ("Gas Service", solana_axelar_gas_service::ID),
        ("ITS", solana_axelar_its::ID),
        ("Governance", solana_axelar_governance::ID),
        ("Operators", solana_axelar_operators::ID),
    ];

    for (name, program_id) in programs_to_check {
        let account = rpc_client
            .get_account(&program_id)
            .await
            .unwrap_or_else(|e| panic!("Failed to get {} program account: {}", name, e));
        assert!(account.executable, "{} program should be executable", name);
    }

    println!("All programs initialized successfully");

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

    // Initialize ITS
    let its_root_pda = solana_axelar_its::InterchainTokenService::find_pda().0;
    let its_program_data =
        solana_sdk::bpf_loader_upgradeable::get_program_data_address(&solana_axelar_its::ID);
    let user_roles_pda =
        solana_axelar_its::UserRoles::find_pda(&its_root_pda, &operator.pubkey()).0;

    let init_its_ix = Instruction {
        program_id: solana_axelar_its::ID,
        accounts: solana_axelar_its::accounts::Initialize {
            payer: operator.pubkey(),
            program_data: its_program_data,
            its_root_pda,
            system_program: SYSTEM_PROGRAM_ID,
            operator: operator.pubkey(),
            user_roles_account: user_roles_pda,
        }
        .to_account_metas(None),
        data: solana_axelar_its::instruction::Initialize {
            chain_name: "solana".to_owned(),
            its_hub_address: "axelar123".to_owned(),
        }
        .data(),
    };

    let recent_blockhash = rpc_client.get_latest_blockhash().await.expect("blockhash");
    let tx = Transaction::new_signed_with_payer(
        &[init_its_ix],
        Some(&payer.pubkey()),
        &[&payer, &operator],
        recent_blockhash,
    );
    rpc_client
        .send_and_confirm_transaction(&tx)
        .await
        .expect("Failed to initialize ITS");

    println!("All programs initialized successfully");

    tokio::time::sleep(std::time::Duration::from_millis(2000)).await;

    let mock_queue = MockQueueTrait::new();
    let queue: Arc<dyn relayer_core::queue::QueueTrait> = Arc::new(mock_queue);

    let solana_rpc_client = SolanaRpcClient::new(&rpc_url, CommitmentConfig::confirmed(), 3)
        .expect("create rpc client");

    let poller = SolanaPoller::new(
        solana_rpc_client,
        "test_poller".to_string(),
        Arc::new(transaction_model),
        Arc::new(postgres_db),
        queue,
    )
    .await
    .expect("Failed to create poller");

    let gas_service_txs = poller
        .poll_account(solana_axelar_gas_service::ID, AccountPollerEnum::GasService)
        .await
        .expect("Failed to poll gas service");
    assert!(
        !gas_service_txs.is_empty(),
        "Should find gas service initialization transaction"
    );
    println!(
        "Found {} transaction(s) for Gas Service",
        gas_service_txs.len()
    );

    let has_gas_init = gas_service_txs.iter().any(|tx| {
        tx.logs
            .iter()
            .any(|log| log.contains("Instruction: Initialize"))
    });
    assert!(
        has_gas_init,
        "Gas Service transactions should include Initialize instruction"
    );

    let gateway_txs = poller
        .poll_account(solana_axelar_gateway::ID, AccountPollerEnum::Gateway)
        .await
        .expect("Failed to poll gateway");
    assert!(
        !gateway_txs.is_empty(),
        "Should find gateway initialization transaction"
    );
    println!("Found {} transaction(s) for Gateway", gateway_txs.len());

    let has_gateway_init = gateway_txs.iter().any(|tx| {
        tx.logs
            .iter()
            .any(|log| log.contains("Instruction: InitializeConfig"))
    });
    assert!(
        has_gateway_init,
        "Gateway transactions should include InitializeConfig instruction"
    );

    let its_txs = poller
        .poll_account(solana_axelar_its::ID, AccountPollerEnum::ITS)
        .await
        .expect("Failed to poll ITS");
    assert!(
        !its_txs.is_empty(),
        "Should find ITS initialization transaction"
    );
    println!("Found {} transaction(s) for ITS", its_txs.len());

    let has_its_init = its_txs.iter().any(|tx| {
        tx.logs
            .iter()
            .any(|log| log.contains("Instruction: Initialize"))
    });
    assert!(
        has_its_init,
        "ITS transactions should include Initialize instruction"
    );

    println!("Subscriber poller successfully found all initialization transactions!");

    drop(test_validator);
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    cleanup_leftover_validators();
}
