//! Common test utilities and setup for integration tests

use std::path::PathBuf;
use std::sync::Arc;

use anchor_lang::{AnchorSerialize, Discriminator, InstructionData, ToAccountMetas};
use async_trait::async_trait;
use relayer_core::gmp_api::MockGmpApiTrait;
use relayer_core::queue::{QueueItem, QueueTrait};
use solana::gas_calculator::GasCalculator;
use solana::includer_client::IncluderClient;
use solana::mocks::MockRedisConnectionTrait;
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
use testcontainers::runners::AsyncRunner;
use testcontainers::ContainerAsync;
use testcontainers_modules::postgres;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use solana_axelar_gateway::state::config::{InitialVerifierSet, InitializeConfigParams};
use solana_axelar_std::{hasher::LeafHash, MerkleTree, PublicKey, VerifierSetLeaf, U256};

#[cfg(test)]
pub struct TestQueue {
    items: Mutex<Vec<QueueItem>>,
}

#[cfg(test)]
impl TestQueue {
    pub fn new() -> Self {
        Self {
            items: Mutex::new(Vec::new()),
        }
    }

    pub async fn get_items(&self) -> Vec<QueueItem> {
        self.items.lock().await.clone()
    }

    pub async fn clear(&self) {
        self.items.lock().await.clear()
    }
}

#[cfg(test)]
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

#[cfg(test)]
pub fn programs_dir() -> PathBuf {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")
        .unwrap_or_else(|_| env!("CARGO_MANIFEST_DIR").to_owned());
    PathBuf::from(manifest_dir)
        .join("tests")
        .join("testdata")
        .join("programs")
}

#[cfg(test)]
pub fn cleanup_leftover_validators() {
    use std::process::Command;
    let _ = Command::new("pkill")
        .args(["-9", "-f", "solana-test-validator"])
        .output();
    std::thread::sleep(std::time::Duration::from_millis(500));
}

/// Generate a random secp256k1 signer for gateway verification
#[cfg(test)]
pub fn generate_random_signer() -> (libsecp256k1::SecretKey, [u8; 33]) {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let secret_key_bytes: [u8; 32] = rng.gen();
    let secret_key = libsecp256k1::SecretKey::parse(&secret_key_bytes).expect("valid secret key");
    let public_key = libsecp256k1::PublicKey::from_secret_key(&secret_key);
    let compressed_pubkey = public_key.serialize_compressed();
    (secret_key, compressed_pubkey)
}

#[cfg(test)]
pub fn create_mock_redis() -> MockRedisConnectionTrait {
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
    mock_redis.expect_get_alt_entry().returning(|_| Ok(None));
    mock_redis
        .expect_write_alt_entry()
        .returning(|_, _, _| Ok(()));
    mock_redis
        .expect_get_cu_price()
        .times(..)
        .returning(|| Ok(Some(100_000u64)));
    mock_redis
}

#[cfg(test)]
pub fn create_mock_gmp_api_for_execute() -> MockGmpApiTrait {
    use relayer_core::gmp_api::gmp_types::Event;
    let mut mock_gmp_api = MockGmpApiTrait::new();
    mock_gmp_api
        .expect_execute_message()
        .returning(|msg_id, source_chain, status, cost| {
            println!(
                "GMP API execute_message called: msg_id={}, source_chain={}, status={:?}, cost={:?}",
                msg_id, source_chain, status, cost
            );
            Event::MessageExecuted {
                common: solana_transaction_parser::gmp_types::CommonEventFields {
                    r#type: "MESSAGE_EXECUTED".to_string(),
                    event_id: "mock-event-id".to_string(),
                    meta: None,
                },
                message_id: msg_id,
                source_chain,
                status,
                cost,
            }
        });
    mock_gmp_api
}

#[cfg(test)]
pub struct IncluderComponents {
    pub includer_client: IncluderClient,
    pub keypair: Arc<Keypair>,
    pub transaction_builder:
        TransactionBuilder<GasCalculator<IncluderClient>, IncluderClient, MockRedisConnectionTrait>,
}

#[cfg(test)]
pub fn create_includer_components(rpc_url: &str, payer: &Keypair) -> IncluderComponents {
    create_includer_components_with_redis(rpc_url, payer, None)
}

#[cfg(test)]
pub fn create_includer_components_with_redis(
    rpc_url: &str,
    payer: &Keypair,
    mock_redis: Option<MockRedisConnectionTrait>,
) -> IncluderComponents {
    let includer_client = IncluderClient::new(rpc_url, CommitmentConfig::confirmed(), 3)
        .expect("Failed to create includer client");

    let gas_calculator = GasCalculator::new(includer_client.clone());

    let keypair = Arc::new(Keypair::from_bytes(&payer.to_bytes()).unwrap());

    let mock_redis = mock_redis.unwrap_or_else(create_mock_redis);

    let transaction_builder = TransactionBuilder::new(
        Arc::clone(&keypair),
        gas_calculator,
        Arc::new(includer_client.clone()),
        mock_redis,
    );

    IncluderComponents {
        includer_client,
        keypair,
        transaction_builder,
    }
}

pub struct PollerHandle {
    #[allow(dead_code)]
    pub poller: Arc<SolanaPoller<SolanaRpcClient, PostgresDB, PgSolanaTransactionModel>>,
    pub cancellation: CancellationToken,
    pub handle: tokio::task::JoinHandle<()>,
}

impl PollerHandle {
    pub async fn stop(self) {
        self.cancellation.cancel();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), self.handle).await;
    }
}

pub async fn spawn_poller(
    rpc_url: &str,
    name: &str,
    transaction_model: &PgSolanaTransactionModel,
    postgres_db: &PostgresDB,
    events_queue: Arc<dyn QueueTrait>,
) -> PollerHandle {
    let poll_client = SolanaRpcClient::new(rpc_url, CommitmentConfig::confirmed(), 3)
        .expect("Failed to create poll client");

    let poller = Arc::new(
        SolanaPoller::new(
            poll_client,
            name.to_string(),
            Arc::new(transaction_model.clone()),
            Arc::new(postgres_db.clone()),
            events_queue,
        )
        .await
        .expect("Failed to create poller"),
    );

    let cancellation = CancellationToken::new();
    let cancellation_clone = cancellation.clone();

    let poller_clone = Arc::clone(&poller);
    let handle = tokio::spawn(async move {
        poller_clone
            .run(
                solana_axelar_gas_service::ID,
                solana_axelar_gateway::ID,
                solana_axelar_its::ID,
                cancellation_clone,
            )
            .await;
    });

    PollerHandle {
        poller,
        cancellation,
        handle,
    }
}

/// Test environment with deployed Axelar programs and initialized services.
pub struct TestEnvironment {
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
    pub memo_program_id: solana_sdk::pubkey::Pubkey,
    #[allow(dead_code)]
    pub db_container: ContainerAsync<postgres::Postgres>,
    pub its_hub_address: String,
}

impl TestEnvironment {
    pub async fn new() -> Self {
        cleanup_leftover_validators();

        let init_sql = format!(
            "{}\n{}\n{}\n",
            include_str!("../../migrations/0006_solana_transactions.sql"),
            include_str!("../../migrations/0007_solana_subscriber_cursors.sql"),
            include_str!("../../migrations/0008_solana_refunds.sql"),
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
            "mpl_token_metadata.so",
        ];
        for file in &program_files {
            let path = programs_path.join(file);
            if !path.exists() {
                panic!("Program file not found: {:?}", path);
            }
        }

        let memo_program_id = solana_axelar_memo::ID;

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
            UpgradeableProgramInfo {
                program_id: mpl_token_metadata::ID,
                loader: bpf_loader_upgradeable::id(),
                upgrade_authority: upgrade_authority.pubkey(),
                program_path: programs_path.join("mpl_token_metadata.so"),
            },
        ]);

        // Pre-initialize ITS state during genesis to bypass the program_data deserialization issue
        let its_hub_address =
            "axelar157hl7gpuknjmhtac2qnphuazv2yerfagva7lsu9vuj2pgn32z22qa26dk4".to_string();
        let its_chain_name = "solana-devnet".to_string();
        let (its_root_pda, its_root_bump) = solana_axelar_its::InterchainTokenService::find_pda();

        // Create ITS root PDA account data with Anchor format
        {
            // InterchainTokenService state
            let its_state = solana_axelar_its::InterchainTokenService {
                its_hub_address: its_hub_address.clone(),
                chain_name: its_chain_name.clone(),
                paused: false,
                trusted_chains: vec!["axelar".to_string()], // Pre-add ethereum as trusted
                bump: its_root_bump,
            };

            // Serialize with Anchor format: 8-byte discriminator + Borsh data
            let mut data = Vec::new();
            data.extend_from_slice(solana_axelar_its::InterchainTokenService::DISCRIMINATOR);
            its_state
                .serialize(&mut data)
                .expect("Failed to serialize ITS state");

            let lamports = solana_sdk::rent::Rent::default().minimum_balance(data.len());
            validator.add_account(
                its_root_pda,
                AccountSharedData::from(solana_sdk::account::Account {
                    lamports,
                    data,
                    owner: solana_axelar_its::ID,
                    executable: false,
                    rent_epoch: 0,
                }),
            );
        }

        // Create UserRoles account for operator
        {
            let (user_roles_pda, user_roles_bump) =
                solana_axelar_its::state::UserRoles::find_pda(&its_root_pda, &operator.pubkey());

            let user_roles_state = solana_axelar_its::state::UserRoles {
                roles: solana_axelar_its::state::Roles::OPERATOR,
                bump: user_roles_bump,
            };

            let mut data = Vec::new();
            data.extend_from_slice(solana_axelar_its::state::UserRoles::DISCRIMINATOR);
            user_roles_state
                .serialize(&mut data)
                .expect("Failed to serialize UserRoles state");

            let lamports = solana_sdk::rent::Rent::default().minimum_balance(data.len());
            validator.add_account(
                user_roles_pda,
                AccountSharedData::from(solana_sdk::account::Account {
                    lamports,
                    data,
                    owner: solana_axelar_its::ID,
                    executable: false,
                    rent_epoch: 0,
                }),
            );
        }

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
            minimum_rotation_delay: 0,
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
            memo_program_id,
            db_container,
            its_hub_address,
        }
    }

    pub async fn cleanup(self) {
        drop(self.rpc_client);
        drop(self.postgres_db);
        drop(self.transaction_model);
        drop(self.test_validator);
        cleanup_leftover_validators();
        drop(self.db_container);
    }
}
