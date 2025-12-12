//! Integration tests
//! To run:
//! cargo test --test test

use std::path::PathBuf;

use solana_rpc::rpc::JsonRpcConfig;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::account::AccountSharedData;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::signature::{Keypair, Signer};
use solana_sdk_ids::bpf_loader_upgradeable;
use solana_system_interface::program::ID as SYSTEM_PROGRAM_ID;
use solana_test_validator::{TestValidatorGenesis, UpgradeableProgramInfo};

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

/// Start a test validator with the Axelar programs deployed and verify deployment succeeds.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_validator_program_deployment() {
    cleanup_leftover_validators();

    let upgrade_authority = Keypair::new();

    let mut validator = TestValidatorGenesis::default();

    let mut rpc_config = JsonRpcConfig::default_for_test();
    rpc_config.enable_rpc_transaction_history = true;
    rpc_config.enable_extended_tx_metadata_storage = true;
    validator.rpc_config(rpc_config);

    // Fund the upgrade authority account
    validator.add_account(
        upgrade_authority.pubkey(),
        AccountSharedData::new(u64::MAX, 0, &SYSTEM_PROGRAM_ID),
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
            upgrade_authority: upgrade_authority.pubkey(),
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
    let (test_validator, _payer) = validator.start_async().await;

    let rpc_url = test_validator.rpc_url();

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let rpc_client = RpcClient::new_with_commitment(rpc_url.clone(), CommitmentConfig::confirmed());

    // Verify each program is deployed by checking the account exists and is executable
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

    drop(test_validator);
    drop(_payer);

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    cleanup_leftover_validators();
}
