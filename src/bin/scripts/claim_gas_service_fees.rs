use anchor_lang::{InstructionData, ToAccountMetas};
use axelar_relayer_solana::config::SolanaConfig;
use axelar_relayer_solana::gas_calculator::GasCalculator;
use axelar_relayer_solana::includer_client::{IncluderClient, IncluderClientTrait};
use axelar_relayer_solana::redis::RedisConnection;
use axelar_relayer_solana::transaction_builder::{TransactionBuilder, TransactionBuilderTrait};
use axelar_relayer_solana::utils::get_gas_service_event_authority_pda;
use dotenv::dotenv;
use relayer_core::config::config_from_yaml;
use relayer_core::logging::setup_logging;
use relayer_core::redis::connection_manager;
use solana_sdk::instruction::Instruction;
use solana_sdk::signer::Signer;
use std::sync::Arc;
use tracing::info;

// Amount of lamports to claim from the gas service treasury to the operator's wallet
// Note: 1 SOL = 1_000_000_000
const CLAIM_AMOUNT_LAMPORTS: u64 = 100_000_000;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv().ok();
    let network = std::env::var("NETWORK").expect("NETWORK must be set");
    let config: SolanaConfig = config_from_yaml(&format!("config.{network}.yaml"))?;

    let (_sentry_guard, _otel_guard) = setup_logging(&config.common_config);

    let keypair = Arc::new(config.signing_keypair());
    let operator = keypair.pubkey();
    let receiver = operator;

    let client = Arc::new(
        IncluderClient::new(&config.solana_poll_rpc, config.solana_commitment(), 3)
            .map_err(|e| anyhow::anyhow!("Failed to create includer client: {}", e))?,
    );

    let redis_client = redis::Client::open(config.common_config.redis_server.clone())?;
    let redis_conn_manager = connection_manager(redis_client, None, None, None, None).await?;
    let redis_conn = RedisConnection::new(redis_conn_manager);

    let gas_calculator = GasCalculator::new(client.as_ref().clone());
    let transaction_builder = TransactionBuilder::new(
        Arc::clone(&keypair),
        gas_calculator,
        Arc::clone(&client),
        redis_conn.clone(),
    );

    let (operator_pda, _) = solana_axelar_operators::OperatorAccount::try_find_pda(&operator)
        .ok_or_else(|| anyhow::anyhow!("Failed to derive operator PDA"))?;
    let (treasury, _) = solana_axelar_gas_service::Treasury::try_find_pda()
        .ok_or_else(|| anyhow::anyhow!("Failed to derive treasury PDA"))?;
    let (event_authority, _) = get_gas_service_event_authority_pda()?;

    let accounts = solana_axelar_gas_service::accounts::CollectFees {
        operator,
        operator_pda,
        receiver,
        treasury,
        event_authority,
        program: solana_axelar_gas_service::ID,
    }
    .to_account_metas(None);

    let data = solana_axelar_gas_service::instruction::CollectFees {
        amount: CLAIM_AMOUNT_LAMPORTS,
    }
    .data();

    let ix = Instruction {
        program_id: solana_axelar_gas_service::ID,
        accounts,
        data,
    };

    let (tx, estimated_tx_cost) = transaction_builder
        .build(&[ix], vec![], None)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to build collect_fees transaction: {}", e))?;

    info!(
        operator = %operator,
        receiver = %receiver,
        amount_lamports = CLAIM_AMOUNT_LAMPORTS,
        estimated_tx_cost_lamports = estimated_tx_cost,
        "Submitting collect_fees transaction",
    );

    let (signature, _) = client
        .send_transaction(tx, None)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to send collect_fees transaction: {}", e))?;

    info!(%signature, "collect_fees transaction confirmed");

    Ok(())
}
