use crate::config::SolanaConfig;
use crate::gas_estimator::GasEstimator;
use crate::poll_client::SolanaRpcClientTrait;
use crate::wallet::Wallet;
use crate::{
    broadcaster::SolanaBroadcaster, poll_client::SolanaRpcClient,
    refund_manager::SolanaRefundManager,
};
use redis::aio::ConnectionManager;
use relayer_core::utils::ThreadSafe;
use relayer_core::{
    database::Database, error::BroadcasterError, gmp_api::GmpApiTrait, includer::Includer,
    includer_worker::IncluderWorker, payload_cache::PayloadCache, queue::Queue,
};
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use std::sync::Arc;

pub struct TONIncluder {}

impl TONIncluder {
    #[allow(clippy::new_ret_no_self)]
    pub async fn new<DB: Database + ThreadSafe + Clone, G: GmpApiTrait + ThreadSafe + Clone>(
        config: SolanaConfig,
        gmp_api: Arc<G>,
        redis_conn: ConnectionManager,
        payload_cache_for_includer: PayloadCache<DB>,
        construct_proof_queue: Arc<Queue>,
    ) -> error_stack::Result<
        Includer<
            SolanaBroadcaster<SolanaGasEstimator>,
            Arc<dyn SolanaRpcClientTrait>,
            SolanaRefundManager,
            DB,
            G,
        >,
        BroadcasterError,
    > {
        let solana_rpc = config.solana_poll_rpc;
        let solana_commitment = config.solana_commitment;
        let wallets = config.wallets;
        let solana_gateway = config.solana_gateway;
        let solana_gas_service = config.solana_gas_service;

        let wallet = Arc::new(
            Wallet::new(
                wallets
                    .first()
                    .ok_or(BroadcasterError::GenericError(
                        "No wallet found".to_string(),
                    ))
                    .map_err(|e| BroadcasterError::GenericError(e.to_string()))?
                    .clone(),
            )
            .await
            .map_err(|e| BroadcasterError::GenericError(e.to_string()))?,
        );

        let client: Arc<dyn SolanaRpcClientTrait> = Arc::new(
            SolanaRpcClient::new(&solana_rpc, solana_commitment, 5)
                .map_err(|e| error_stack::report!(BroadcasterError::GenericError(e.to_string())))?,
        );

        let gateway_address = Pubkey::from_str(&solana_gateway)
            .map_err(|e| BroadcasterError::GenericError(e.to_string()))?;
        let gas_service_address = Pubkey::from_str(&solana_gas_service)
            .map_err(|e| BroadcasterError::GenericError(e.to_string()))?;

        let broadcaster = SolanaBroadcaster::new(
            Arc::clone(&wallet),
            Arc::clone(&client),
            gateway_address,
            gas_service_address,
            config.common_config.chain_name,
            SolanaGasEstimator::new(config.gas_estimates.clone()),
        )
        .map_err(|e| BroadcasterError::GenericError(e.to_string()))?;

        let refund_manager = SolanaRefundManager::new()
            .map_err(|e| error_stack::report!(BroadcasterError::GenericError(e.to_string())))?;

        let worker = IncluderWorker::new(
            client,
            broadcaster,
            refund_manager,
            gmp_api,
            payload_cache_for_includer,
            construct_proof_queue,
            redis_conn,
        );

        let includer = Includer::new(worker);

        Ok(includer)
    }
}
