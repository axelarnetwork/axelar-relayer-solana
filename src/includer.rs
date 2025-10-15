use crate::config::SolanaConfig;
use crate::gas_estimator::GasEstimator;
use crate::includer_client::{IncluderClient, IncluderClientTrait};
use crate::transaction_builder::{TransactionBuilder, TransactionBuilderTrait};
use crate::{broadcaster::SolanaBroadcaster, refund_manager::SolanaRefundManager};
use redis::aio::ConnectionManager;
use relayer_core::utils::ThreadSafe;
use relayer_core::{
    database::Database, error::BroadcasterError, gmp_api::GmpApiTrait, includer::Includer,
    includer_worker::IncluderWorker, payload_cache::PayloadCache, queue::Queue,
};
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use std::sync::Arc;

pub struct SolanaIncluder {}

impl SolanaIncluder {
    #[allow(clippy::new_ret_no_self)]
    pub async fn new<
        DB: Database + ThreadSafe + Clone,
        G: GmpApiTrait + ThreadSafe + Clone,
        IC: IncluderClientTrait + ThreadSafe + Clone,
        TB: TransactionBuilderTrait + ThreadSafe + Clone,
    >(
        config: SolanaConfig,
        gmp_api: Arc<G>,
        redis_conn: ConnectionManager,
        payload_cache_for_includer: PayloadCache<DB>,
        construct_proof_queue: Arc<Queue>,
    ) -> error_stack::Result<
        Includer<
            SolanaBroadcaster<
                TransactionBuilder<GasEstimator<IncluderClient>, IncluderClient>,
                IncluderClient,
            >,
            Arc<IncluderClient>,
            SolanaRefundManager,
            DB,
            G,
        >,
        BroadcasterError,
    > {
        let solana_rpc = config.solana_poll_rpc.clone();
        let solana_commitment = config.solana_commitment.clone();
        let solana_gateway = config.solana_gateway.clone();
        let solana_gas_service = config.solana_gas_service.clone();

        let client = Arc::new(
            IncluderClient::new(&solana_rpc, solana_commitment, 3)
                .map_err(|e| error_stack::report!(BroadcasterError::GenericError(e.to_string())))?,
        );

        let gateway_address = Pubkey::from_str(&solana_gateway)
            .map_err(|e| BroadcasterError::GenericError(e.to_string()))?;
        let gas_service_address = Pubkey::from_str(&solana_gas_service)
            .map_err(|e| BroadcasterError::GenericError(e.to_string()))?;

        let keypair = Arc::new(config.signing_keypair());

        let gas_estimator = GasEstimator::new(client.as_ref().clone(), Arc::clone(&keypair));

        let transaction_builder = TransactionBuilder::new(
            Arc::clone(&keypair),
            client.as_ref().clone(),
            gateway_address,
            gas_service_address,
            config.common_config.chain_name.clone(),
            gas_estimator,
        );

        let broadcaster = SolanaBroadcaster::new(
            Arc::clone(&client),
            Arc::clone(&keypair),
            gateway_address,
            gas_service_address,
            config.common_config.chain_name,
            transaction_builder,
            3,
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
