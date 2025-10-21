use crate::config::SolanaConfig;
use crate::error::IncluderClientError;
use crate::gas_calculator::GasCalculator;
use crate::includer_client::{IncluderClient, IncluderClientTrait};
use crate::refund_manager::SolanaRefundManager;
use crate::transaction_builder::{TransactionBuilder, TransactionBuilderTrait};
use crate::utils::{
    calculate_total_cost_lamports, get_cannot_execute_events_from_execute_data,
    get_gateway_event_authority_pda, get_incoming_message_pda, get_signature_verification_pda,
    get_verifier_set_tracker_pda,
};
use crate::v2_program_types::{ExecuteData, MerkleisedPayload};
use anchor_lang::{InstructionData, ToAccountMetas};
use async_trait::async_trait;
use axelar_solana_gateway_v2::{CrossChainId, Message};
use base64::Engine as _;
use borsh::BorshDeserialize;
use futures::stream::FuturesUnordered;
use futures::StreamExt as _;
use redis::aio::ConnectionManager;
use relayer_core::error::IncluderError;
use relayer_core::includer_worker::IncluderTrait;
use relayer_core::utils::ThreadSafe;
use relayer_core::{
    database::Database, gmp_api::GmpApiTrait, includer::Includer, includer_worker::IncluderWorker,
    payload_cache::PayloadCache, queue::Queue,
};
use solana_sdk::instruction::Instruction;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signer::{keypair::Keypair, Signer};
use solana_transaction_parser::gmp_types::{
    CannotExecuteMessageReason, Event, ExecuteTask, GatewayTxTask, RefundTask,
};
use std::str::FromStr;
use std::sync::Arc;
use tracing::{debug, info, warn};

#[derive(Clone)]
pub struct SolanaIncluder<G: GmpApiTrait + ThreadSafe + Clone> {
    client: Arc<IncluderClient>,
    keypair: Arc<Keypair>,
    gateway_address: Pubkey,
    _gas_service_address: Pubkey,
    chain_name: String,
    transaction_builder: TransactionBuilder<GasCalculator<IncluderClient>>,
    max_retries: usize,
    _config: SolanaConfig,
    gmp_api: Arc<G>,
}

impl<G: GmpApiTrait + ThreadSafe + Clone> SolanaIncluder<G> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        client: Arc<IncluderClient>,
        keypair: Arc<Keypair>,
        gateway_address: Pubkey,
        gas_service_address: Pubkey,
        chain_name: String,
        transaction_builder: TransactionBuilder<GasCalculator<IncluderClient>>,
        max_retries: usize,
        config: SolanaConfig,
        gmp_api: Arc<G>,
    ) -> Self {
        Self {
            client,
            keypair,
            gateway_address,
            _gas_service_address: gas_service_address,
            chain_name,
            transaction_builder,
            max_retries,
            _config: config,
            gmp_api,
        }
    }

    pub async fn create_includer<
        DB: Database + ThreadSafe + Clone,
        GMP: GmpApiTrait + ThreadSafe + Clone,
    >(
        config: SolanaConfig,
        gmp_api: Arc<GMP>,
        redis_conn: ConnectionManager,
        payload_cache_for_includer: PayloadCache<DB>,
        construct_proof_queue: Arc<Queue>,
    ) -> error_stack::Result<
        Includer<Arc<IncluderClient>, SolanaRefundManager, DB, GMP, SolanaIncluder<GMP>>,
        IncluderError,
    > {
        let solana_rpc = config.solana_poll_rpc.clone();
        let solana_commitment = config.solana_commitment;
        let solana_gateway = config.solana_gateway.clone();
        let solana_gas_service = config.solana_gas_service.clone();

        let client = Arc::new(
            IncluderClient::new(&solana_rpc, solana_commitment, 3)
                .map_err(|e| error_stack::report!(IncluderError::GenericError(e.to_string())))?,
        );

        let gateway_address = Pubkey::from_str(&solana_gateway)
            .map_err(|e| IncluderError::GenericError(e.to_string()))?;
        let gas_service_address = Pubkey::from_str(&solana_gas_service)
            .map_err(|e| IncluderError::GenericError(e.to_string()))?;

        let keypair = Arc::new(config.signing_keypair());

        let gas_calculator = GasCalculator::new(client.as_ref().clone(), Arc::clone(&keypair));

        let transaction_builder = TransactionBuilder::new(Arc::clone(&keypair), gas_calculator);

        let solana_includer = SolanaIncluder::new(
            Arc::clone(&client),
            Arc::clone(&keypair),
            gateway_address,
            gas_service_address,
            config.common_config.chain_name.clone(),
            transaction_builder,
            3,
            config,
            Arc::clone(&gmp_api),
        );

        let refund_manager = SolanaRefundManager::new()
            .map_err(|e| error_stack::report!(IncluderError::GenericError(e.to_string())))?;

        let worker = IncluderWorker::new(
            client,
            refund_manager,
            gmp_api,
            payload_cache_for_includer,
            construct_proof_queue,
            redis_conn,
            solana_includer,
        );

        let includer = Includer::new(worker);

        Ok(includer)
    }

    async fn send_to_chain(
        &self,
        ix: Instruction,
        message_id: Option<String>,
        source_chain: Option<String>,
    ) -> SendToChainResult {
        let mut retries: usize = 0;
        loop {
            let tx_res = self.transaction_builder.build(ix.clone()).await;

            let tx = match tx_res {
                Ok(tx) => tx,
                Err(e) => {
                    return SendToChainResult {
                        tx_hash: None,
                        status: Err(IncluderError::GenericError(e.to_string())),
                        message_id,
                        source_chain,
                    }
                }
            };

            let res = self.client.send_transaction(tx).await;
            match res {
                Ok(signature) => {
                    let tx_hash = signature.to_string();
                    debug!("Transaction sent successfully: {}", tx_hash);

                    return SendToChainResult {
                        tx_hash: Some(tx_hash),
                        status: Ok(()),
                        message_id,
                        source_chain,
                    };
                }
                Err(e) => {
                    match e {
                        IncluderClientError::GasExceededError(e) => {
                            // handle gas exceeded error
                            return SendToChainResult {
                                tx_hash: None,
                                status: Err(IncluderError::RPCError(e.to_string())),
                                message_id,
                                source_chain,
                            };
                        }
                        _ => {
                            retries += 1;
                            if retries >= self.max_retries {
                                return SendToChainResult {
                                    tx_hash: None,
                                    status: Err(IncluderError::RPCError(e.to_string())),
                                    message_id,
                                    source_chain,
                                };
                            }
                            warn!(
                                "Transaction failed, retrying ({}/{}): {}",
                                retries, self.max_retries, e
                            );
                        }
                    }
                }
            }
        }
    }
}

struct SendToChainResult {
    tx_hash: Option<String>,
    status: Result<(), IncluderError>,
    message_id: Option<String>,
    source_chain: Option<String>,
}

#[async_trait]
impl<G: GmpApiTrait + ThreadSafe + Clone> IncluderTrait for SolanaIncluder<G> {
    #[tracing::instrument(skip(self), fields(message_id))]
    async fn handle_gateway_tx_task(
        &self,
        task: GatewayTxTask,
    ) -> Result<Vec<Event>, IncluderError> {
        let execute_data_bytes = hex::encode(
            base64::prelude::BASE64_STANDARD
                .decode(task.task.execute_data)
                .map_err(|e| IncluderError::GenericError(e.to_string()))?,
        )
        .into_bytes();

        let execute_data = ExecuteData::try_from_slice(&execute_data_bytes).map_err(|_err| {
            IncluderError::GenericError("cannot decode execute data".to_string())
        })?;

        let (verification_session_tracker_pda, _) =
            get_signature_verification_pda(&execute_data.payload_merkle_root);

        let ix_data = axelar_solana_gateway_v2::instruction::InitializePayloadVerificationSession {
            signing_verifier_set_hash: execute_data.signing_verifier_set_merkle_root,
            merkle_root: execute_data.payload_merkle_root,
        }
        .data();

        let (verifier_set_tracker_pda, _) =
            get_verifier_set_tracker_pda(execute_data.signing_verifier_set_merkle_root);

        let accounts = axelar_solana_gateway_v2::accounts::InitializePayloadVerificationSession {
            verifier_set_tracker_pda,
            payer: self.keypair.pubkey(),
            gateway_root_pda: self.gateway_address,
            verification_session_account: verification_session_tracker_pda,
            system_program: solana_program::system_program::id(),
        };

        let ix = Instruction {
            program_id: axelar_solana_gateway_v2::ID,
            accounts: accounts.to_account_metas(None),
            data: ix_data,
        };

        let send_to_chain_res = self.send_to_chain(ix, None, None).await;
        if let Err(e) = send_to_chain_res.status {
            return get_cannot_execute_events_from_execute_data(
                &execute_data,
                CannotExecuteMessageReason::Error,
                e.to_string(),
                task.common.id.clone(),
                Arc::clone(&self.gmp_api),
            )
            .await
            .map_err(|e| IncluderError::GenericError(e.to_string()));
        }
        debug!(
            "Transaction for initializing payload verification session successfully: {}",
            send_to_chain_res.tx_hash.unwrap_or("".to_string())
        );

        let verifier_set_tracker_pda =
            get_verifier_set_tracker_pda(execute_data.signing_verifier_set_merkle_root).0;

        // verify each signature in the signing session
        let signing_verifier_set_leaves = execute_data.signing_verifier_set_leaves.clone();
        let mut verifier_ver_future_set = signing_verifier_set_leaves
            .into_iter()
            .map(|verifier_info| {
                let ix_data = axelar_solana_gateway_v2::instruction::VerifySignature {
                    signing_verifier_set_hash: execute_data.signing_verifier_set_merkle_root,
                    payload_merkle_root: execute_data.payload_merkle_root,
                    verifier_info,
                }
                .data();

                let accounts = axelar_solana_gateway_v2::accounts::VerifySignature {
                    gateway_root_pda: self.gateway_address,
                    verification_session_account: verification_session_tracker_pda,
                    verifier_set_tracker_pda,
                };
                let ix = Instruction {
                    program_id: axelar_solana_gateway_v2::ID,
                    accounts: accounts.to_account_metas(None),
                    data: ix_data,
                };

                self.send_to_chain(ix, None, None)
            })
            .collect::<FuturesUnordered<_>>();
        while let Some(result) = verifier_ver_future_set.next().await {
            if let Err(e) = result.status {
                return get_cannot_execute_events_from_execute_data(
                    &execute_data,
                    CannotExecuteMessageReason::Error,
                    e.to_string(),
                    task.common.id.clone(),
                    Arc::clone(&self.gmp_api),
                )
                .await
                .map_err(|e| IncluderError::GenericError(e.to_string()));
            }
        }

        let (event_authority, _) = get_gateway_event_authority_pda();

        match execute_data.payload_items {
            MerkleisedPayload::VerifierSetRotation {
                new_verifier_set_merkle_root,
            } => {
                let (new_verifier_set_tracker_pda, _) =
                    get_verifier_set_tracker_pda(new_verifier_set_merkle_root);
                let ix_data = axelar_solana_gateway_v2::instruction::RotateSigners {
                    new_verifier_set_merkle_root,
                }
                .data();
                let accounts = axelar_solana_gateway_v2::accounts::RotateSigners {
                    payer: self.keypair.pubkey(),
                    program: axelar_solana_gateway_v2::ID,
                    system_program: solana_program::system_program::id(),
                    gateway_root_pda: self.gateway_address,
                    verifier_set_tracker_pda,
                    operator: Some(self.keypair.pubkey()),
                    new_verifier_set_tracker: new_verifier_set_tracker_pda,
                    verification_session_account: verification_session_tracker_pda,
                    event_authority,
                };
                let ix = Instruction {
                    program_id: axelar_solana_gateway_v2::ID,
                    accounts: accounts.to_account_metas(None),
                    data: ix_data,
                };
                match self.send_to_chain(ix, None, None).await.status {
                    Ok(_) => {
                        debug!("Rotated signers transaction sent successfully");
                        // For verifier set rotation, we don't have messages to process, so return empty
                        return Ok(vec![]);
                    }
                    Err(e) => {
                        return Err(IncluderError::GatewayTxTaskError(e.to_string()));
                    }
                }
            }
            MerkleisedPayload::NewMessages { messages } => {
                let mut merkelised_message_futures = messages
                    .into_iter()
                    .map(|merkleised_message| {
                        let command_id = merkleised_message.leaf.message.command_id();
                        let (pda, _) = get_incoming_message_pda(&command_id);

                        let msg_id = merkleised_message.leaf.message.cc_id.id.clone();
                        let chain = merkleised_message.leaf.message.cc_id.chain.clone();

                        let ix_data = axelar_solana_gateway_v2::instruction::ApproveMessage {
                            signing_verifier_set_hash: execute_data
                                .signing_verifier_set_merkle_root,
                            merkleised_message,
                            payload_merkle_root: execute_data.payload_merkle_root,
                        }
                        .data();
                        let accounts = axelar_solana_gateway_v2::accounts::ApproveMessage {
                            funder: self.keypair.pubkey(),
                            incoming_message_pda: pda,
                            program: axelar_solana_gateway_v2::ID,
                            system_program: solana_program::system_program::id(),
                            gateway_root_pda: self.gateway_address,
                            verification_session_account: verification_session_tracker_pda,
                            event_authority,
                        };
                        let ix = Instruction {
                            program_id: axelar_solana_gateway_v2::ID,
                            accounts: accounts.to_account_metas(None),
                            data: ix_data,
                        };
                        self.send_to_chain(ix, Some(msg_id), Some(chain))
                    })
                    .collect::<FuturesUnordered<_>>();

                let mut failed_events = vec![];
                while let Some(result) = merkelised_message_futures.next().await {
                    match result.status {
                        Ok(_) => {
                            debug!(
                                "Message approved successfully, signature: {}",
                                result.tx_hash.unwrap_or("".to_string())
                            );
                        }
                        Err(e) => {
                            // Create the cannot execute event for this specific failed message
                            let event = self
                                .gmp_api
                                .cannot_execute_message(
                                    task.common.id.clone(),
                                    result.message_id.unwrap_or("".to_string()),
                                    result.source_chain.unwrap_or("".to_string()),
                                    e.to_string(),
                                    CannotExecuteMessageReason::Error,
                                )
                                .await
                                .map_err(|e| IncluderError::GenericError(e.to_string()))?;
                            failed_events.push(event);
                        }
                    }
                }
                debug!("Approved messages transaction sent successfully");

                return Ok(failed_events);
            }
        };
    }

    #[tracing::instrument(skip(self))]
    async fn handle_refund_task(&self, _task: RefundTask) -> Result<(), IncluderError> {
        Ok(())
    }

    #[tracing::instrument(skip(self), fields(message_id))]
    async fn handle_execute_task(&self, task: ExecuteTask) -> Result<(), IncluderError> {
        let message = Message {
            cc_id: CrossChainId {
                chain: task.task.message.source_chain,
                id: task.task.message.message_id,
            },
            source_address: task.task.message.source_address,
            destination_chain: self.chain_name.clone(),
            destination_address: task.task.message.destination_address.clone(),
            payload_hash: task
                .task
                .message
                .payload_hash
                .into_bytes()
                .as_slice()
                .try_into()
                .map_err(|_| {
                    IncluderError::GenericError(
                        "Failed to convert payload hash to [u8; 32]".to_string(),
                    )
                })?,
        };
        let command_id = message.command_id();
        let (gateway_incoming_message_pda, ..) = get_incoming_message_pda(&command_id);

        if self
            .client
            .incoming_message_already_executed(&gateway_incoming_message_pda)
            .await
            .map_err(|e| IncluderError::GenericError(e.to_string()))?
        {
            tracing::warn!("incoming message already executed");
            return Ok(());
        }

        let destination_address = task
            .task
            .message
            .destination_address
            .parse::<Pubkey>()
            .map_err(|e| IncluderError::GenericError(e.to_string()))?;

        let instruction = self
            .transaction_builder
            .build_execute_instruction(
                &message,
                &task.task.payload.into_bytes(),
                destination_address,
            )
            .await
            .map_err(|e| IncluderError::GenericError(e.to_string()))?;

        let transaction = self
            .transaction_builder
            .build(instruction)
            .await
            .map_err(|e| IncluderError::GenericError(e.to_string()))?;

        let gas_cost = self
            .client
            .get_gas_cost_from_simulation(&transaction)
            .await
            .map_err(|e| IncluderError::GenericError(e.to_string()))?;

        let gas_cost_lamports = calculate_total_cost_lamports(&transaction, gas_cost)
            .map_err(|e| IncluderError::GenericError(e.to_string()))?;

        if gas_cost_lamports
            > u64::from_str(&task.task.available_gas_balance.amount)
                .map_err(|e| IncluderError::GenericError(e.to_string()))?
        {
            return Err(IncluderError::GenericError(
                "Not enough gas to execute message".to_string(),
            ));
        }

        let signature = self
            .client
            .send_transaction(transaction)
            .await
            .map_err(|e| IncluderError::GenericError(e.to_string()))?;

        info!("Transaction sent successfully: {}", signature.to_string());

        Ok(())
    }

    // #[tracing::instrument(skip(self), fields(message_id))]
    // async fn broadcast_refund_message(
    //     &self,
    //     refund_task: RefundTaskFields,
    // ) -> Result<String, BroadcasterError> {
    //     if refund_task.remaining_gas_balance.token_id.is_some() {
    //         return Err(BroadcasterError::GenericError(
    //             "Refund task with token_id is not supported".to_string(),
    //         ));
    //     }

    //     let cleaned_hash = refund_task
    //         .message
    //         .message_id
    //         .strip_prefix("0x")
    //         .unwrap_or(&refund_task.message.message_id);

    //     tracing::Span::current().record("message_id", &refund_task.message.message_id);

    //     let tx_hash = TonHash::from_hex(cleaned_hash)
    //         .map_err(|e| BroadcasterError::GenericError(e.to_string()))?;

    //     let address = Pubkey::from_str(&refund_task.refund_recipient_address)
    //         .map_err(|err| BroadcasterError::GenericError(err.to_string()))?;

    //     let original_amount = BigUint::from_str(&refund_task.remaining_gas_balance.amount)
    //         .map_err(|err| BroadcasterError::GenericError(err.to_string()))?;
    //     let gas_estimate = self.gas_estimator.native_gas_refund_estimate().await;

    //     info!(
    //         "Considering refund message: message_id={}, address={}, original_amount={}, gas_estimate={}",
    //         refund_task.message.message_id, address, refund_task.remaining_gas_balance.amount, gas_estimate
    //     );

    //     if original_amount < BigUint::from(gas_estimate) {
    //         info!(
    //             "Not enough balance to cover gas for refund: message_id={}",
    //             refund_task.message.message_id
    //         );
    //         return Err(BroadcasterError::InsufficientGas(
    //             "Not enough balance to cover gas for refund".to_string(),
    //         ));
    //     }

    //     let amount = original_amount - BigUint::from(gas_estimate);

    //     let native_refund = NativeRefundMessage::new(tx_hash, address, amount);

    //     let boc = native_refund
    //         .to_cell()
    //         .map_err(|e| BroadcasterError::GenericError(e.to_string()))?
    //         .to_boc_hex(true)
    //         .map_err(|e| {
    //             BroadcasterError::GenericError(format!(
    //                 "Failed to serialize relayer execute message: {e:?}"
    //             ))
    //         })?;

    //     let wallet = self.wallet.acquire().await.map_err(|e| {
    //         error!("Error acquiring wallet: {e:?}");
    //         BroadcasterError::GenericError(format!("Wallet acquire failed: {e:?}"))
    //     })?;

    //     let result = async {
    //         let msg_value: BigUint = BigUint::from(REFUND_DUST);

    //         let actions: Vec<OutAction> =
    //             vec![
    //                 out_action(&boc, msg_value.clone(), self.gas_service_address.clone())
    //                     .map_err(|e| BroadcasterError::GenericError(e.to_string()))?,
    //             ];

    //         let res = self.send_to_chain(wallet, actions.clone(), None).await;
    //         let (tx_hash, _status) = match res {
    //             Ok(response) => (response.message_hash, Ok(())),
    //             Err(err) => (String::new(), Err(err)),
    //         };

    //         Ok(tx_hash)
    //     }
    //     .await;

    //     self.wallet.release(wallet).await;

    //     result
    // }
}
