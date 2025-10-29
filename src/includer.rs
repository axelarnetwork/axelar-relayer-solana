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
use axelar_solana_gateway_v2::{state::incoming_message::Message, CrossChainId};
use base64::Engine as _;
use borsh::BorshDeserialize;
use futures::stream::FuturesUnordered;
use futures::StreamExt as _;
use redis::aio::ConnectionManager;
use redis::{AsyncTypedCommands, SetExpiry, SetOptions};
use relayer_core::error::IncluderError;
use relayer_core::includer_worker::{ExecuteTaskEvents, IncluderTrait};
use relayer_core::utils::ThreadSafe;
use relayer_core::{
    database::Database, gmp_api::GmpApiTrait, includer::Includer, includer_worker::IncluderWorker,
    payload_cache::PayloadCache, queue::Queue,
};

use solana_sdk::instruction::Instruction;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signer::{keypair::Keypair, Signer};
use solana_transaction_parser::gmp_types::{
    Amount, CannotExecuteMessageReason, Event, ExecuteTask, GatewayTxTask, MessageExecutionStatus,
    RefundTask,
};
use solana_transaction_parser::redis::TransactionType;
use std::str::FromStr;
use std::sync::Arc;
use tracing::{debug, info, warn};

const MAX_GAS_EXCEEDED_COUNT: u64 = 3;
const GAS_COST_EXPIRATION: u64 = 604800; // one week

#[derive(Clone)]
pub struct SolanaIncluder<G: GmpApiTrait + ThreadSafe + Clone> {
    client: Arc<IncluderClient>,
    keypair: Arc<Keypair>,
    gateway_address: Pubkey,
    chain_name: String,
    transaction_builder: TransactionBuilder<GasCalculator<IncluderClient>>,
    gmp_api: Arc<G>,
    redis_conn: ConnectionManager,
}

impl<G: GmpApiTrait + ThreadSafe + Clone> SolanaIncluder<G> {
    pub fn new(
        client: Arc<IncluderClient>,
        keypair: Arc<Keypair>,
        gateway_address: Pubkey,
        chain_name: String,
        transaction_builder: TransactionBuilder<GasCalculator<IncluderClient>>,
        gmp_api: Arc<G>,
        redis_conn: ConnectionManager,
    ) -> Self {
        Self {
            client,
            keypair,
            gateway_address,
            chain_name,
            transaction_builder,
            gmp_api,
            redis_conn,
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

        let client = Arc::new(
            IncluderClient::new(&solana_rpc, solana_commitment, 3)
                .map_err(|e| error_stack::report!(IncluderError::GenericError(e.to_string())))?,
        );

        let gateway_address = Pubkey::from_str(&solana_gateway)
            .map_err(|e| IncluderError::GenericError(e.to_string()))?;

        let keypair = Arc::new(config.signing_keypair());

        let gas_calculator = GasCalculator::new(client.as_ref().clone(), Arc::clone(&keypair));

        let transaction_builder = TransactionBuilder::new(Arc::clone(&keypair), gas_calculator);

        let solana_includer = SolanaIncluder::new(
            Arc::clone(&client),
            Arc::clone(&keypair),
            gateway_address,
            config.common_config.chain_name,
            transaction_builder,
            Arc::clone(&gmp_api),
            redis_conn.clone(),
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

    async fn send_gateway_tx_to_chain(
        &self,
        ix: Instruction,
        message_id: Option<String>,
        source_chain: Option<String>,
        gas_exceeded_count: u64,
    ) -> SendToChainResult {
        let tx_res = self
            .transaction_builder
            .build(ix.clone(), gas_exceeded_count)
            .await;

        let tx = match tx_res {
            Ok(tx) => tx,
            Err(e) => {
                return SendToChainResult {
                    gas_cost: None,
                    tx_hash: None,
                    status: Err(IncluderError::GenericError(e.to_string())),
                    message_id,
                    source_chain,
                }
            }
        };

        let res = self.client.send_transaction(tx).await;
        match res {
            Ok((signature, gas_cost)) => {
                let tx_hash = signature.to_string();
                debug!("Transaction sent successfully: {}", tx_hash);

                SendToChainResult {
                    tx_hash: Some(tx_hash),
                    status: Ok(()),
                    message_id,
                    source_chain,
                    gas_cost,
                }
            }
            Err(e) => match e {
                IncluderClientError::GasExceededError(e) => {
                    if gas_exceeded_count > MAX_GAS_EXCEEDED_COUNT {
                        return SendToChainResult {
                            gas_cost: None,
                            tx_hash: None,
                            status: Err(IncluderError::RPCError(e)),
                            message_id,
                            source_chain,
                        };
                    }
                    Box::pin(self.send_gateway_tx_to_chain(
                        ix,
                        message_id,
                        source_chain,
                        gas_exceeded_count + 1,
                    ))
                    .await
                }
                _ => SendToChainResult {
                    gas_cost: None,
                    tx_hash: None,
                    status: Err(IncluderError::RPCError(e.to_string())),
                    message_id,
                    source_chain,
                },
            },
        }
    }

    async fn build_execute_transaction_and_send(
        &self,
        instruction: Instruction,
        task: ExecuteTask,
        gas_exceeded_count: u64,
    ) -> Result<ExecuteTaskEvents, IncluderError> {
        let transaction = self
            .transaction_builder
            .build(instruction.clone(), gas_exceeded_count)
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
            let error_message = format!(
                "Not enough gas to execute message. Available gas: {}, required gas: {}",
                task.task.available_gas_balance.amount, gas_cost_lamports
            );
            let event = self
                .gmp_api
                .cannot_execute_message(
                    task.common.id.clone(),
                    task.task.message.message_id.clone(),
                    task.task.message.source_chain.clone(),
                    error_message,
                    CannotExecuteMessageReason::InsufficientGas,
                )
                .await
                .map_err(|e| IncluderError::GenericError(e.to_string()))?;
            return Ok(ExecuteTaskEvents {
                cannot_execute_events: vec![event],
                reverted_events: vec![],
            });
        }

        let signature_res = self.client.send_transaction(transaction).await;

        match signature_res {
            Ok((signature, gas_cost)) => {
                info!("Transaction sent successfully: {}", signature.to_string());

                // TODO: Spawn a task to write to Redis
                self.write_to_redis(
                    task.task.message.message_id.clone(),
                    gas_cost.unwrap_or(0),
                    TransactionType::Execute,
                )
                .await;

                Ok(ExecuteTaskEvents {
                    cannot_execute_events: vec![],
                    reverted_events: vec![],
                })
            }
            Err(e) => match e {
                IncluderClientError::GasExceededError(e) => {
                    warn!("Gas exceeded: {}", e);
                    if gas_exceeded_count > MAX_GAS_EXCEEDED_COUNT {
                        return Err(IncluderError::GenericError(e));
                    }
                    // needed because of recursive async functions in rust
                    Box::pin(self.build_execute_transaction_and_send(
                        instruction.clone(),
                        task.clone(),
                        gas_exceeded_count + 1,
                    ))
                    .await
                    .map_err(|e| IncluderError::GenericError(e.to_string()))
                }
                IncluderClientError::TransactionError(e) => {
                    warn!("Transaction reverted: {}", e);
                    let event = self.gmp_api.execute_message(
                        task.task.message.message_id.clone(),
                        task.task.message.source_chain.clone(),
                        MessageExecutionStatus::REVERTED,
                        Amount {
                            amount: gas_cost_lamports.to_string(),
                            token_id: None,
                        },
                    );

                    Ok(ExecuteTaskEvents {
                        cannot_execute_events: vec![],
                        reverted_events: vec![event],
                    })
                }
                _ => Err(IncluderError::GenericError(e.to_string())),
            },
        }
    }

    async fn write_to_redis(
        &self,
        message_id: String,
        gas_cost: u64,
        transaction_type: TransactionType,
    ) {
        debug!("Writing gas cost to Redis");
        let mut redis_conn = self.redis_conn.clone();
        let set_opts = SetOptions::default().with_expiration(SetExpiry::EX(GAS_COST_EXPIRATION));
        let key = format!("cost:{}:{}", transaction_type, message_id);
        let result = redis_conn
            .set_options(key.clone(), gas_cost, set_opts)
            .await;

        match result {
            Ok(_) => {
                debug!(
                    "Gas cost written to Redis successfully, key: {}, value: {}",
                    key, gas_cost
                );
            }
            Err(e) => {
                warn!("Failed to write gas cost to Redis: {}", e);
            }
        }
    }
}

struct SendToChainResult {
    tx_hash: Option<String>,
    status: Result<(), IncluderError>,
    message_id: Option<String>,
    source_chain: Option<String>,
    gas_cost: Option<u64>,
}

#[async_trait]
impl<G: GmpApiTrait + ThreadSafe + Clone> IncluderTrait for SolanaIncluder<G> {
    #[tracing::instrument(skip(self), fields(message_id))]
    async fn handle_gateway_tx_task(
        &self,
        task: GatewayTxTask,
    ) -> Result<Vec<Event>, IncluderError> {
        let mut total_cost: u64 = 0;

        let execute_data_bytes = hex::encode(
            base64::prelude::BASE64_STANDARD
                .decode(task.task.execute_data)
                .map_err(|e| IncluderError::GenericError(e.to_string()))?,
        )
        .into_bytes();

        let execute_data = ExecuteData::try_from_slice(&execute_data_bytes)
            .map_err(|_| IncluderError::GenericError("cannot decode execute data".to_string()))?;

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

        let send_to_chain_res = self.send_gateway_tx_to_chain(ix, None, None, 0).await;
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
            "Transaction for initializing payload verification session successfully: {}. Cost: {}",
            send_to_chain_res.tx_hash.unwrap_or("".to_string()),
            send_to_chain_res.gas_cost.unwrap_or(0)
        );

        total_cost += send_to_chain_res.gas_cost.unwrap_or(0);

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

                self.send_gateway_tx_to_chain(ix, None, None, 0)
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
            total_cost += result.gas_cost.unwrap_or(0);
            debug!(
                "Transaction for verifying signature successfully: {}. Cost: {}",
                result.tx_hash.unwrap_or("".to_string()),
                result.gas_cost.unwrap_or(0)
            );
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
                match self
                    .send_gateway_tx_to_chain(ix, None, None, 0)
                    .await
                    .status
                {
                    Ok(_) => {
                        debug!(
                            "Rotated signers transaction sent successfully. Cost: {}",
                            total_cost
                        );
                        total_cost += send_to_chain_res.gas_cost.unwrap_or(0);
                        debug!("Total cost: {}", total_cost);
                        // For verifier set rotation, we don't have messages to process, so return empty
                        return Ok(vec![]);
                    }
                    Err(e) => {
                        return Err(IncluderError::GatewayTxTaskError(e.to_string()));
                    }
                }
            }
            MerkleisedPayload::NewMessages { messages } => {
                let number_of_messages = messages.len();
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
                        self.send_gateway_tx_to_chain(ix, Some(msg_id), Some(chain), 0)
                    })
                    .collect::<FuturesUnordered<_>>();

                let mut failed_events = vec![];
                while let Some(result) = merkelised_message_futures.next().await {
                    match result.status {
                        Ok(_) => {
                            // The overhead cost is the initialize payload verification session and the total cost of verifying all signatures
                            // divided by the number of messages. The total cost for the message is the overhead plus its own cost.
                            let overhead_cost =
                                total_cost.saturating_div(number_of_messages as u64);
                            let message_cost =
                                result.gas_cost.unwrap_or(0).saturating_add(overhead_cost);
                            debug!(
                                "Message approved successfully, signature: {}",
                                result.tx_hash.unwrap_or("".to_string())
                            );
                            self.write_to_redis(
                                result.message_id.unwrap_or("".to_string()),
                                message_cost,
                                TransactionType::Approve,
                            )
                            .await;
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
    async fn handle_execute_task(
        &self,
        task: ExecuteTask,
    ) -> Result<ExecuteTaskEvents, IncluderError> {
        let message = Message {
            cc_id: CrossChainId {
                chain: task.task.message.source_chain.clone(),
                id: task.task.message.message_id.clone(),
            },
            source_address: task.task.message.source_address.clone(),
            destination_chain: self.chain_name.clone(),
            destination_address: task.task.message.destination_address.clone(),
            payload_hash: task
                .task
                .message
                .payload_hash
                .clone()
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
            return Ok(ExecuteTaskEvents {
                cannot_execute_events: vec![],
                reverted_events: vec![],
            });
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
                &task.task.payload.clone().into_bytes(),
                destination_address,
            )
            .await
            .map_err(|e| IncluderError::GenericError(e.to_string()))?;

        self.build_execute_transaction_and_send(instruction, task, 0)
            .await
            .map_err(|e| IncluderError::GenericError(e.to_string()))
    }
}
