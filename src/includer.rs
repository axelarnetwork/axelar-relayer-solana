use crate::config::SolanaConfig;
use crate::error::IncluderClientError;
use crate::gas_calculator::GasCalculator;
use crate::includer_client::{IncluderClient, IncluderClientTrait};
use crate::models::refunds::RefundsModel;
use crate::program_types::{ExecuteData, MerkleisedPayload};
use crate::redis::RedisConnectionTrait;
use crate::refund_manager::SolanaRefundManager;
use crate::transaction_builder::{TransactionBuilder, TransactionBuilderTrait};
use crate::utils::{
    calculate_total_cost_lamports, get_cannot_execute_events_from_execute_data,
    get_gas_service_event_authority_pda, get_gateway_event_authority_pda, get_incoming_message_pda,
    get_operator_pda, get_signature_verification_pda, get_treasury_pda,
    get_verifier_set_tracker_pda,
};
use anchor_lang::{InstructionData, ToAccountMetas};
use async_trait::async_trait;
use axelar_solana_gas_service_v2;
use axelar_solana_gateway_v2::{state::incoming_message::Message, CrossChainId};
use base64::Engine as _;
use borsh::BorshDeserialize;
use chrono::Utc;
use futures::stream::FuturesUnordered;
use futures::StreamExt as _;
use relayer_core::error::IncluderError;
use relayer_core::includer_worker::IncluderTrait;
use relayer_core::utils::ThreadSafe;
use relayer_core::{
    database::Database, gmp_api::GmpApiTrait, includer::Includer, includer_worker::IncluderWorker,
    payload_cache::PayloadCache, queue::Queue,
};
use solana_sdk::instruction::Instruction;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use solana_sdk::signer::{keypair::Keypair, Signer};
use solana_transaction_parser::gmp_types::{
    Amount, CannotExecuteMessageReason, Event, ExecuteTask, GatewayTxTask, MessageExecutionStatus,
    RefundTask,
};
use solana_transaction_parser::redis::TransactionType;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

pub const MAX_GAS_EXCEEDED_RETRIES: u64 = 3;
const SOLANA_EXPIRATION_TIME: u64 = 90;

#[derive(Clone)]
pub struct SolanaIncluder<
    G: GmpApiTrait + ThreadSafe + Clone,
    R: RedisConnectionTrait + Clone,
    RF: RefundsModel + Clone,
    IC: IncluderClientTrait + Clone,
> {
    client: Arc<IC>,
    keypair: Arc<Keypair>,
    gateway_address: Pubkey,
    chain_name: String,
    transaction_builder: TransactionBuilder<GasCalculator<IC>, IC>,
    gmp_api: Arc<G>,
    redis_conn: R,
    refunds_model: Arc<RF>,
}

impl<
        G: GmpApiTrait + ThreadSafe + Clone,
        R: RedisConnectionTrait + Clone,
        RF: RefundsModel + Clone,
        IC: IncluderClientTrait + Clone,
    > SolanaIncluder<G, R, RF, IC>
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        client: Arc<IC>,
        keypair: Arc<Keypair>,
        gateway_address: Pubkey,
        chain_name: String,
        transaction_builder: TransactionBuilder<GasCalculator<IC>, IC>,
        gmp_api: Arc<G>,
        redis_conn: R,
        refunds_model: Arc<RF>,
    ) -> Self {
        Self {
            client,
            keypair,
            gateway_address,
            chain_name,
            transaction_builder,
            gmp_api,
            redis_conn,
            refunds_model,
        }
    }

    pub async fn create_includer<
        DB: Database + ThreadSafe + Clone,
        GMP: GmpApiTrait + ThreadSafe + Clone,
    >(
        config: SolanaConfig,
        gmp_api: Arc<GMP>,
        redis_conn: R,
        payload_cache_for_includer: PayloadCache<DB>,
        construct_proof_queue: Arc<Queue>,
        refunds_model: Arc<RF>,
    ) -> error_stack::Result<
        Includer<
            Arc<IncluderClient>,
            SolanaRefundManager,
            DB,
            GMP,
            SolanaIncluder<GMP, R, RF, IncluderClient>,
        >,
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

        let transaction_builder =
            TransactionBuilder::new(Arc::clone(&keypair), gas_calculator, Arc::clone(&client));

        let solana_includer = SolanaIncluder::new(
            Arc::clone(&client),
            Arc::clone(&keypair),
            gateway_address,
            config.common_config.chain_name,
            transaction_builder,
            Arc::clone(&gmp_api),
            redis_conn.clone(),
            refunds_model,
        );

        let refund_manager = SolanaRefundManager::new()
            .map_err(|e| error_stack::report!(IncluderError::GenericError(e.to_string())))?;

        let worker = IncluderWorker::new(
            client,
            refund_manager,
            gmp_api,
            payload_cache_for_includer,
            construct_proof_queue,
            redis_conn.inner().clone(),
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
        let tx = match self
            .transaction_builder
            .build(std::slice::from_ref(&ix), gas_exceeded_count, None)
            .await
        {
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
                    if gas_exceeded_count > MAX_GAS_EXCEEDED_RETRIES {
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
        alt_info: Option<ALTInfo>,
    ) -> Result<Vec<Event>, IncluderError> {
        let alt_pubkey = alt_info.as_ref().and_then(|a| a.alt_pubkey);
        let transaction = self
            .transaction_builder
            .build(
                std::slice::from_ref(&instruction),
                gas_exceeded_count,
                alt_info.clone(),
            )
            .await
            .map_err(|e| IncluderError::GenericError(e.to_string()))?;

        let gas_cost = self
            .client
            .get_gas_cost_from_simulation(transaction.clone())
            .await
            .map_err(|e| IncluderError::GenericError(e.to_string()))?;

        let gas_cost_lamports = calculate_total_cost_lamports(&transaction, gas_cost)
            .map_err(|e| IncluderError::GenericError(e.to_string()))?;

        let mut alt_gas_cost_lamports = 0;
        let mut alt_tx = None;
        // create the ALT table first if it is a versioned transaction
        // Check if ALT already exists in Redis (from a previous attempt) before trying to create it
        let alt_exists_in_redis = if let Some(alt_pubkey) = alt_pubkey {
            self.redis_conn
                .get_alt_pubkey(task.task.message.message_id.clone())
                .await
                .ok()
                .flatten()
                .map(|existing| existing == alt_pubkey)
                .unwrap_or(false)
        } else {
            false
        };

        if let Some(ALTInfo {
            alt_ix_create: Some(ref alt_ix_create),
            alt_ix_extend: Some(ref alt_ix_extend),
            alt_pubkey: Some(_),
            alt_addresses: _,
        }) = alt_info
        {
            // Only create ALT if it doesn't already exist in Redis
            if !alt_exists_in_redis {
                // ALT doesn't exist, create it
                let alt_tx_build = self
                    .transaction_builder
                    .build(
                        &[alt_ix_create.clone(), alt_ix_extend.clone()],
                        gas_exceeded_count,
                        None,
                    )
                    .await
                    .map_err(|e| IncluderError::GenericError(e.to_string()))?;

                alt_tx = Some(alt_tx_build.clone());

                let alt_simulation_res = self
                    .client
                    .get_gas_cost_from_simulation(alt_tx_build.clone())
                    .await
                    .map_err(|e| IncluderError::GenericError(e.to_string()))?;

                alt_gas_cost_lamports =
                    calculate_total_cost_lamports(&alt_tx_build, alt_simulation_res)
                        .map_err(|e| IncluderError::GenericError(e.to_string()))?;
            } else {
                debug!("ALT already exists in Redis, skipping creation");
            }
        }

        debug!(
            "Available balance: {}, needed gas cost: {}, alt gas cost: {}",
            task.task.available_gas_balance.amount, gas_cost_lamports, alt_gas_cost_lamports
        );

        let total_cost = gas_cost_lamports.saturating_add(alt_gas_cost_lamports);

        if total_cost
            > u64::from_str(&task.task.available_gas_balance.amount)
                .map_err(|e| IncluderError::GenericError(e.to_string()))?
        {
            let error_message = format!(
                "Not enough gas to execute message. Available gas: {}, required gas: {}",
                task.task.available_gas_balance.amount, total_cost
            );
            let event = self.gmp_api.cannot_execute_message(
                task.common.id.clone(),
                task.task.message.message_id.clone(),
                task.task.message.source_chain.clone(),
                error_message,
                CannotExecuteMessageReason::InsufficientGas,
            );
            return Ok(vec![event]);
        }

        let mut alt_actual_gas_cost = 0;

        if let Some(alt_tx) = alt_tx {
            let alt_signature_res = self.client.send_transaction(alt_tx).await;
            match alt_signature_res {
                Ok((signature, alt_cost)) => {
                    alt_actual_gas_cost = alt_cost.unwrap_or(0);
                    debug!(
                        "ALT transaction sent successfully: {}",
                        signature.to_string()
                    );
                    // Write ALT pubkey to Redis upon successful ALT transaction
                    // Use alt_pubkey extracted earlier (line 233) - we only write if we created a new ALT
                    if let Some(alt_pubkey) = alt_pubkey {
                        if let Err(e) = self
                            .redis_conn
                            .write_alt_pubkey(task.task.message.message_id.clone(), alt_pubkey)
                            .await
                        {
                            error!("Failed to write ALT pubkey to Redis: {}", e);
                        }
                    }
                }
                Err(e) => match e {
                    IncluderClientError::GasExceededError(e) => {
                        warn!("Gas exceeded in ALT transaction: {}", e);
                        if gas_exceeded_count > MAX_GAS_EXCEEDED_RETRIES {
                            return Err(IncluderError::GenericError(e));
                        }
                        // needed because of recursive async functions in rust
                        return Box::pin(self.build_execute_transaction_and_send(
                            instruction.clone(),
                            task.clone(),
                            gas_exceeded_count + 1,
                            alt_info,
                        ))
                        .await
                        .map_err(|e| IncluderError::GenericError(e.to_string()));
                    }
                    _ => return Err(IncluderError::GenericError(e.to_string())),
                },
            }
        }

        let signature_res = self.client.send_transaction(transaction).await;

        match signature_res {
            Ok((signature, gas_cost)) => {
                info!("Transaction sent successfully: {}", signature.to_string());

                // TODO: Spawn a task to write to Redis
                self.redis_conn
                    .write_gas_cost(
                        task.task.message.message_id.clone(),
                        gas_cost.unwrap_or(0).saturating_add(alt_actual_gas_cost),
                        TransactionType::Execute,
                    )
                    .await;

                Ok(vec![])
            }
            Err(e) => match e {
                IncluderClientError::GasExceededError(e) => {
                    warn!("Gas exceeded in execute transaction: {}", e);
                    if gas_exceeded_count > MAX_GAS_EXCEEDED_RETRIES {
                        return Err(IncluderError::GenericError(e));
                    }
                    // needed because of recursive async functions in rust
                    Box::pin(self.build_execute_transaction_and_send(
                        instruction.clone(),
                        task.clone(),
                        gas_exceeded_count + 1,
                        alt_info,
                    ))
                    .await
                    .map_err(|e| IncluderError::GenericError(e.to_string()))
                }
                IncluderClientError::TransactionError(e) => {
                    warn!("Transaction reverted: {}", e);
                    // Include ALT cost if ALT transaction was sent successfully
                    let total_reverted_cost = gas_cost_lamports.saturating_add(alt_actual_gas_cost);
                    let event = self.gmp_api.execute_message(
                        task.task.message.message_id.clone(),
                        task.task.message.source_chain.clone(),
                        MessageExecutionStatus::REVERTED,
                        Amount {
                            amount: total_reverted_cost.to_string(),
                            token_id: None,
                        },
                    );

                    Ok(vec![event])
                }
                _ => Err(IncluderError::GenericError(e.to_string())),
            },
        }
    }

    async fn refund_already_processed(&self, refund_id: String) -> Result<bool, IncluderError> {
        let refund_id_clone = refund_id.clone();
        let potential_refund = self
            .refunds_model
            .find(refund_id)
            .await
            .map_err(|e| IncluderError::GenericError(format!("Failed to get refund: {}", e)))?;

        match potential_refund {
            // if signature was written in DB, check its status to see if we need to re-process it or if is has already been processed
            Some((signature, updated_at)) => {
                let tx_res = self
                    .client
                    .get_signature_status(
                        &Signature::from_str(&signature)
                            .map_err(|e| IncluderError::GenericError(e.to_string()))?,
                    )
                    .await;
                match tx_res {
                    Ok(Some(Ok(_))) => Ok(true),
                    Ok(Some(Err(_))) => Ok(false), // failed on chain
                    Ok(None) => {
                        // Check if transaction has expired (90 seconds has passed)
                        let expiration_time = updated_at.checked_add_signed(
                            chrono::Duration::seconds(SOLANA_EXPIRATION_TIME as i64),
                        );

                        match expiration_time {
                            Some(expires_at) => {
                                if Utc::now() > expires_at {
                                    // Transaction has expired, allow reprocessing
                                    Ok(false)
                                } else {
                                    tokio::time::sleep(Duration::from_secs(1)).await;
                                    // Transaction hasn't expired yet, recurse to check again
                                    Box::pin(self.refund_already_processed(refund_id_clone)).await
                                }
                            }
                            None => {
                                Err(IncluderError::GenericError("Overflow occurred".to_string()))
                            }
                        }
                    }
                    Err(e) => Err(IncluderError::GenericError(e.to_string())),
                }
            }
            None => Ok(false),
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
impl<
        G: GmpApiTrait + ThreadSafe + Clone,
        R: RedisConnectionTrait + Clone,
        RF: RefundsModel + Clone,
        IC: IncluderClientTrait + Clone,
    > IncluderTrait for SolanaIncluder<G, R, RF, IC>
{
    #[tracing::instrument(skip(self), fields(message_id))]
    async fn handle_gateway_tx_task(
        &self,
        task: GatewayTxTask,
    ) -> Result<Vec<Event>, IncluderError> {
        let mut total_cost: u64 = 0;

        let execute_data_bytes = base64::prelude::BASE64_STANDARD
            .decode(task.task.execute_data)
            .map_err(|e| IncluderError::GenericError(e.to_string()))?;

        let execute_data = ExecuteData::try_from_slice(&execute_data_bytes)
            .map_err(|_| IncluderError::GenericError("cannot decode execute data".to_string()))?;

        let (verification_session_tracker_pda, _) =
            get_signature_verification_pda(&execute_data.payload_merkle_root);

        let ix_data = axelar_solana_gateway_v2::instruction::InitializePayloadVerificationSession {
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
                let tx_res = self.send_gateway_tx_to_chain(ix, None, None, 0).await;
                match tx_res.status {
                    Ok(_) => {
                        debug!(
                            "Rotated signers transaction sent successfully. Cost: {}",
                            total_cost
                        );
                        total_cost += tx_res.gas_cost.unwrap_or(0);
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
                            self.redis_conn
                                .write_gas_cost(
                                    result.message_id.unwrap_or("".to_string()),
                                    message_cost,
                                    TransactionType::Approve,
                                )
                                .await;
                        }
                        Err(e) => {
                            // Create the cannot execute event for this specific failed message
                            let event = self.gmp_api.cannot_execute_message(
                                task.common.id.clone(),
                                result.message_id.unwrap_or("".to_string()),
                                result.source_chain.unwrap_or("".to_string()),
                                e.to_string(),
                                CannotExecuteMessageReason::Error,
                            );
                            failed_events.push(event);
                        }
                    }
                }
                debug!("Approved messages transaction sent successfully");

                return Ok(failed_events);
            }
        };
    }

    #[tracing::instrument(skip(self), fields(message_id))]
    async fn handle_execute_task(&self, task: ExecuteTask) -> Result<Vec<Event>, IncluderError> {
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
            return Ok(vec![]);
        }

        let destination_address = task
            .task
            .message
            .destination_address
            .parse::<Pubkey>()
            .map_err(|e| IncluderError::GenericError(e.to_string()))?;

        // Check if ALT already exists in Redis for this message_id
        let existing_alt_pubkey = self
            .redis_conn
            .get_alt_pubkey(task.task.message.message_id.clone())
            .await
            .map_err(|e| IncluderError::GenericError(e.to_string()))?;

        let (instruction, alt_info) = self
            .transaction_builder
            .build_execute_instruction(
                &message,
                &task.task.payload.clone().into_bytes(),
                destination_address,
                existing_alt_pubkey,
            )
            .await
            .map_err(|e| IncluderError::GenericError(e.to_string()))?;

        self.build_execute_transaction_and_send(instruction, task, 0, alt_info)
            .await
            .map_err(|e| IncluderError::GenericError(e.to_string()))
    }

    #[tracing::instrument(skip(self))]
    async fn handle_refund_task(&self, task: RefundTask) -> Result<(), IncluderError> {
        let refund_id = task.task.message.message_id.clone();
        if self.refund_already_processed(refund_id.clone()).await? {
            warn!("Refund already processed: {}", refund_id);
            self.refunds_model
                .delete(refund_id.clone())
                .await
                .map_err(|e| IncluderError::GenericError(e.to_string()))?;
            debug!("Deleted refund from database: {}", refund_id);
            return Ok(());
        }

        let receiver = Pubkey::from_str(&task.task.refund_recipient_address.clone())
            .map_err(|e| IncluderError::GenericError(e.to_string()))?;
        let (operator_pda, _) = get_operator_pda(&self.keypair.pubkey());
        let (treasury, _) = get_treasury_pda();
        let (event_authority, _) = get_gas_service_event_authority_pda();

        let refund_amount = task
            .task
            .remaining_gas_balance
            .amount
            .parse::<u64>()
            .map_err(|e| IncluderError::GenericError(e.to_string()))?;

        let accounts = axelar_solana_gas_service_v2::accounts::RefundFees {
            operator: self.keypair.pubkey(),
            operator_pda,
            receiver,
            treasury,
            event_authority,
            program: axelar_solana_gas_service_v2::ID,
        }
        .to_account_metas(None);

        let data = axelar_solana_gas_service_v2::instruction::RefundFees {
            message_id: task.task.message.message_id.clone(),
            amount: refund_amount,
        }
        .data();

        let ix = Instruction {
            program_id: axelar_solana_gas_service_v2::ID,
            accounts,
            data,
        };

        let tx = self
            .transaction_builder
            .build(&[ix], 0, None)
            .await
            .map_err(|e| IncluderError::GenericError(e.to_string()))?;
        let compute_units_cost = self
            .client
            .get_gas_cost_from_simulation(tx.clone())
            .await
            .map_err(|e| IncluderError::GenericError(e.to_string()))?;
        let gas_cost_lamports = calculate_total_cost_lamports(&tx, compute_units_cost)
            .map_err(|e| IncluderError::GenericError(e.to_string()))?;

        if gas_cost_lamports > refund_amount {
            return Err(IncluderError::GenericError(format!(
                "Cost is higher than remaining balance to refund. Cost: {}, Remaining balance: {}",
                gas_cost_lamports, refund_amount
            )));
        }

        // write the signature to the database before sending the transaction to avoid
        // re processing it if we do not capture the success/failure response
        self.refunds_model
            .upsert(
                refund_id.clone(),
                tx.get_signature()
                    .ok_or_else(|| {
                        IncluderError::GenericError("Failed to get signature".to_string())
                    })?
                    .to_string(),
            )
            .await
            .map_err(|e| IncluderError::GenericError(e.to_string()))?;

        let (signature, _gas_cost) = self
            .client
            .send_transaction(tx)
            .await
            .map_err(|e| IncluderError::GenericError(e.to_string()))?;

        info!(
            "Refund fees transaction sent successfully: {}",
            signature.to_string()
        );

        Ok(())
    }
}

#[derive(Clone)]
pub struct ALTInfo {
    pub alt_ix_create: Option<Instruction>,
    pub alt_ix_extend: Option<Instruction>,
    pub alt_pubkey: Option<Pubkey>,
    pub alt_addresses: Option<Vec<Pubkey>>,
}

impl ALTInfo {
    pub fn new(
        alt_ix_create: Option<Instruction>,
        alt_ix_extend: Option<Instruction>,
        alt_pubkey: Option<Pubkey>,
    ) -> Self {
        Self {
            alt_ix_create,
            alt_ix_extend,
            alt_pubkey,
            alt_addresses: None,
        }
    }

    pub fn with_addresses(mut self, addresses: Vec<Pubkey>) -> Self {
        self.alt_addresses = Some(addresses);
        self
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::includer_client::MockIncluderClientTrait;
//     use crate::models::refunds::MockRefundsModel;
//     use crate::redis::MockRedisConnectionTrait;
//     use crate::transaction_builder::TransactionBuilder;
//     use chrono::Utc;
//     use relayer_core::gmp_api::GmpApi;
//     use solana_sdk::pubkey::Pubkey;
//     use solana_sdk::signature::Signature;
//     use std::str::FromStr;
//     use std::sync::Arc;

//     fn create_test_includer(
//         mock_client: MockIncluderClientTrait,
//         mock_refunds_model: MockRefundsModel,
//     ) -> SolanaIncluder<
//         Arc<GmpApi>,
//         MockRedisConnectionTrait,
//         MockRefundsModel,
//         MockIncluderClientTrait,
//     > {
//         let keypair = Arc::new(Keypair::new());
//         let gateway_address = Pubkey::new_unique();
//         let chain_name = "test-chain".to_string();
//         // Use a real GmpApi instance (refund_already_processed doesn't use it anyway)
//         let gmp_api = Arc::new(
//             GmpApi::new("http://localhost:8080".to_string())
//                 .unwrap_or_else(|_| panic!("Failed to create GmpApi")),
//         );
//         let redis_conn = MockRedisConnectionTrait::new();

//         let gas_calculator =
//             crate::gas_calculator::GasCalculator::new(mock_client.clone(), Arc::clone(&keypair));
//         let transaction_builder = TransactionBuilder::new(
//             Arc::clone(&keypair),
//             gas_calculator,
//             Arc::new(mock_client.clone()),
//         );

//         SolanaIncluder::new(
//             Arc::new(mock_client),
//             keypair,
//             gateway_address,
//             chain_name,
//             transaction_builder,
//             gmp_api,
//             redis_conn,
//             Arc::new(mock_refunds_model),
//         )
//     }

//     #[tokio::test]
//     async fn test_refund_already_processed_successful_transaction() {
//         let mut mock_client = MockIncluderClientTrait::new();
//         let mut mock_refunds_model = MockRefundsModel::new();

//         let refund_id = "test-refund-123".to_string();
//         let signature_str = "4BmMcXeedDZ3p3sugJmtHTx2rHScRW6RYYXydjrSHUstDN4ELFVZRmWBqh5ZxPwoQ6WbhqwkUhnbDM341Qc8vHii";
//         let signature = Signature::from_str(signature_str).unwrap();
//         let updated_at = Utc::now() - chrono::Duration::seconds(30); // 30 seconds ago

//         // Mock refunds_model.find to return the signature and updated_at
//         mock_refunds_model
//             .expect_find()
//             .withf(move |id| id == &refund_id)
//             .times(1)
//             .returning(move |_| {
//                 Box::pin(async move { Ok(Some((signature_str.to_string(), updated_at))) })
//             });

//         // Mock client.get_signature_status to return Ok(Some(Ok(()))) for successful transaction
//         mock_client
//             .expect_get_signature_status()
//             .withf(move |sig| *sig == signature)
//             .times(1)
//             .returning(|_| Box::pin(async move { Ok(Some(Ok(()))) }));

//         let includer = create_test_includer(mock_client, mock_refunds_model);

//         let result = includer.refund_already_processed(refund_id).await;

//         assert!(result.is_ok());
//         assert_eq!(result.unwrap(), true);
//     }

//     #[tokio::test]
//     async fn test_refund_already_processed_not_found_on_chain() {
//         let mut mock_client = MockIncluderClientTrait::new();
//         let mut mock_refunds_model = MockRefundsModel::new();

//         let refund_id = "test-refund-456".to_string();
//         let signature_str = "4BmMcXeedSZ3p3sugJmtHTx2rHScRW6RYYXydjrSHUstDN4ELFVZRmWBqh5ZxPwoQ6WbhqwkUhnbDM341Qc8vHod";
//         let signature = Signature::from_str(signature_str).unwrap();
//         let updated_at = Utc::now() - chrono::Duration::seconds(30); // 30 seconds ago

//         // Mock refunds_model.find to return the signature and updated_at
//         mock_refunds_model
//             .expect_find()
//             .withf(move |id| id == &refund_id)
//             .times(1)
//             .returning(move |_| {
//                 Box::pin(async move { Ok(Some((signature_str.to_string(), updated_at))) })
//             });

//         // Mock client.get_signature_status to return Ok(None) for transaction not found
//         mock_client
//             .expect_get_signature_status()
//             .withf(move |sig| *sig == signature)
//             .times(1)
//             .returning(|_| Box::pin(async move { Ok(None) }));

//         let includer = create_test_includer(mock_client, mock_refunds_model);

//         let result = includer.refund_already_processed(refund_id).await;

//         assert!(result.is_ok());
//         assert_eq!(result.unwrap(), false);
//     }

//     #[tokio::test]
//     async fn test_refund_already_processed_not_in_db() {
//         let mut mock_refunds_model = MockRefundsModel::new();
//         let refund_id = "test-refund-789".to_string();

//         // Mock refunds_model.find to return None (not in database)
//         mock_refunds_model
//             .expect_find()
//             .withf(move |id| id == &refund_id)
//             .times(1)
//             .returning(|_| Box::pin(async move { Ok(None) }));

//         let mock_client = Arc::new(MockIncluderClientTrait::new());
//         let includer = create_test_includer(mock_client, Arc::new(mock_refunds_model));

//         let result = includer.refund_already_processed(refund_id).await;

//         assert!(result.is_ok());
//         assert_eq!(result.unwrap(), false);
//     }
// }
