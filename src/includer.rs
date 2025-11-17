use crate::config::SolanaConfig;
use crate::error::{IncluderClientError, TransactionBuilderError};
use crate::fees_client::FeesClient;
use crate::gas_calculator::GasCalculator;
use crate::includer_client::{IncluderClient, IncluderClientTrait};
use crate::models::refunds::RefundsModel;
use crate::program_types::{ExecuteData, MerkleisedPayload};
use crate::redis::RedisConnectionTrait;
use crate::refund_manager::SolanaRefundManager;
use crate::transaction_builder::{TransactionBuilder, TransactionBuilderTrait};
use crate::utils::{
    get_cannot_execute_events_from_execute_data, get_gas_service_event_authority_pda,
    get_gateway_event_authority_pda, get_gateway_root_config_internal, get_incoming_message_pda,
    get_operator_pda, get_signature_verification_pda, get_treasury_pda,
    get_verifier_set_tracker_pda, not_enough_gas_event,
};
use anchor_lang::{InstructionData, ToAccountMetas};
use async_trait::async_trait;
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
use solana_axelar_gas_service;
use solana_axelar_gateway::{state::incoming_message::Message, CrossChainId};
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

const SOLANA_EXPIRATION_TIME: u64 = 90;

#[derive(Clone)]
pub struct SolanaIncluder<
    G: GmpApiTrait + ThreadSafe + Clone,
    R: RedisConnectionTrait + Clone,
    RF: RefundsModel + Clone,
    IC: IncluderClientTrait + Clone,
    TB: TransactionBuilderTrait<IC> + Clone,
> {
    client: Arc<IC>,
    keypair: Arc<Keypair>,
    chain_name: String,
    transaction_builder: TB,
    gmp_api: Arc<G>,
    redis_conn: R,
    refunds_model: Arc<RF>,
}

impl<
        G: GmpApiTrait + ThreadSafe + Clone,
        R: RedisConnectionTrait + Clone,
        RF: RefundsModel + Clone,
        IC: IncluderClientTrait + Clone,
        TB: TransactionBuilderTrait<IC> + Clone,
    > SolanaIncluder<G, R, RF, IC, TB>
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        client: Arc<IC>,
        keypair: Arc<Keypair>,
        chain_name: String,
        transaction_builder: TB,
        gmp_api: Arc<G>,
        redis_conn: R,
        refunds_model: Arc<RF>,
    ) -> Self {
        Self {
            client,
            keypair,
            chain_name,
            transaction_builder,
            gmp_api,
            redis_conn,
            refunds_model,
        }
    }

    pub async fn create_includer<DB: Database + ThreadSafe + Clone, GMP: GmpApiTrait + Clone>(
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
            SolanaIncluder<
                GMP,
                R,
                RF,
                IncluderClient,
                TransactionBuilder<
                    GasCalculator<IncluderClient, FeesClient<IncluderClient>>,
                    IncluderClient,
                >,
            >,
        >,
        IncluderError,
    > {
        let solana_rpc = config.solana_poll_rpc.clone();
        let solana_commitment = config.solana_commitment;

        let client = Arc::new(
            IncluderClient::new(&solana_rpc, solana_commitment, 3)
                .map_err(|e| error_stack::report!(IncluderError::GenericError(e.to_string())))?,
        );

        let fees_client = Arc::new(
            FeesClient::new(client.as_ref().clone(), 10)
                .map_err(|e| error_stack::report!(IncluderError::GenericError(e.to_string())))?,
        );

        let keypair = Arc::new(config.signing_keypair());

        let gas_calculator = GasCalculator::new(
            client.as_ref().clone(),
            Arc::clone(&keypair),
            fees_client.as_ref().clone(),
        );

        let transaction_builder =
            TransactionBuilder::new(Arc::clone(&keypair), gas_calculator, Arc::clone(&client));

        let solana_includer = SolanaIncluder::new(
            Arc::clone(&client),
            Arc::clone(&keypair),
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
    ) -> SendToChainResult {
        let (tx, _) = match self
            .transaction_builder
            .build(std::slice::from_ref(&ix), None)
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
            Err(e) => SendToChainResult {
                gas_cost: None,
                tx_hash: None,
                status: Err(IncluderError::RPCError(e.to_string())),
                message_id,
                source_chain,
            },
        }
    }

    async fn build_execute_transaction_and_send(
        &self,
        instruction: Instruction,
        task: ExecuteTask,
        alt_info: Option<ALTInfo>,
    ) -> Result<Vec<Event>, IncluderError> {
        let mut alt_cost = None;
        let mut available_gas_balance = i64::from_str(&task.task.available_gas_balance.amount)
            .map_err(|e| IncluderError::GenericError(e.to_string()))?;

        if let Some(ALTInfo {
            alt_ix_create: Some(ref alt_ix_create),
            alt_ix_extend: Some(ref alt_ix_extend),
            alt_pubkey: Some(ref alt_pubkey),
            alt_addresses: _,
        }) = alt_info
        {
            // ALT doesn't exist, create it
            let (alt_tx_build, estimated_alt_cost) = self
                .transaction_builder
                .build(&[alt_ix_create.clone(), alt_ix_extend.clone()], None)
                .await
                .map_err(|e| IncluderError::GenericError(e.to_string()))?;

            if estimated_alt_cost as i64 > available_gas_balance {
                // TODO: should take into account the cost for closing the ALT
                return Ok(not_enough_gas_event(
                    available_gas_balance,
                    estimated_alt_cost,
                    task,
                    Arc::clone(&self.gmp_api),
                ));
            }

            let (signature, actual_alt_cost) = self
                .client
                .send_transaction(alt_tx_build)
                .await
                .map_err(|e| IncluderError::GenericError(e.to_string()))?;

            // TODO: give it some time to activate (1 block?)

            self.redis_conn
                .write_gas_cost(
                    task.task.message.message_id.clone(),
                    actual_alt_cost.unwrap_or(0),
                    TransactionType::Execute,
                )
                .await;

            alt_cost = actual_alt_cost;
            available_gas_balance =
                available_gas_balance.saturating_sub(alt_cost.unwrap_or(0) as i64);

            debug!(
                "ALT transaction sent successfully: {}",
                signature.to_string()
            );

            if let Err(e) = self
                .redis_conn
                .write_alt_pubkey(task.task.message.message_id.clone(), *alt_pubkey)
                .await
            {
                error!("Failed to write ALT pubkey to Redis: {}", e);
            }
        }

        let (transaction, estimated_tx_cost) = self
            .transaction_builder
            .build(std::slice::from_ref(&instruction), alt_info.clone())
            .await
            .map_err(|e| IncluderError::GenericError(e.to_string()))?;

        if estimated_tx_cost as i64 > available_gas_balance {
            return Ok(not_enough_gas_event(
                available_gas_balance,
                estimated_tx_cost,
                task,
                Arc::clone(&self.gmp_api),
            ));
        }

        match self.client.send_transaction(transaction).await {
            Ok((signature, actual_tx_cost)) => {
                info!("Transaction sent successfully: {}", signature.to_string());

                // TODO: Spawn a task to write to Redis
                self.redis_conn
                    .write_gas_cost(
                        task.task.message.message_id.clone(),
                        actual_tx_cost
                            .unwrap_or(0)
                            .saturating_add(alt_cost.unwrap_or(0)),
                        TransactionType::Execute,
                    )
                    .await;

                Ok(vec![])
            }
            Err(e) => match e {
                IncluderClientError::TransactionError(e) => {
                    warn!("Transaction reverted: {}", e);
                    // Include ALT cost if ALT transaction was sent successfully
                    // TODO: this might be off. We need to know the actual cost of the transaction
                    // in the case where it failed.
                    let total_reverted_cost =
                        estimated_tx_cost.saturating_add(alt_cost.unwrap_or(0));
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
        TB: TransactionBuilderTrait<IC> + Clone,
    > IncluderTrait for SolanaIncluder<G, R, RF, IC, TB>
{
    #[cfg_attr(
        feature = "instrumentation",
        tracing::instrument(skip(self), fields(message_id))
    )]
    async fn handle_gateway_tx_task(
        &self,
        task: GatewayTxTask,
    ) -> Result<Vec<Event>, IncluderError> {
        let mut total_cost: u64 = 0;

        let execute_data_bytes = base64::prelude::BASE64_STANDARD
            .decode(task.task.execute_data)
            .map_err(|e| IncluderError::GenericError(e.to_string()))?;

        let execute_data = ExecuteData::try_from_slice(&execute_data_bytes)
            .map_err(|e| IncluderError::GenericError(e.to_string()))?;

        let (verification_session_tracker_pda, _) = get_signature_verification_pda(
            &execute_data.payload_merkle_root,
            &execute_data.signing_verifier_set_merkle_root,
        );

        let ix_data = solana_axelar_gateway::instruction::InitializePayloadVerificationSession {
            merkle_root: execute_data.payload_merkle_root,
        }
        .data();

        let (verifier_set_tracker_pda, _) =
            get_verifier_set_tracker_pda(&execute_data.signing_verifier_set_merkle_root);

        let (gateway_root_pda, _) = get_gateway_root_config_internal();

        let accounts = solana_axelar_gateway::accounts::InitializePayloadVerificationSession {
            payer: self.keypair.pubkey(),
            gateway_root_pda,
            verification_session_account: verification_session_tracker_pda,
            verifier_set_tracker_pda,
            system_program: solana_program::system_program::id(),
        };

        let ix = Instruction {
            program_id: solana_axelar_gateway::ID,
            accounts: accounts.to_account_metas(None),
            data: ix_data,
        };

        let send_to_chain_res = self.send_gateway_tx_to_chain(ix, None, None).await;
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
                let ix_data = solana_axelar_gateway::instruction::VerifySignature {
                    payload_merkle_root: execute_data.payload_merkle_root,
                    verifier_info,
                }
                .data();

                let accounts = solana_axelar_gateway::accounts::VerifySignature {
                    gateway_root_pda,
                    verification_session_account: verification_session_tracker_pda,
                    verifier_set_tracker_pda,
                };
                let ix = Instruction {
                    program_id: solana_axelar_gateway::ID,
                    accounts: accounts.to_account_metas(None),
                    data: ix_data,
                };

                self.send_gateway_tx_to_chain(ix, None, None)
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
                    get_verifier_set_tracker_pda(&execute_data.signing_verifier_set_merkle_root);
                let ix_data = solana_axelar_gateway::instruction::RotateSigners {
                    new_verifier_set_merkle_root,
                }
                .data();
                let accounts = solana_axelar_gateway::accounts::RotateSigners {
                    payer: self.keypair.pubkey(),
                    program: solana_axelar_gateway::ID,
                    system_program: solana_program::system_program::id(),
                    gateway_root_pda,
                    verifier_set_tracker_pda,
                    operator: Some(self.keypair.pubkey()),
                    new_verifier_set_tracker: new_verifier_set_tracker_pda,
                    verification_session_account: verification_session_tracker_pda,
                    event_authority,
                };
                let ix = Instruction {
                    program_id: solana_axelar_gateway::ID,
                    accounts: accounts.to_account_metas(None),
                    data: ix_data,
                };
                let tx_res = self.send_gateway_tx_to_chain(ix, None, None).await;
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
                    .map(|merklized_message| {
                        let command_id = merklized_message.leaf.message.command_id();
                        let (pda, _) = get_incoming_message_pda(&command_id);

                        let msg_id = merklized_message.leaf.message.cc_id.id.clone();
                        let chain = merklized_message.leaf.message.cc_id.chain.clone();

                        let ix_data = solana_axelar_gateway::instruction::ApproveMessage {
                            merklized_message,
                            payload_merkle_root: execute_data.payload_merkle_root,
                        }
                        .data();
                        let accounts = solana_axelar_gateway::accounts::ApproveMessage {
                            funder: self.keypair.pubkey(),
                            incoming_message_pda: pda,
                            program: solana_axelar_gateway::ID,
                            system_program: solana_program::system_program::id(),
                            gateway_root_pda,
                            verification_session_account: verification_session_tracker_pda,
                            event_authority,
                        };
                        let ix = Instruction {
                            program_id: solana_axelar_gateway::ID,
                            accounts: accounts.to_account_metas(None),
                            data: ix_data,
                        };
                        self.send_gateway_tx_to_chain(ix, Some(msg_id), Some(chain))
                    })
                    .collect::<FuturesUnordered<_>>();

                let mut gmp_events = vec![];
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
                            gmp_events.push(event);
                        }
                    }
                }
                debug!("Approved messages transaction sent successfully");

                return Ok(gmp_events);
            }
        };
    }

    #[cfg_attr(
        feature = "instrumentation",
        tracing::instrument(skip(self), fields(message_id))
    )]
    async fn handle_execute_task(&self, task: ExecuteTask) -> Result<Vec<Event>, IncluderError> {
        let message = Message {
            cc_id: CrossChainId {
                chain: task.task.message.source_chain.clone(),
                id: task.task.message.message_id.clone(),
            },
            source_address: task.task.message.source_address.clone(),
            destination_chain: self.chain_name.clone(),
            destination_address: task.task.message.destination_address.clone(),
            payload_hash: base64::prelude::BASE64_STANDARD
                .decode(task.task.message.payload_hash.clone())
                .map_err(|e| {
                    IncluderError::GenericError(format!("Failed to decode payload hash: {}", e))
                })?
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

        let destination_address = match task.task.message.destination_address.parse::<Pubkey>() {
            Ok(pubkey) => pubkey,
            Err(e) => {
                let event = self.gmp_api.cannot_execute_message(
                    task.common.id.clone(),
                    task.task.message.message_id.clone(),
                    task.task.message.source_chain.clone(),
                    e.to_string(),
                    CannotExecuteMessageReason::Error,
                );
                return Ok(vec![event]);
            }
        };

        // Check if ALT already exists in Redis for this message_id
        let existing_alt_pubkey = self
            .redis_conn
            .get_alt_pubkey(task.task.message.message_id.clone())
            .await
            .map_err(|e| IncluderError::GenericError(e.to_string()))?;

        let (instruction, alt_info) = match self
            .transaction_builder
            .build_execute_instruction(
                &message,
                base64::prelude::BASE64_STANDARD
                    .decode(task.task.payload.clone())
                    .map_err(|e| IncluderError::GenericError(e.to_string()))?
                    .as_slice(),
                destination_address,
                existing_alt_pubkey,
            )
            .await
        {
            Ok((instruction, alt_info)) => (instruction, alt_info),
            Err(e) => match e {
                TransactionBuilderError::PayloadDecodeError(e) => {
                    let event = self.gmp_api.cannot_execute_message(
                        task.common.id.clone(),
                        task.task.message.message_id.clone(),
                        task.task.message.source_chain.clone(),
                        e,
                        CannotExecuteMessageReason::Error,
                    );
                    return Ok(vec![event]);
                }
                _ => {
                    return Err(IncluderError::GenericError(e.to_string()));
                }
            },
        };

        self.build_execute_transaction_and_send(instruction, task, alt_info)
            .await
            .map_err(|e| IncluderError::GenericError(e.to_string()))
    }

    #[cfg_attr(
        feature = "instrumentation",
        tracing::instrument(skip(self), fields(refund_id))
    )]
    async fn handle_refund_task(&self, task: RefundTask) -> Result<(), IncluderError> {
        let refund_id = task.common.id.clone();
        if self.refund_already_processed(refund_id.clone()).await? {
            warn!("Refund already processed: {}", refund_id);
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

        let accounts = solana_axelar_gas_service::accounts::RefundFees {
            operator: self.keypair.pubkey(),
            operator_pda,
            receiver,
            treasury,
            event_authority,
            program: solana_axelar_gas_service::ID,
        }
        .to_account_metas(None);

        let data = solana_axelar_gas_service::instruction::RefundFees {
            message_id: task.task.message.message_id.clone(),
            amount: refund_amount,
        }
        .data();

        let ix = Instruction {
            program_id: solana_axelar_gas_service::ID,
            accounts,
            data,
        };

        let (tx, estimated_tx_cost) = self
            .transaction_builder
            .build(&[ix], None)
            .await
            .map_err(|e| IncluderError::GenericError(e.to_string()))?;

        if estimated_tx_cost > refund_amount {
            return Err(IncluderError::GenericError(format!(
                "Cost is higher than remaining balance to refund. Cost: {}, Remaining balance: {}",
                estimated_tx_cost, refund_amount
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::includer_client::MockIncluderClientTrait;
    use crate::models::refunds::MockRefundsModel;
    use crate::redis::MockRedisConnectionTrait;
    use crate::transaction_builder::MockTransactionBuilderTrait;
    use crate::transaction_type::SolanaTransactionType;
    use base64::prelude::BASE64_STANDARD;
    use borsh::BorshSerialize;
    use relayer_core::gmp_api::MockGmpApiTrait;
    use solana_axelar_gateway::{
        MerklizedMessage, MessageLeaf, PublicKey, SigningVerifierSetInfo, VerifierSetLeaf,
    };
    use solana_sdk::address_lookup_table::AddressLookupTableAccount;
    use solana_sdk::compute_budget::ComputeBudgetInstruction;
    use solana_sdk::hash::Hash;
    use solana_sdk::instruction::AccountMeta;
    use solana_sdk::message::{v0, VersionedMessage};
    use solana_sdk::pubkey::Pubkey;
    use solana_sdk::transaction::{Transaction, VersionedTransaction};
    use solana_transaction_parser::gmp_types::{
        Amount, CannotExecuteMessageReason, CommonEventFields, CommonTaskFields, Event,
        ExecuteTask, ExecuteTaskFields, GatewayV2Message, RefundTask,
    };
    use solana_transaction_parser::gmp_types::{GatewayTxTaskFields, RefundTaskFields};
    use std::str::FromStr;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    impl Clone for MockIncluderClientTrait {
        fn clone(&self) -> Self {
            Self::new()
        }
    }

    impl Clone for MockRedisConnectionTrait {
        fn clone(&self) -> Self {
            Self::new()
        }
    }

    impl Clone for MockRefundsModel {
        fn clone(&self) -> Self {
            Self::new()
        }
    }

    impl Clone for MockTransactionBuilderTrait<MockIncluderClientTrait> {
        fn clone(&self) -> Self {
            Self::new()
        }
    }

    #[cfg(test)]
    fn get_includer_fields() -> (
        MockGmpApiTrait,
        Keypair,
        String,
        MockRedisConnectionTrait,
        MockRefundsModel,
        MockIncluderClientTrait,
        MockTransactionBuilderTrait<MockIncluderClientTrait>,
    ) {
        use crate::transaction_builder::MockTransactionBuilderTrait;

        let keypair = Keypair::new();
        let chain_name = "test-chain".to_string();

        let mock_client = MockIncluderClientTrait::new();
        let mock_refunds_model = MockRefundsModel::new();

        let mock_gmp_api = MockGmpApiTrait::new();
        let redis_conn = MockRedisConnectionTrait::new();

        let transaction_builder = MockTransactionBuilderTrait::new();

        (
            mock_gmp_api,
            keypair,
            chain_name,
            redis_conn,
            mock_refunds_model,
            mock_client,
            transaction_builder,
        )
    }

    #[tokio::test]
    async fn test_refund_already_processed_successful_transaction() {
        let (
            mock_gmp_api,
            keypair,
            chain_name,
            redis_conn,
            mut mock_refunds_model,
            mut mock_client,
            transaction_builder,
        ) = get_includer_fields();

        let refund_id = "test-message-123".to_string();
        let refund_id_clone = refund_id.clone();
        let signature_str = "4BmMcXeedDZ3p3sugJmtHTx2rHScRW6RYYXydjrSHUstDN4ELFVZRmWBqh5ZxPwoQ6WbhqwkUhnbDM341Qc8vHii";
        let signature = Signature::from_str(signature_str).unwrap();
        let updated_at = Utc::now() - chrono::Duration::seconds(30); // 30 seconds ago

        // Mock refunds_model.find to return the signature and updated_at
        mock_refunds_model
            .expect_find()
            .withf(move |id| *id == refund_id_clone)
            .times(1)
            .returning(move |_| {
                Box::pin(async move { Ok(Some((signature_str.to_string(), updated_at))) })
            });

        mock_client
            .expect_get_signature_status()
            .withf(move |sig| *sig == signature)
            .times(1)
            .returning(|_| Box::pin(async move { Ok(Some(Ok(()))) }));

        let includer = SolanaIncluder::new(
            Arc::new(mock_client),
            Arc::new(keypair),
            chain_name,
            transaction_builder,
            Arc::new(mock_gmp_api),
            redis_conn,
            Arc::new(mock_refunds_model),
        );

        let result = includer
            .refund_already_processed("test-message-123".to_string())
            .await;

        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[tokio::test]
    async fn test_refund_not_found() {
        let (
            mock_gmp_api,
            keypair,
            chain_name,
            redis_conn,
            mut mock_refunds_model,
            mock_client,
            transaction_builder,
        ) = get_includer_fields();

        let refund_id = "test-refund-123".to_string();
        let refund_id_clone = refund_id.clone();

        mock_refunds_model
            .expect_find()
            .withf(move |id| *id == refund_id_clone)
            .times(1)
            .returning(move |_| Box::pin(async move { Ok(None) }));
        let includer = SolanaIncluder::new(
            Arc::new(mock_client),
            Arc::new(keypair),
            chain_name,
            transaction_builder,
            Arc::new(mock_gmp_api),
            redis_conn,
            Arc::new(mock_refunds_model),
        );

        let result = includer.refund_already_processed(refund_id).await;

        assert!(result.is_ok());
        assert!(!result.unwrap());
    }

    #[tokio::test]
    async fn test_refund_already_processed_failed_transaction() {
        let (
            mock_gmp_api,
            keypair,
            chain_name,
            redis_conn,
            mut mock_refunds_model,
            mut mock_client,
            transaction_builder,
        ) = get_includer_fields();

        let refund_id = "test-refund-123".to_string();
        let refund_id_clone = refund_id.clone();
        let signature_str = "4BmMcXeedDZ3p3sugJmtHTx2rHScRW6RYYXydjrSHUstDN4ELFVZRmWBqh5ZxPwoQ6WbhqwkUhnbDM341Qc8vHii";
        let signature = Signature::from_str(signature_str).unwrap();
        let updated_at = Utc::now() - chrono::Duration::seconds(30); // 30 seconds ago

        mock_refunds_model
            .expect_find()
            .withf(move |id| *id == refund_id_clone)
            .times(1)
            .returning(move |_| {
                Box::pin(async move { Ok(Some((signature_str.to_string(), updated_at))) })
            });

        mock_client
            .expect_get_signature_status()
            .withf(move |sig| *sig == signature)
            .times(1)
            .returning(|_| {
                Box::pin(async move {
                    Ok(Some(Err(IncluderClientError::GenericError(
                        "Some Error".to_string(),
                    ))))
                })
            });

        let includer = SolanaIncluder::new(
            Arc::new(mock_client),
            Arc::new(keypair),
            chain_name,
            transaction_builder,
            Arc::new(mock_gmp_api),
            redis_conn,
            Arc::new(mock_refunds_model),
        );

        let result = includer.refund_already_processed(refund_id).await;

        assert!(result.is_ok());
        assert!(!result.unwrap());
    }

    #[tokio::test]
    async fn test_refund_already_processed_client_error() {
        let (
            mock_gmp_api,
            keypair,
            chain_name,
            redis_conn,
            mut mock_refunds_model,
            mut mock_client,
            transaction_builder,
        ) = get_includer_fields();

        let refund_id = "test-refund-123".to_string();
        let refund_id_clone = refund_id.clone();
        let signature_str = "4BmMcXeedDZ3p3sugJmtHTx2rHScRW6RYYXydjrSHUstDN4ELFVZRmWBqh5ZxPwoQ6WbhqwkUhnbDM341Qc8vHii";
        let signature = Signature::from_str(signature_str).unwrap();
        let updated_at = Utc::now() - chrono::Duration::seconds(30); // 30 seconds ago

        // Mock refunds_model.find to return the signature and updated_at
        mock_refunds_model
            .expect_find()
            .withf(move |id| *id == refund_id_clone)
            .times(1)
            .returning(move |_| {
                Box::pin(async move { Ok(Some((signature_str.to_string(), updated_at))) })
            });

        mock_client
            .expect_get_signature_status()
            .withf(move |sig| *sig == signature)
            .times(1)
            .returning(|_| {
                Box::pin(
                    async move { Err(IncluderClientError::GenericError("Some Error".to_string())) },
                )
            });

        let includer = SolanaIncluder::new(
            Arc::new(mock_client),
            Arc::new(keypair),
            chain_name,
            transaction_builder,
            Arc::new(mock_gmp_api),
            redis_conn,
            Arc::new(mock_refunds_model),
        );

        let result = includer.refund_already_processed(refund_id).await;

        assert!(result.is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn test_refund_already_processed_pending_on_chain() {
        let (
            mock_gmp_api,
            keypair,
            chain_name,
            redis_conn,
            mut mock_refunds_model,
            mut mock_client,
            transaction_builder,
        ) = get_includer_fields();

        let refund_id = "test-refund-789".to_string();
        let signature_str = "4BmMcXeedDZ3p3sugJmtHTx2rHScRW6RYYXydjrSHUstDN4ELFVZRmWBqh5ZxPwoQ6WbhqwkUhnbDM341Qc8vHii";
        let signature = Signature::from_str(signature_str).unwrap();
        // Make the stored timestamp already expired so the recursion terminates immediately.
        let updated_at = Utc::now() - chrono::Duration::seconds(SOLANA_EXPIRATION_TIME as i64 + 5);

        mock_refunds_model
            .expect_find()
            .withf(|id| id == "test-refund-789")
            .times(1)
            .returning(move |_| {
                Box::pin(async move { Ok(Some((signature_str.to_string(), updated_at))) })
            });

        mock_client
            .expect_get_signature_status()
            .withf(move |sig| *sig == signature)
            .times(1)
            .returning(|_| Box::pin(async move { Ok(None) }));

        let includer = SolanaIncluder::new(
            Arc::new(mock_client),
            Arc::new(keypair),
            chain_name,
            transaction_builder,
            Arc::new(mock_gmp_api),
            redis_conn,
            Arc::new(mock_refunds_model),
        );

        let result = includer.refund_already_processed(refund_id).await;

        assert!(result.is_ok());
        assert!(!result.unwrap());
    }

    #[tokio::test]
    async fn test_handle_refund_task_already_processed_success_on_chain() {
        let (
            mock_gmp_api,
            keypair,
            chain_name,
            redis_conn,
            mut mock_refunds_model,
            mut mock_client,
            transaction_builder,
        ) = get_includer_fields();

        let refund_id = "test-refund-task-123".to_string();
        let refund_id_clone = refund_id.clone();
        let signature_str = "4BmMcXeedDZ3p3sugJmtHTx2rHScRW6RYYXydjrSHUstDN4ELFVZRmWBqh5ZxPwoQ6WbhqwkUhnbDM341Qc8vHii";
        let signature = Signature::from_str(signature_str).unwrap();
        let updated_at = Utc::now() - chrono::Duration::seconds(30); // 30 seconds ago

        mock_refunds_model
            .expect_find()
            .withf(move |id| *id == refund_id_clone)
            .times(1)
            .returning(move |_| {
                Box::pin(async move { Ok(Some((signature_str.to_string(), updated_at))) })
            });

        mock_client
            .expect_get_signature_status()
            .withf(move |sig| *sig == signature)
            .times(1)
            .returning(|_| Box::pin(async move { Ok(Some(Ok(()))) }));

        let includer = SolanaIncluder::new(
            Arc::new(mock_client),
            Arc::new(keypair),
            chain_name,
            transaction_builder,
            Arc::new(mock_gmp_api),
            redis_conn,
            Arc::new(mock_refunds_model),
        );

        let result = includer
            .handle_refund_task(RefundTask {
                common: CommonTaskFields {
                    id: "test-refund-task-123".to_string(),
                    chain: "test-chain".to_string(),
                    timestamp: Utc::now().to_string(),
                    r#type: "refund".to_string(),
                    meta: None,
                },
                task: RefundTaskFields {
                    message: GatewayV2Message {
                        message_id: "test-message-123".to_string(),
                        source_chain: "test-chain".to_string(),
                        destination_address: "test-destination-123".to_string(),
                        payload_hash: "test-payload-hash-123".to_string(),
                        source_address: Pubkey::new_unique().to_string(),
                    },
                    refund_recipient_address: "test-recipient-123".to_string(),
                    remaining_gas_balance: Amount {
                        amount: "1000000000".to_string(),
                        token_id: None,
                    },
                },
            })
            .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_handle_refund_task_successful() {
        let (
            mock_gmp_api,
            keypair,
            chain_name,
            redis_conn,
            mut mock_refunds_model,
            mut mock_client,
            mut transaction_builder,
        ) = get_includer_fields();

        let refund_id = "test-refund-task-456".to_string();
        let refund_id_clone = refund_id.clone();
        let refund_recipient = Pubkey::new_unique();
        let refund_amount = 1000000000u64; // 1 billion lamports

        mock_refunds_model
            .expect_find()
            .withf(move |id| *id == refund_id_clone)
            .times(1)
            .returning(|_| Box::pin(async move { Ok(None) }));

        let mut test_tx =
            solana_sdk::transaction::Transaction::new_with_payer(&[], Some(&keypair.pubkey()));
        test_tx.sign(&[&keypair], solana_sdk::hash::Hash::default());
        let test_signature = test_tx.signatures[0];

        let test_tx_for_build = test_tx.clone();
        transaction_builder
            .expect_build()
            .times(1)
            .returning(move |_, _| {
                Ok((
                    crate::transaction_type::SolanaTransactionType::Legacy(
                        test_tx_for_build.clone(),
                    ),
                    100_000u64,
                ))
            });

        // This will result in gas_cost_lamports < refund_amount
        let compute_units = 100000u64;
        mock_client
            .expect_get_units_consumed_from_simulation()
            .times(1)
            .returning(move |_| Box::pin(async move { Ok(compute_units) }));

        let refund_id_for_upsert = refund_id.clone();
        let signature_str_for_upsert = test_signature.to_string();
        mock_refunds_model
            .expect_upsert()
            .withf(move |id, sig| *id == refund_id_for_upsert && *sig == signature_str_for_upsert)
            .times(1)
            .returning(|_, _| Box::pin(async move { Ok(()) }));

        mock_client
            .expect_send_transaction()
            .times(1)
            .returning(move |_| Box::pin(async move { Ok((test_signature, Some(5000u64))) }));

        let includer = SolanaIncluder::new(
            Arc::new(mock_client),
            Arc::new(keypair),
            chain_name,
            transaction_builder,
            Arc::new(mock_gmp_api),
            redis_conn,
            Arc::new(mock_refunds_model),
        );

        let result = includer
            .handle_refund_task(RefundTask {
                common: CommonTaskFields {
                    id: refund_id,
                    chain: "test-chain".to_string(),
                    timestamp: Utc::now().to_string(),
                    r#type: "refund".to_string(),
                    meta: None,
                },
                task: RefundTaskFields {
                    message: GatewayV2Message {
                        message_id: "test-message-456".to_string(),
                        source_chain: "test-chain".to_string(),
                        destination_address: "test-destination-456".to_string(),
                        payload_hash: "test-payload-hash-456".to_string(),
                        source_address: Pubkey::new_unique().to_string(),
                    },
                    refund_recipient_address: refund_recipient.to_string(),
                    remaining_gas_balance: Amount {
                        amount: refund_amount.to_string(),
                        token_id: None,
                    },
                },
            })
            .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_handle_refund_task_gas_cost_exceeds_refund_amount() {
        let (
            mock_gmp_api,
            keypair,
            chain_name,
            redis_conn,
            mut mock_refunds_model,
            mut mock_client,
            mut transaction_builder,
        ) = get_includer_fields();

        let refund_id = "test-refund-task-789".to_string();
        let refund_id_clone = refund_id.clone();
        let refund_recipient = Pubkey::new_unique();
        let refund_amount = 1_000_000u64; // 1 million lamports

        mock_refunds_model
            .expect_find()
            .withf(move |id| *id == refund_id_clone)
            .times(1)
            .returning(|_| Box::pin(async move { Ok(None) }));

        // The function should return an error before calling upsert or send_transaction
        mock_refunds_model.expect_upsert().times(0);

        // Mock transaction_builder.build to return a transaction with a high compute unit price
        let high_micro_price = 2_000_000_000_000u64;
        let keypair_bytes = keypair.to_bytes();
        let keypair_for_mock = Keypair::try_from(&keypair_bytes[..]).unwrap();
        transaction_builder
            .expect_build()
            .times(1)
            .returning(move |ixs, _| {
                // Build a transaction that includes both the refund instruction AND compute budget
                let mut all_ixs = vec![ComputeBudgetInstruction::set_compute_unit_price(
                    high_micro_price,
                )];
                all_ixs.extend_from_slice(ixs);

                let test_tx = solana_sdk::transaction::Transaction::new_signed_with_payer(
                    &all_ixs,
                    Some(&keypair_for_mock.pubkey()),
                    &[&keypair_for_mock],
                    solana_sdk::hash::Hash::default(),
                );
                Ok((
                    crate::transaction_type::SolanaTransactionType::Legacy(test_tx),
                    100_000u64,
                ))
            });

        // This will result in gas_cost_lamports > refund_amount
        let compute_units = 5_000_000u64;
        mock_client
            .expect_get_units_consumed_from_simulation()
            .times(1)
            .returning(move |_| Box::pin(async move { Ok(compute_units) }));

        // Should not reach send_transaction since the check fails
        mock_client.expect_send_transaction().times(0);

        let includer = SolanaIncluder::new(
            Arc::new(mock_client),
            Arc::new(keypair),
            chain_name,
            transaction_builder,
            Arc::new(mock_gmp_api),
            redis_conn,
            Arc::new(mock_refunds_model),
        );

        let result = includer
            .handle_refund_task(RefundTask {
                common: CommonTaskFields {
                    id: refund_id,
                    chain: "test-chain".to_string(),
                    timestamp: Utc::now().to_string(),
                    r#type: "refund".to_string(),
                    meta: None,
                },
                task: RefundTaskFields {
                    message: GatewayV2Message {
                        message_id: "test-message-789".to_string(),
                        source_chain: "test-chain".to_string(),
                        destination_address: "test-destination-789".to_string(),
                        payload_hash: "test-payload-hash-789".to_string(),
                        source_address: Pubkey::new_unique().to_string(),
                    },
                    refund_recipient_address: refund_recipient.to_string(),
                    remaining_gas_balance: Amount {
                        amount: refund_amount.to_string(),
                        token_id: None,
                    },
                },
            })
            .await;

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Cost is higher than remaining balance"));
    }

    #[tokio::test]
    async fn test_handle_execute_task_governance_success() {
        let (
            mock_gmp_api,
            keypair,
            chain_name,
            mut redis_conn,
            mock_refunds_model,
            mut mock_client,
            mut transaction_builder,
        ) = get_includer_fields();

        let message_id = "test-execute-governance-123".to_string();
        let message_id_clone = message_id.clone();
        let source_chain = "ethereum".to_string();
        let destination_address = solana_axelar_governance::ID.to_string();
        let available_gas = 5000u64; // enough lamports to cover a 0 prio fee + 1 signature cost
        let payload_hash = BASE64_STANDARD.encode([0u8; 32]);

        mock_client
            .expect_incoming_message_already_executed()
            .times(1)
            .returning(|_| Box::pin(async move { Ok(false) }));

        redis_conn
            .expect_get_alt_pubkey()
            .withf(move |id| *id == message_id_clone)
            .times(1)
            .returning(|_| Ok(None));

        let test_instruction =
            Instruction::new_with_bytes(solana_axelar_governance::ID, &[1, 2, 3, 4], vec![]);
        let instruction_for_mock = test_instruction.clone();
        transaction_builder
            .expect_build_execute_instruction()
            .times(1)
            .returning(move |_, _, _, _| {
                Ok((
                    instruction_for_mock.clone(),
                    None, // No ALT for governance
                ))
            });

        let mut test_tx =
            solana_sdk::transaction::Transaction::new_with_payer(&[], Some(&keypair.pubkey()));
        test_tx.sign(&[&keypair], solana_sdk::hash::Hash::default());
        let test_signature = test_tx.signatures[0];
        let test_tx_for_build = test_tx.clone();
        transaction_builder
            .expect_build()
            .times(1)
            .returning(move |_, _| {
                Ok((
                    crate::transaction_type::SolanaTransactionType::Legacy(
                        test_tx_for_build.clone(),
                    ),
                    100_000u64,
                ))
            });

        let message_id_clone = message_id.clone();

        // cost < available gas
        let compute_units = 100_000u64;
        mock_client
            .expect_get_units_consumed_from_simulation()
            .times(1)
            .returning(move |_| Box::pin(async move { Ok(compute_units) }));

        mock_client
            .expect_send_transaction()
            .times(1)
            .returning(move |_| Box::pin(async move { Ok((test_signature, Some(5_000u64))) }));

        redis_conn
            .expect_write_gas_cost()
            .withf(move |id, cost, tx_type| {
                *id == message_id_clone
                    && *cost == 5_000u64
                    && matches!(tx_type, TransactionType::Execute)
            })
            .times(1)
            .returning(|_, _, _| ());

        let includer = SolanaIncluder::new(
            Arc::new(mock_client),
            Arc::new(keypair),
            chain_name,
            transaction_builder,
            Arc::new(mock_gmp_api),
            redis_conn,
            Arc::new(mock_refunds_model),
        );

        let result = includer
            .handle_execute_task(ExecuteTask {
                common: CommonTaskFields {
                    id: "test-execute-task-123".to_string(),
                    chain: "test-chain".to_string(),
                    timestamp: Utc::now().to_string(),
                    r#type: "execute".to_string(),
                    meta: None,
                },
                task: ExecuteTaskFields {
                    message: GatewayV2Message {
                        message_id: message_id.clone(),
                        source_chain: source_chain.clone(),
                        destination_address: destination_address.clone(),
                        payload_hash: payload_hash.clone(),
                        source_address: Pubkey::new_unique().to_string(),
                    },
                    payload: "test-payload".to_string(),
                    available_gas_balance: Amount {
                        amount: available_gas.to_string(),
                        token_id: None,
                    },
                },
            })
            .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), vec![]);
    }

    #[tokio::test]
    async fn test_handle_execute_task_governance_insufficient_gas() {
        let (
            mut mock_gmp_api,
            keypair,
            chain_name,
            mut redis_conn,
            mock_refunds_model,
            mut mock_client,
            mut transaction_builder,
        ) = get_includer_fields();

        let message_id = "test-execute-governance-456".to_string();
        let message_id_clone = message_id.clone();
        let source_chain = "ethereum".to_string();
        let destination_address = solana_axelar_governance::ID.to_string();
        let available_gas = 1_000u64;
        let payload_hash = BASE64_STANDARD.encode([1u8; 32]);

        mock_client
            .expect_incoming_message_already_executed()
            .times(1)
            .returning(|_| Box::pin(async move { Ok(false) }));

        redis_conn
            .expect_get_alt_pubkey()
            .withf(move |id| *id == message_id_clone)
            .times(1)
            .returning(|_| Ok(None));

        let test_instruction =
            Instruction::new_with_bytes(solana_axelar_governance::ID, &[1, 2, 3, 4], vec![]);
        let instruction_for_mock = test_instruction.clone();
        transaction_builder
            .expect_build_execute_instruction()
            .times(1)
            .returning(move |_, _, _, _| Ok((instruction_for_mock.clone(), None)));

        let mut test_tx =
            solana_sdk::transaction::Transaction::new_with_payer(&[], Some(&keypair.pubkey()));
        test_tx.sign(&[&keypair], solana_sdk::hash::Hash::default());
        let test_tx_for_build = test_tx.clone();
        transaction_builder
            .expect_build()
            .times(1)
            .returning(move |_, _| {
                Ok((
                    crate::transaction_type::SolanaTransactionType::Legacy(
                        test_tx_for_build.clone(),
                    ),
                    100_000u64,
                ))
            });

        let compute_units = 100_000u64;
        mock_client
            .expect_get_units_consumed_from_simulation()
            .times(1)
            .returning(move |_| Box::pin(async move { Ok(compute_units) }));

        mock_client.expect_send_transaction().times(0);
        redis_conn.expect_write_gas_cost().times(0);

        let message_id_clone = message_id.clone();
        let source_chain_clone = source_chain.clone();
        mock_gmp_api
            .expect_cannot_execute_message()
            .withf(move |id, msg_id, src_chain, details, reason| {
                *id == "test-execute-task-456"
                    && *msg_id == message_id_clone
                    && *src_chain == source_chain_clone
                    && details.contains("Not enough gas")
                    && matches!(reason, CannotExecuteMessageReason::InsufficientGas)
            })
            .times(1)
            .returning(|_, _, _, _, _| Event::CannotExecuteMessageV2 {
                common: CommonEventFields {
                    r#type: "CANNOT_EXECUTE_MESSAGE/V2".to_string(),
                    event_id: "test-event".to_string(),
                    meta: None,
                },
                message_id: "test".to_string(),
                source_chain: "test".to_string(),
                reason: CannotExecuteMessageReason::InsufficientGas,
                details: "test".to_string(),
            });

        let includer = SolanaIncluder::new(
            Arc::new(mock_client),
            Arc::new(keypair),
            chain_name,
            transaction_builder,
            Arc::new(mock_gmp_api),
            redis_conn,
            Arc::new(mock_refunds_model),
        );

        let result = includer
            .handle_execute_task(ExecuteTask {
                common: CommonTaskFields {
                    id: "test-execute-task-456".to_string(),
                    chain: "test-chain".to_string(),
                    timestamp: Utc::now().to_string(),
                    r#type: "execute".to_string(),
                    meta: None,
                },
                task: ExecuteTaskFields {
                    message: GatewayV2Message {
                        message_id: message_id.clone(),
                        source_chain: source_chain.clone(),
                        destination_address: destination_address.clone(),
                        payload_hash: payload_hash.clone(),
                        source_address: Pubkey::new_unique().to_string(),
                    },
                    payload: "test-payload".to_string(),
                    available_gas_balance: Amount {
                        amount: available_gas.to_string(),
                        token_id: None,
                    },
                },
            })
            .await;

        assert!(result.is_ok());
        let events = result.unwrap();
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], Event::CannotExecuteMessageV2 { .. }));
    }

    #[tokio::test]
    async fn test_handle_execute_task_executable_success() {
        let (
            mock_gmp_api,
            keypair,
            chain_name,
            mut redis_conn,
            mock_refunds_model,
            mut mock_client,
            mut transaction_builder,
        ) = get_includer_fields();

        let message_id = "test-execute-executable-789".to_string();
        let source_chain = "polygon".to_string();
        let destination_address = Pubkey::new_unique().to_string(); // Arbitrary program
        let available_gas = 10_000_000_000u64;
        let payload_hash = BASE64_STANDARD.encode([2u8; 32]);

        let message_id_clone = message_id.clone();

        mock_client
            .expect_incoming_message_already_executed()
            .times(1)
            .returning(|_| Box::pin(async move { Ok(false) }));

        redis_conn
            .expect_get_alt_pubkey()
            .withf(move |id| *id == message_id_clone)
            .times(1)
            .returning(|_| Ok(None));

        let executable_program = Pubkey::new_unique();
        let test_instruction =
            Instruction::new_with_bytes(executable_program, &[5, 6, 7, 8], vec![]);
        transaction_builder
            .expect_build_execute_instruction()
            .times(1)
            .returning(move |_, _, _, _| {
                Ok((
                    test_instruction.clone(),
                    None, // No ALT for executable
                ))
            });

        let mut test_tx =
            solana_sdk::transaction::Transaction::new_with_payer(&[], Some(&keypair.pubkey()));
        test_tx.sign(&[&keypair], solana_sdk::hash::Hash::default());
        let test_signature = test_tx.signatures[0];
        let test_tx_for_build = test_tx.clone();
        transaction_builder
            .expect_build()
            .times(1)
            .returning(move |_, _| {
                Ok((
                    crate::transaction_type::SolanaTransactionType::Legacy(
                        test_tx_for_build.clone(),
                    ),
                    100_000u64,
                ))
            });

        let message_id_clone = message_id.clone();

        let compute_units = 100_000u64;
        mock_client
            .expect_get_units_consumed_from_simulation()
            .times(1)
            .returning(move |_| Box::pin(async move { Ok(compute_units) }));

        mock_client
            .expect_send_transaction()
            .times(1)
            .returning(move |_| Box::pin(async move { Ok((test_signature, Some(5_000u64))) }));

        redis_conn
            .expect_write_gas_cost()
            .withf(move |id, cost, tx_type| {
                *id == message_id_clone
                    && *cost == 5_000u64
                    && matches!(tx_type, TransactionType::Execute)
            })
            .times(1)
            .returning(|_, _, _| ());

        let includer = SolanaIncluder::new(
            Arc::new(mock_client),
            Arc::new(keypair),
            chain_name,
            transaction_builder,
            Arc::new(mock_gmp_api),
            redis_conn,
            Arc::new(mock_refunds_model),
        );

        let result = includer
            .handle_execute_task(ExecuteTask {
                common: CommonTaskFields {
                    id: "test-execute-task-789".to_string(),
                    chain: "test-chain".to_string(),
                    timestamp: Utc::now().to_string(),
                    r#type: "execute".to_string(),
                    meta: None,
                },
                task: ExecuteTaskFields {
                    message: GatewayV2Message {
                        message_id: message_id.clone(),
                        source_chain: source_chain.clone(),
                        destination_address: destination_address.clone(),
                        payload_hash: payload_hash.clone(),
                        source_address: Pubkey::new_unique().to_string(),
                    },
                    payload: "test-payload".to_string(),
                    available_gas_balance: Amount {
                        amount: available_gas.to_string(),
                        token_id: None,
                    },
                },
            })
            .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), vec![]);
    }

    #[tokio::test]
    async fn test_handle_execute_task_executable_insufficient_gas() {
        let (
            mut mock_gmp_api,
            keypair,
            chain_name,
            mut redis_conn,
            mock_refunds_model,
            mut mock_client,
            mut transaction_builder,
        ) = get_includer_fields();

        let message_id = "test-execute-executable-999".to_string();
        let source_chain = "avalanche".to_string();
        let destination_address = Pubkey::new_unique().to_string();
        let available_gas = 1_000u64;
        let payload_hash = BASE64_STANDARD.encode([3u8; 32]);

        mock_client
            .expect_incoming_message_already_executed()
            .times(1)
            .returning(|_| Box::pin(async move { Ok(false) }));

        let message_id_clone = message_id.clone();

        redis_conn
            .expect_get_alt_pubkey()
            .withf(move |id| *id == message_id_clone)
            .times(1)
            .returning(|_| Ok(None));

        let executable_program = Pubkey::new_unique();
        let test_instruction =
            Instruction::new_with_bytes(executable_program, &[9, 10, 11, 12], vec![]);
        let instruction_for_mock = test_instruction.clone();
        transaction_builder
            .expect_build_execute_instruction()
            .times(1)
            .returning(move |_, _, _, _| Ok((instruction_for_mock.clone(), None)));

        let mut test_tx =
            solana_sdk::transaction::Transaction::new_with_payer(&[], Some(&keypair.pubkey()));
        test_tx.sign(&[&keypair], solana_sdk::hash::Hash::default());
        let test_tx_for_build = test_tx.clone();
        transaction_builder
            .expect_build()
            .times(1)
            .returning(move |_, _| {
                Ok((
                    crate::transaction_type::SolanaTransactionType::Legacy(
                        test_tx_for_build.clone(),
                    ),
                    100_000u64,
                ))
            });

        let compute_units = 100_000u64;
        mock_client
            .expect_get_units_consumed_from_simulation()
            .times(1)
            .returning(move |_| Box::pin(async move { Ok(compute_units) }));

        // Should not reach send_transaction
        mock_client.expect_send_transaction().times(0);
        redis_conn.expect_write_gas_cost().times(0);
        let message_id_clone = message_id.clone();
        let source_chain_clone = source_chain.clone();

        mock_gmp_api
            .expect_cannot_execute_message()
            .withf(move |id, msg_id, src_chain, details, reason| {
                *id == "test-execute-task-999"
                    && *msg_id == message_id_clone
                    && *src_chain == source_chain_clone
                    && details.contains("Not enough gas")
                    && matches!(reason, CannotExecuteMessageReason::InsufficientGas)
            })
            .times(1)
            .returning(|_, _, _, _, _| Event::CannotExecuteMessageV2 {
                common: CommonEventFields {
                    r#type: "CANNOT_EXECUTE_MESSAGE/V2".to_string(),
                    event_id: "test-event".to_string(),
                    meta: None,
                },
                message_id: "test".to_string(),
                source_chain: "test".to_string(),
                reason: CannotExecuteMessageReason::InsufficientGas,
                details: "test".to_string(),
            });

        let includer = SolanaIncluder::new(
            Arc::new(mock_client),
            Arc::new(keypair),
            chain_name,
            transaction_builder,
            Arc::new(mock_gmp_api),
            redis_conn,
            Arc::new(mock_refunds_model),
        );

        let result = includer
            .handle_execute_task(ExecuteTask {
                common: CommonTaskFields {
                    id: "test-execute-task-999".to_string(),
                    chain: "test-chain".to_string(),
                    timestamp: Utc::now().to_string(),
                    r#type: "execute".to_string(),
                    meta: None,
                },
                task: ExecuteTaskFields {
                    message: GatewayV2Message {
                        message_id: message_id.clone(),
                        source_chain: source_chain.clone(),
                        destination_address: destination_address.clone(),
                        payload_hash: payload_hash.clone(),
                        source_address: Pubkey::new_unique().to_string(),
                    },
                    payload: "test-payload".to_string(),
                    available_gas_balance: Amount {
                        amount: available_gas.to_string(),
                        token_id: None,
                    },
                },
            })
            .await;

        assert!(result.is_ok());
        let events = result.unwrap();
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], Event::CannotExecuteMessageV2 { .. }));
    }

    #[tokio::test]
    async fn test_handle_execute_task_invalid_destination_address_returns_event() {
        let (
            mut mock_gmp_api,
            keypair,
            chain_name,
            redis_conn,
            mock_refunds_model,
            mut mock_client,
            transaction_builder,
        ) = get_includer_fields();

        let message_id = "test-execute-invalid-destination-001".to_string();
        let source_chain = "ethereum".to_string();
        let destination_address = "not-a-valid-pubkey".to_string();
        let parse_error = Pubkey::from_str(&destination_address)
            .unwrap_err()
            .to_string();
        let payload_hash = BASE64_STANDARD.encode([8u8; 32]);

        mock_client
            .expect_incoming_message_already_executed()
            .times(1)
            .returning(|_| Box::pin(async move { Ok(false) }));

        let message_id_clone = message_id.clone();
        let source_chain_clone = source_chain.clone();
        let parse_error_clone = parse_error.clone();
        mock_gmp_api
            .expect_cannot_execute_message()
            .withf(move |id, msg_id, src_chain, details, reason| {
                *id == "test-execute-task-invalid-destination-001"
                    && *msg_id == message_id_clone
                    && *src_chain == source_chain_clone
                    && *details == parse_error_clone
                    && matches!(reason, CannotExecuteMessageReason::Error)
            })
            .times(1)
            .returning(|_, _, _, _, _| Event::CannotExecuteMessageV2 {
                common: CommonEventFields {
                    r#type: "CANNOT_EXECUTE_MESSAGE/V2".to_string(),
                    event_id: "test-event".to_string(),
                    meta: None,
                },
                message_id: "test".to_string(),
                source_chain: "test".to_string(),
                reason: CannotExecuteMessageReason::Error,
                details: "test".to_string(),
            });

        let includer = SolanaIncluder::new(
            Arc::new(mock_client),
            Arc::new(keypair),
            chain_name,
            transaction_builder,
            Arc::new(mock_gmp_api),
            redis_conn,
            Arc::new(mock_refunds_model),
        );

        let result = includer
            .handle_execute_task(ExecuteTask {
                common: CommonTaskFields {
                    id: "test-execute-task-invalid-destination-001".to_string(),
                    chain: "test-chain".to_string(),
                    timestamp: Utc::now().to_string(),
                    r#type: "execute".to_string(),
                    meta: None,
                },
                task: ExecuteTaskFields {
                    message: GatewayV2Message {
                        message_id: message_id.clone(),
                        source_chain: source_chain.clone(),
                        destination_address: destination_address.clone(),
                        payload_hash: payload_hash.clone(),
                        source_address: Pubkey::new_unique().to_string(),
                    },
                    payload: "test-payload".to_string(),
                    available_gas_balance: Amount {
                        amount: "1000".to_string(),
                        token_id: None,
                    },
                },
            })
            .await;

        assert!(result.is_ok());
        let events = result.unwrap();
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], Event::CannotExecuteMessageV2 { .. }));
    }

    #[tokio::test]
    async fn test_handle_execute_task_payload_decode_error_returns_event() {
        let (
            mut mock_gmp_api,
            keypair,
            chain_name,
            mut redis_conn,
            mock_refunds_model,
            mut mock_client,
            mut transaction_builder,
        ) = get_includer_fields();

        let message_id = "test-execute-payload-decode-error-001".to_string();
        let source_chain = "polygon".to_string();
        let destination_address = solana_axelar_governance::ID.to_string();
        let payload_hash = BASE64_STANDARD.encode([9u8; 32]);
        let payload_decode_error = "Failed to decode payload".to_string();

        mock_client
            .expect_incoming_message_already_executed()
            .times(1)
            .returning(|_| Box::pin(async move { Ok(false) }));

        let message_id_clone = message_id.clone();
        redis_conn
            .expect_get_alt_pubkey()
            .withf(move |id| *id == message_id_clone)
            .times(1)
            .returning(|_| Ok(None));

        let payload_decode_error_clone = payload_decode_error.clone();
        transaction_builder
            .expect_build_execute_instruction()
            .times(1)
            .returning(move |_, _, _, _| {
                Err(TransactionBuilderError::PayloadDecodeError(
                    payload_decode_error_clone.clone(),
                ))
            });

        let message_id_clone = message_id.clone();
        let source_chain_clone = source_chain.clone();
        let payload_decode_error_clone = payload_decode_error.clone();
        mock_gmp_api
            .expect_cannot_execute_message()
            .withf(move |id, msg_id, src_chain, details, reason| {
                *id == "test-execute-task-payload-decode-error-001"
                    && *msg_id == message_id_clone
                    && *src_chain == source_chain_clone
                    && *details == payload_decode_error_clone
                    && matches!(reason, CannotExecuteMessageReason::Error)
            })
            .times(1)
            .returning(|_, _, _, _, _| Event::CannotExecuteMessageV2 {
                common: CommonEventFields {
                    r#type: "CANNOT_EXECUTE_MESSAGE/V2".to_string(),
                    event_id: "test-event".to_string(),
                    meta: None,
                },
                message_id: "test".to_string(),
                source_chain: "test".to_string(),
                reason: CannotExecuteMessageReason::Error,
                details: "test".to_string(),
            });

        let includer = SolanaIncluder::new(
            Arc::new(mock_client),
            Arc::new(keypair),
            chain_name,
            transaction_builder,
            Arc::new(mock_gmp_api),
            redis_conn,
            Arc::new(mock_refunds_model),
        );

        let result = includer
            .handle_execute_task(ExecuteTask {
                common: CommonTaskFields {
                    id: "test-execute-task-payload-decode-error-001".to_string(),
                    chain: "test-chain".to_string(),
                    timestamp: Utc::now().to_string(),
                    r#type: "execute".to_string(),
                    meta: None,
                },
                task: ExecuteTaskFields {
                    message: GatewayV2Message {
                        message_id: message_id.clone(),
                        source_chain: source_chain.clone(),
                        destination_address: destination_address.clone(),
                        payload_hash: payload_hash.clone(),
                        source_address: Pubkey::new_unique().to_string(),
                    },
                    payload: "malformed-payload".to_string(),
                    available_gas_balance: Amount {
                        amount: "1000".to_string(),
                        token_id: None,
                    },
                },
            })
            .await;

        assert!(result.is_ok());
        let events = result.unwrap();
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], Event::CannotExecuteMessageV2 { .. }));
    }

    #[tokio::test]
    async fn handle_execute_its_task_happy_path_with_alt() {
        let (
            mock_gmp_api,
            keypair,
            chain_name,
            mut redis_conn,
            mock_refunds_model,
            mut mock_client,
            mut transaction_builder,
        ) = get_includer_fields();

        let message_id = "test-execute-task-its-123".to_string();
        let available_gas = 15_000u64;

        mock_client
            .expect_incoming_message_already_executed()
            .times(1)
            .returning(|_| Box::pin(async { Ok(false) }));

        redis_conn
            .expect_get_alt_pubkey()
            .times(1)
            .returning(|_| Ok(None));

        let alt_pubkey = Pubkey::new_unique();
        let alt_addresses = vec![Pubkey::new_unique()];
        let alt_addresses_for_builder = alt_addresses.clone();

        let exec_ix = Instruction::new_with_bytes(
            solana_axelar_its::ID,
            &[],
            vec![AccountMeta::new(keypair.pubkey(), true)],
        );

        let alt_ix_create =
            Instruction::new_with_bytes(solana_program::system_program::ID, &[1], vec![]);
        let alt_ix_extend =
            Instruction::new_with_bytes(solana_program::system_program::ID, &[2], vec![]);

        let alt_info = ALTInfo::new(
            Some(alt_ix_create.clone()),
            Some(alt_ix_extend.clone()),
            Some(alt_pubkey),
        )
        .with_addresses(alt_addresses_for_builder.clone());

        let exec_ix_for_builder = exec_ix.clone();
        let alt_info_for_builder = alt_info.clone();

        transaction_builder
            .expect_build_execute_instruction()
            .times(1)
            .returning(move |_, _, _, _| {
                Ok((
                    exec_ix_for_builder.clone(),
                    Some(alt_info_for_builder.clone()),
                ))
            });

        let lookup_account = AddressLookupTableAccount {
            key: alt_pubkey,
            addresses: alt_addresses.clone(),
        };

        let v0_msg = v0::Message::try_compile(
            &keypair.pubkey(),
            std::slice::from_ref(&exec_ix),
            &[lookup_account],
            Hash::default(),
        )
        .unwrap();

        let main_tx =
            VersionedTransaction::try_new(VersionedMessage::V0(v0_msg), &[&keypair]).unwrap();

        let mut alt_tx = Transaction::new_with_payer(
            &[alt_ix_create.clone(), alt_ix_extend.clone()],
            Some(&keypair.pubkey()),
        );
        alt_tx.sign(&[&keypair], Hash::default());

        let build_calls = Arc::new(AtomicUsize::new(0));
        let build_calls_clone = Arc::clone(&build_calls);
        let main_tx_clone = main_tx.clone();
        let alt_tx_clone = alt_tx.clone();

        transaction_builder
            .expect_build()
            .times(2)
            .returning(move |_, _| {
                let idx = build_calls_clone.fetch_add(1, Ordering::SeqCst);
                if idx == 0 {
                    Ok((
                        SolanaTransactionType::Versioned(main_tx_clone.clone()),
                        100_000u64,
                    ))
                } else {
                    Ok((
                        SolanaTransactionType::Legacy(alt_tx_clone.clone()),
                        100_000u64,
                    ))
                }
            });

        let sim_calls = Arc::new(AtomicUsize::new(0));
        let sim_calls_clone = Arc::clone(&sim_calls);
        mock_client
            .expect_get_units_consumed_from_simulation()
            .times(2)
            .returning(move |_| {
                let idx = sim_calls_clone.fetch_add(1, Ordering::SeqCst);
                let units = if idx == 0 { 100_000 } else { 80_000 };
                Box::pin(async move { Ok(units) })
            });

        mock_client
            .expect_send_transaction()
            .times(2)
            .returning(|_| {
                static CALL: AtomicUsize = AtomicUsize::new(0);
                let idx = CALL.fetch_add(1, Ordering::SeqCst);
                if idx == 0 {
                    Box::pin(async { Ok((Signature::default(), Some(5_000))) })
                } else {
                    Box::pin(async { Ok((Signature::default(), Some(4_000))) })
                }
            });

        let msg_id_for_alt = message_id.clone();
        let alt_pubkey_for_expect = alt_pubkey;
        redis_conn
            .expect_write_alt_pubkey()
            .times(1)
            .withf(move |id, pubkey| id == &msg_id_for_alt && *pubkey == alt_pubkey_for_expect)
            .returning(|_, _| Ok(()));

        let msg_id_for_cost = message_id.clone();
        redis_conn
            .expect_write_gas_cost()
            .times(1)
            .withf(move |id, cost, tx_type| {
                id == &msg_id_for_cost
                    && *cost == 9_000
                    && matches!(tx_type, TransactionType::Execute)
            })
            .returning(|_, _, _| ());

        let includer = SolanaIncluder::new(
            Arc::new(mock_client),
            Arc::new(keypair),
            chain_name,
            transaction_builder,
            Arc::new(mock_gmp_api),
            redis_conn,
            Arc::new(mock_refunds_model),
        );

        let execute_task = ExecuteTask {
            common: CommonTaskFields {
                id: format!("test-{}", message_id),
                chain: "test-chain".to_string(),
                timestamp: Utc::now().to_string(),
                r#type: "execute".to_string(),
                meta: None,
            },
            task: ExecuteTaskFields {
                message: GatewayV2Message {
                    message_id: message_id.clone(),
                    source_chain: "ethereum".to_string(),
                    destination_address: solana_axelar_its::ID.to_string(),
                    payload_hash: BASE64_STANDARD.encode([4u8; 32]),
                    source_address: "test-source-address".to_string(),
                },
                payload: "test-payload".to_string(),
                available_gas_balance: Amount {
                    amount: available_gas.to_string(),
                    token_id: None,
                },
            },
        };

        let result = includer.handle_execute_task(execute_task).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), vec![]);
    }

    #[tokio::test]
    async fn handle_execute_its_task_insufficient_gas_due_to_alt() {
        let (
            mut mock_gmp_api,
            keypair,
            chain_name,
            mut redis_conn,
            mock_refunds_model,
            mut mock_client,
            mut transaction_builder,
        ) = get_includer_fields();

        let message_id = "test-execute-its-456".to_string();
        let available_gas = 9_000u64;

        mock_client
            .expect_incoming_message_already_executed()
            .times(1)
            .returning(|_| Box::pin(async { Ok(false) }));

        redis_conn
            .expect_get_alt_pubkey()
            .times(1)
            .returning(|_| Ok(None));

        let alt_pubkey = Pubkey::new_unique();
        let alt_addresses = vec![Pubkey::new_unique()];
        let alt_addresses_for_builder = alt_addresses.clone();

        let exec_ix = Instruction::new_with_bytes(
            solana_axelar_its::ID,
            &[21, 22, 23, 24],
            vec![
                AccountMeta::new(keypair.pubkey(), true),
                AccountMeta::new_readonly(alt_addresses[0], false),
            ],
        );

        let alt_ix_create =
            Instruction::new_with_bytes(solana_program::system_program::ID, &[3], vec![]);
        let alt_ix_extend =
            Instruction::new_with_bytes(solana_program::system_program::ID, &[4], vec![]);

        let alt_info = ALTInfo::new(
            Some(alt_ix_create.clone()),
            Some(alt_ix_extend.clone()),
            Some(alt_pubkey),
        )
        .with_addresses(alt_addresses_for_builder.clone());

        let exec_ix_for_builder = exec_ix.clone();
        let alt_info_for_builder = alt_info.clone();

        transaction_builder
            .expect_build_execute_instruction()
            .times(1)
            .returning(move |_, _, _, _| {
                Ok((
                    exec_ix_for_builder.clone(),
                    Some(alt_info_for_builder.clone()),
                ))
            });

        let lookup_account = AddressLookupTableAccount {
            key: alt_pubkey,
            addresses: alt_addresses.clone(),
        };

        let v0_msg = v0::Message::try_compile(
            &keypair.pubkey(),
            std::slice::from_ref(&exec_ix),
            &[lookup_account],
            Hash::default(),
        )
        .unwrap();

        let main_tx =
            VersionedTransaction::try_new(VersionedMessage::V0(v0_msg), &[&keypair]).unwrap();

        let mut alt_tx = Transaction::new_with_payer(
            &[alt_ix_create.clone(), alt_ix_extend.clone()],
            Some(&keypair.pubkey()),
        );
        alt_tx.sign(&[&keypair], Hash::default());

        let build_calls = Arc::new(AtomicUsize::new(0));
        let build_calls_clone = Arc::clone(&build_calls);
        let main_tx_clone = main_tx.clone();
        let alt_tx_clone = alt_tx.clone();

        transaction_builder
            .expect_build()
            .times(2)
            .returning(move |_, _| {
                let idx = build_calls_clone.fetch_add(1, Ordering::SeqCst);
                if idx == 0 {
                    Ok((
                        SolanaTransactionType::Versioned(main_tx_clone.clone()),
                        100_000u64,
                    ))
                } else {
                    Ok((
                        SolanaTransactionType::Legacy(alt_tx_clone.clone()),
                        100_000u64,
                    ))
                }
            });

        let sim_calls = Arc::new(AtomicUsize::new(0));
        let sim_calls_clone = Arc::clone(&sim_calls);
        mock_client
            .expect_get_units_consumed_from_simulation()
            .times(2)
            .returning(move |_| {
                let idx = sim_calls_clone.fetch_add(1, Ordering::SeqCst);
                let units = if idx == 0 { 100_000 } else { 120_000 };
                Box::pin(async move { Ok(units) })
            });

        // With insufficient gas, we must NOT send any txs or write Redis
        mock_client.expect_send_transaction().times(0);
        redis_conn.expect_write_gas_cost().times(0);
        redis_conn.expect_write_alt_pubkey().times(0);

        let msg_id_for_event = message_id.clone();
        mock_gmp_api
            .expect_cannot_execute_message()
            .times(1)
            .withf(move |id, msg_id, _src_chain, details, reason| {
                *id == "test-execute-task-its-456"
                    && *msg_id == msg_id_for_event
                    && details.contains("Not enough gas")
                    && matches!(reason, CannotExecuteMessageReason::InsufficientGas)
            })
            .returning(|_, _, _, _, _| Event::CannotExecuteMessageV2 {
                common: CommonEventFields {
                    r#type: "CANNOT_EXECUTE_MESSAGE/V2".to_string(),
                    event_id: "test-event".to_string(),
                    meta: None,
                },
                message_id: "test".to_string(),
                source_chain: "test".to_string(),
                reason: CannotExecuteMessageReason::InsufficientGas,
                details: "test".to_string(),
            });

        let includer = SolanaIncluder::new(
            Arc::new(mock_client),
            Arc::new(keypair),
            chain_name,
            transaction_builder,
            Arc::new(mock_gmp_api),
            redis_conn,
            Arc::new(mock_refunds_model),
        );

        let execute_task = ExecuteTask {
            common: CommonTaskFields {
                id: "test-execute-task-its-456".to_string(),
                chain: "test-chain".to_string(),
                timestamp: Utc::now().to_string(),
                r#type: "execute".to_string(),
                meta: None,
            },
            task: ExecuteTaskFields {
                message: GatewayV2Message {
                    message_id: message_id.clone(),
                    source_chain: "ethereum".to_string(),
                    destination_address: solana_axelar_its::ID.to_string(),
                    payload_hash: BASE64_STANDARD.encode([5u8; 32]),
                    source_address: Pubkey::new_unique().to_string(),
                },
                payload: "test-payload".to_string(),
                available_gas_balance: Amount {
                    amount: available_gas.to_string(),
                    token_id: None,
                },
            },
        };

        let result: Result<Vec<Event>, IncluderError> =
            includer.handle_execute_task(execute_task).await;

        assert!(result.is_ok());
        let events = result.unwrap();
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], Event::CannotExecuteMessageV2 { .. }));
    }

    #[tokio::test]
    async fn handle_execute_its_task_tx_error_records_alt_cost() {
        let (
            mut mock_gmp_api,
            keypair,
            chain_name,
            mut redis_conn,
            mock_refunds_model,
            mut mock_client,
            mut transaction_builder,
        ) = get_includer_fields();

        let message_id = "test-execute-its-789".to_string();
        let available_gas = 20_000u64;

        mock_client
            .expect_incoming_message_already_executed()
            .times(1)
            .returning(|_| Box::pin(async { Ok(false) }));

        redis_conn
            .expect_get_alt_pubkey()
            .times(1)
            .returning(|_| Ok(None));

        let alt_pubkey = Pubkey::new_unique();
        let alt_addresses = vec![Pubkey::new_unique()];
        let alt_addresses_for_builder = alt_addresses.clone();

        let exec_ix = Instruction::new_with_bytes(
            solana_axelar_its::ID,
            &[31, 32, 33, 34],
            vec![
                AccountMeta::new(keypair.pubkey(), true),
                AccountMeta::new_readonly(alt_addresses[0], false),
            ],
        );

        let alt_ix_create =
            Instruction::new_with_bytes(solana_program::system_program::ID, &[5], vec![]);
        let alt_ix_extend =
            Instruction::new_with_bytes(solana_program::system_program::ID, &[6], vec![]);

        let alt_info = ALTInfo::new(
            Some(alt_ix_create.clone()),
            Some(alt_ix_extend.clone()),
            Some(alt_pubkey),
        )
        .with_addresses(alt_addresses_for_builder.clone());

        let exec_ix_for_builder = exec_ix.clone();
        let alt_info_for_builder = alt_info.clone();

        transaction_builder
            .expect_build_execute_instruction()
            .times(1)
            .returning(move |_, _, _, _| {
                Ok((
                    exec_ix_for_builder.clone(),
                    Some(alt_info_for_builder.clone()),
                ))
            });

        let lookup_account = AddressLookupTableAccount {
            key: alt_pubkey,
            addresses: alt_addresses.clone(),
        };

        let v0_msg = v0::Message::try_compile(
            &keypair.pubkey(),
            std::slice::from_ref(&exec_ix),
            &[lookup_account],
            Hash::default(),
        )
        .unwrap();

        let main_tx =
            VersionedTransaction::try_new(VersionedMessage::V0(v0_msg), &[&keypair]).unwrap();

        let mut alt_tx = Transaction::new_with_payer(
            &[alt_ix_create.clone(), alt_ix_extend.clone()],
            Some(&keypair.pubkey()),
        );
        alt_tx.sign(&[&keypair], Hash::default());
        let alt_signature = alt_tx.signatures[0];

        let build_calls = Arc::new(AtomicUsize::new(0));
        let build_calls_clone = Arc::clone(&build_calls);
        let main_tx_clone = main_tx.clone();
        let alt_tx_clone = alt_tx.clone();

        transaction_builder
            .expect_build()
            .times(2)
            .returning(move |_, _| {
                let idx = build_calls_clone.fetch_add(1, Ordering::SeqCst);
                if idx == 0 {
                    Ok((
                        SolanaTransactionType::Versioned(main_tx_clone.clone()),
                        100_000u64,
                    ))
                } else {
                    Ok((
                        SolanaTransactionType::Legacy(alt_tx_clone.clone()),
                        100_000u64,
                    ))
                }
            });

        let sim_calls = Arc::new(AtomicUsize::new(0));
        let sim_calls_clone = Arc::clone(&sim_calls);
        mock_client
            .expect_get_units_consumed_from_simulation()
            .times(2)
            .returning(move |_| {
                let idx = sim_calls_clone.fetch_add(1, Ordering::SeqCst);
                let units = if idx == 0 { 110_000 } else { 90_000 };
                Box::pin(async move { Ok(units) })
            });

        // Send: first ALT succeeds (6000), second (main) fails with TransactionError
        let send_calls = Arc::new(AtomicUsize::new(0));
        let send_calls_clone = Arc::clone(&send_calls);
        let alt_signature_clone = alt_signature;

        mock_client
            .expect_send_transaction()
            .times(2)
            .returning(move |_| {
                let idx = send_calls_clone.fetch_add(1, Ordering::SeqCst);
                if idx == 0 {
                    Box::pin(async move { Ok((alt_signature_clone, Some(6_000u64))) })
                } else {
                    Box::pin(async move {
                        Err(IncluderClientError::TransactionError(
                            "Execution failed".to_string(),
                        ))
                    })
                }
            });

        let msg_id_for_alt = message_id.clone();
        let alt_pubkey_for_expect = alt_pubkey;
        redis_conn
            .expect_write_alt_pubkey()
            .times(1)
            .withf(move |id, pubkey| id == &msg_id_for_alt && *pubkey == alt_pubkey_for_expect)
            .returning(|_, _| Ok(()));

        redis_conn.expect_write_gas_cost().times(0);

        // Expect MessageExecuted(REVERTED) with cost = alt_cost + main_cost_simulated = 11000
        let msg_id_for_event = message_id.clone();
        mock_gmp_api
            .expect_execute_message()
            .times(1)
            .withf(move |msg_id, _src_chain, status, cost| {
                *msg_id == msg_id_for_event
                    && matches!(status, MessageExecutionStatus::REVERTED)
                    && cost.amount == "11000"
            })
            .returning(|_, _, _, _| Event::MessageExecuted {
                common: CommonEventFields {
                    r#type: "MESSAGE_EXECUTED/V2".to_string(),
                    event_id: "test-event".to_string(),
                    meta: None,
                },
                message_id: "test".to_string(),
                source_chain: "test".to_string(),
                status: MessageExecutionStatus::REVERTED,
                cost: Amount {
                    amount: "0".to_string(),
                    token_id: None,
                },
            });

        let includer = SolanaIncluder::new(
            Arc::new(mock_client),
            Arc::new(keypair),
            chain_name,
            transaction_builder,
            Arc::new(mock_gmp_api),
            redis_conn,
            Arc::new(mock_refunds_model),
        );

        let execute_task = ExecuteTask {
            common: CommonTaskFields {
                id: "test-execute-task-its-789".to_string(),
                chain: "test-chain".to_string(),
                timestamp: Utc::now().to_string(),
                r#type: "execute".to_string(),
                meta: None,
            },
            task: ExecuteTaskFields {
                message: GatewayV2Message {
                    message_id: message_id.clone(),
                    source_chain: "ethereum".to_string(),
                    destination_address: solana_axelar_its::ID.to_string(),
                    payload_hash: BASE64_STANDARD.encode([6u8; 32]),
                    source_address: Pubkey::new_unique().to_string(),
                },
                payload: "test-payload".to_string(),
                available_gas_balance: Amount {
                    amount: available_gas.to_string(),
                    token_id: None,
                },
            },
        };

        let result: Result<Vec<Event>, IncluderError> =
            includer.handle_execute_task(execute_task).await;

        assert!(result.is_ok());
        let events = result.unwrap();
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], Event::MessageExecuted { .. }));
    }

    #[tokio::test]
    async fn handle_execute_its_task_with_existing_alt_in_redis() {
        let (
            mock_gmp_api,
            keypair,
            chain_name,
            mut redis_conn,
            mock_refunds_model,
            mut mock_client,
            mut transaction_builder,
        ) = get_includer_fields();

        let message_id = "test-execute-its-existing-alt-123".to_string();
        let available_gas = 5_000u64; // would fail if we were to also create the ALT, but is enough for just the main tx

        mock_client
            .expect_incoming_message_already_executed()
            .times(1)
            .returning(|_| Box::pin(async { Ok(false) }));

        // ALT already exists in Redis for this message_id
        let alt_pubkey = Pubkey::new_unique();
        let alt_pubkey_for_redis = alt_pubkey;
        redis_conn
            .expect_get_alt_pubkey()
            .times(1)
            .returning(move |_| Ok(Some(alt_pubkey_for_redis)));

        let alt_addresses = vec![Pubkey::new_unique()];
        let alt_addresses_for_builder = alt_addresses.clone();

        let exec_ix = Instruction::new_with_bytes(
            solana_axelar_its::ID,
            &[42],
            vec![AccountMeta::new(keypair.pubkey(), true)],
        );

        let alt_info = ALTInfo::new(
            None,             // no alt_ix_create
            None,             // no alt_ix_extend
            Some(alt_pubkey), // existing ALT pubkey
        )
        .with_addresses(alt_addresses_for_builder.clone());

        let exec_ix_for_builder = exec_ix.clone();
        let alt_info_for_builder = alt_info.clone();
        transaction_builder
            .expect_build_execute_instruction()
            .times(1)
            .returning(move |_, _, _, _| {
                Ok((
                    exec_ix_for_builder.clone(),
                    Some(alt_info_for_builder.clone()),
                ))
            });

        let lookup_account = AddressLookupTableAccount {
            key: alt_pubkey,
            addresses: alt_addresses.clone(),
        };

        let v0_msg = v0::Message::try_compile(
            &keypair.pubkey(),
            std::slice::from_ref(&exec_ix),
            &[lookup_account],
            Hash::default(),
        )
        .unwrap();

        let main_tx =
            VersionedTransaction::try_new(VersionedMessage::V0(v0_msg), &[&keypair]).unwrap();

        let main_tx_clone = main_tx.clone();
        transaction_builder
            .expect_build()
            .times(1)
            .returning(move |_, _| {
                Ok((
                    crate::transaction_type::SolanaTransactionType::Versioned(
                        main_tx_clone.clone(),
                    ),
                    100_000u64,
                ))
            });

        let compute_units = 100_000u64;
        mock_client
            .expect_get_units_consumed_from_simulation()
            .times(1)
            .returning(move |_| Box::pin(async move { Ok(compute_units) }));

        // Only the main tx is sent; no ALT tx
        let send_signature = Signature::default();
        mock_client
            .expect_send_transaction()
            .times(1)
            .returning(move |_| Box::pin(async move { Ok((send_signature, Some(5_000u64))) }));

        redis_conn.expect_write_alt_pubkey().times(0);

        let msg_id_for_cost = message_id.clone();
        redis_conn
            .expect_write_gas_cost()
            .times(1)
            .withf(move |id, cost, tx_type| {
                id == &msg_id_for_cost
                    && *cost == 5_000u64
                    && matches!(tx_type, TransactionType::Execute)
            })
            .returning(|_, _, _| ());

        let includer = SolanaIncluder::new(
            Arc::new(mock_client),
            Arc::new(keypair),
            chain_name,
            transaction_builder,
            Arc::new(mock_gmp_api),
            redis_conn,
            Arc::new(mock_refunds_model),
        );

        let execute_task = ExecuteTask {
            common: CommonTaskFields {
                id: "test-execute-its-existing-alt-123".to_string(),
                chain: "test-chain".to_string(),
                timestamp: Utc::now().to_string(),
                r#type: "execute".to_string(),
                meta: None,
            },
            task: ExecuteTaskFields {
                message: GatewayV2Message {
                    message_id: message_id.clone(),
                    source_chain: "ethereum".to_string(),
                    destination_address: solana_axelar_its::ID.to_string(),
                    payload_hash: BASE64_STANDARD.encode([7u8; 32]),
                    source_address: "test-source-address".to_string(),
                },
                payload: "test-payload".to_string(),
                available_gas_balance: Amount {
                    amount: available_gas.to_string(),
                    token_id: None,
                },
            },
        };

        let result: Result<Vec<Event>, IncluderError> =
            includer.handle_execute_task(execute_task).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), vec![]);
    }

    #[tokio::test]
    async fn send_gateway_tx_to_chain_happy_path() {
        let (
            mock_gmp_api,
            keypair,
            chain_name,
            redis_conn,
            mock_refunds_model,
            mut mock_client,
            mut transaction_builder,
        ) = get_includer_fields();

        let instruction = Instruction::new_with_bytes(
            solana_axelar_gateway::ID,
            &[0xAA],
            vec![AccountMeta::new(keypair.pubkey(), true)],
        );

        let mut legacy_tx = Transaction::new_with_payer(
            std::slice::from_ref(&instruction),
            Some(&keypair.pubkey()),
        );
        legacy_tx.sign(&[&keypair], Hash::default());
        let expected_signature = legacy_tx.signatures[0];

        let tx_for_builder = legacy_tx.clone();
        let instruction_program = instruction.program_id;
        let instruction_accounts = instruction.accounts.clone();
        let instruction_data = instruction.data.clone();

        transaction_builder
            .expect_build()
            .times(1)
            .returning(move |ixs, alt| {
                assert!(alt.is_none());
                assert_eq!(ixs.len(), 1);
                assert_eq!(ixs[0].program_id, instruction_program);
                assert_eq!(ixs[0].accounts, instruction_accounts);
                assert_eq!(ixs[0].data, instruction_data);
                Ok((
                    crate::transaction_type::SolanaTransactionType::Legacy(tx_for_builder.clone()),
                    100_000u64,
                ))
            });

        mock_client
            .expect_send_transaction()
            .times(1)
            .returning(move |_| Box::pin(async move { Ok((expected_signature, Some(5_000u64))) }));

        let includer = SolanaIncluder::new(
            Arc::new(mock_client),
            Arc::new(keypair),
            chain_name,
            transaction_builder,
            Arc::new(mock_gmp_api),
            redis_conn,
            Arc::new(mock_refunds_model),
        );

        let message_id = Some("test-gateway-message-123".to_string());
        let source_chain = Some("ethereum".to_string());

        let send_result = includer
            .send_gateway_tx_to_chain(instruction, message_id.clone(), source_chain.clone())
            .await;

        assert!(send_result.status.is_ok());
        assert_eq!(send_result.tx_hash, Some(expected_signature.to_string()));
        assert_eq!(send_result.gas_cost, Some(5_000u64));
        assert_eq!(send_result.message_id, message_id);
        assert_eq!(send_result.source_chain, source_chain);
    }

    #[tokio::test]
    async fn test_handle_gateway_tx_task_rotate_signers_with_one_signature_success() {
        let (
            mock_gmp_api,
            keypair,
            chain_name,
            redis_conn,
            mock_refunds_model,
            mut mock_client,
            mut transaction_builder,
        ) = get_includer_fields();

        let payload_merkle_root = [1u8; 32];
        let signing_verifier_set_merkle_root = [2u8; 32];
        let new_verifier_set_merkle_root = [3u8; 32];

        let verifier_info = SigningVerifierSetInfo {
            leaf: VerifierSetLeaf {
                nonce: 0,
                quorum: 0,
                signer_pubkey: PublicKey::Secp256k1([0; 33]),
                signer_weight: 0,
                position: 0,
                set_size: 0,
                domain_separator: [0; 32],
            },
            merkle_proof: vec![0xDD, 0xEE, 0xFF],
            signature: [0; 65],
        };

        let execute_data = ExecuteData {
            payload_merkle_root,
            signing_verifier_set_merkle_root,
            signing_verifier_set_leaves: vec![verifier_info],
            payload_items: MerkleisedPayload::VerifierSetRotation {
                new_verifier_set_merkle_root,
            },
        };

        let execute_data_b64 =
            base64::prelude::BASE64_STANDARD.encode(execute_data.try_to_vec().unwrap());

        let task = GatewayTxTask {
            common: CommonTaskFields {
                id: "rotate-signer-happy".into(),
                chain: "test-chain".into(),
                timestamp: Utc::now().to_string(),
                r#type: "gateway_tx".into(),
                meta: None,
            },
            task: GatewayTxTaskFields {
                execute_data: execute_data_b64,
            },
        };

        let mut init_tx = Transaction::new_with_payer(&[], Some(&keypair.pubkey()));
        init_tx.sign(&[&keypair], Hash::default());
        let init_sig = init_tx.signatures[0];

        let mut verify_tx = Transaction::new_with_payer(&[], Some(&keypair.pubkey()));
        verify_tx.sign(&[&keypair], Hash::default());
        let verify_sig = verify_tx.signatures[0];

        let mut rotate_tx = Transaction::new_with_payer(&[], Some(&keypair.pubkey()));
        rotate_tx.sign(&[&keypair], Hash::default());
        let rotate_sig = rotate_tx.signatures[0];

        let init_tx_clone = init_tx.clone();
        let verify_tx_clone = verify_tx.clone();
        let rotate_tx_clone = rotate_tx.clone();

        let build_calls = Arc::new(AtomicUsize::new(0));
        let build_calls_clone = Arc::clone(&build_calls);

        transaction_builder
            .expect_build()
            .times(3)
            .returning(move |_, _| {
                let idx = build_calls_clone.fetch_add(1, Ordering::SeqCst);
                match idx {
                    0 => Ok((
                        crate::transaction_type::SolanaTransactionType::Legacy(
                            init_tx_clone.clone(),
                        ),
                        100_000u64,
                    )),
                    1 => Ok((
                        crate::transaction_type::SolanaTransactionType::Legacy(
                            verify_tx_clone.clone(),
                        ),
                        100_000u64,
                    )),
                    _ => Ok((
                        crate::transaction_type::SolanaTransactionType::Legacy(
                            rotate_tx_clone.clone(),
                        ),
                        100_000u64,
                    )),
                }
            });

        let send_calls = Arc::new(AtomicUsize::new(0));
        let send_calls_clone = Arc::clone(&send_calls);
        let send_responses = [(init_sig, 10u64), (verify_sig, 20u64), (rotate_sig, 30u64)];

        mock_client
            .expect_send_transaction()
            .times(3)
            .returning(move |_| {
                let idx = send_calls_clone.fetch_add(1, Ordering::SeqCst);
                let (signature, cost) = send_responses[idx];
                Box::pin(async move { Ok((signature, Some(cost))) })
            });

        let includer = SolanaIncluder::new(
            Arc::new(mock_client),
            Arc::new(keypair),
            chain_name,
            transaction_builder,
            Arc::new(mock_gmp_api),
            redis_conn,
            Arc::new(mock_refunds_model),
        );

        let result = includer.handle_gateway_tx_task(task).await;

        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_handle_gateway_tx_task_approve_message_one_message_one_signature_success() {
        let (
            mock_gmp_api,
            keypair,
            chain_name,
            mut redis_conn,
            mock_refunds_model,
            mut mock_client,
            mut transaction_builder,
        ) = get_includer_fields();

        let payload_merkle_root = [1u8; 32];
        let signing_verifier_set_merkle_root = [2u8; 32];

        let verifier_info = SigningVerifierSetInfo {
            leaf: VerifierSetLeaf {
                nonce: 0,
                quorum: 0,
                signer_pubkey: PublicKey::Secp256k1([0; 33]),
                signer_weight: 0,
                position: 0,
                set_size: 0,
                domain_separator: [0; 32],
            },
            merkle_proof: vec![0xDD, 0xEE, 0xFF],
            signature: [0; 65],
        };

        let execute_data = ExecuteData {
            payload_merkle_root,
            signing_verifier_set_merkle_root,
            signing_verifier_set_leaves: vec![verifier_info],
            payload_items: MerkleisedPayload::NewMessages {
                messages: vec![MerklizedMessage {
                    leaf: MessageLeaf {
                        message: Message {
                            cc_id: CrossChainId {
                                chain: "test-chain".to_string(),
                                id: "test-message-id".to_string(),
                            },
                            source_address: "test-source-address".to_string(),
                            destination_chain: "test-destination-chain".to_string(),
                            destination_address: "test-destination-address".to_string(),
                            payload_hash: [0; 32],
                        },
                        position: 0,
                        set_size: 0,
                        domain_separator: [0; 32],
                    },
                    proof: vec![0xDD, 0xEE, 0xFF],
                }],
            },
        };

        let execute_data_b64 =
            base64::prelude::BASE64_STANDARD.encode(execute_data.try_to_vec().unwrap());

        let task = GatewayTxTask {
            common: CommonTaskFields {
                id: "approve-message-happy".into(),
                chain: "test-chain".into(),
                timestamp: Utc::now().to_string(),
                r#type: "gateway_tx".into(),
                meta: None,
            },
            task: GatewayTxTaskFields {
                execute_data: execute_data_b64,
            },
        };

        let mut init_tx = Transaction::new_with_payer(&[], Some(&keypair.pubkey()));
        init_tx.sign(&[&keypair], Hash::default());
        let init_sig = init_tx.signatures[0];

        let mut verify_tx = Transaction::new_with_payer(&[], Some(&keypair.pubkey()));
        verify_tx.sign(&[&keypair], Hash::default());
        let verify_sig = verify_tx.signatures[0];

        let mut approve_tx = Transaction::new_with_payer(&[], Some(&keypair.pubkey()));
        approve_tx.sign(&[&keypair], Hash::default());
        let approve_sig = approve_tx.signatures[0];

        let init_tx_clone = init_tx.clone();
        let verify_tx_clone = verify_tx.clone();
        let approve_tx_clone = approve_tx.clone();

        let build_calls = Arc::new(AtomicUsize::new(0));
        let build_calls_clone = Arc::clone(&build_calls);

        transaction_builder
            .expect_build()
            .times(3)
            .returning(move |_, _| {
                let idx = build_calls_clone.fetch_add(1, Ordering::SeqCst);
                match idx {
                    0 => Ok((
                        crate::transaction_type::SolanaTransactionType::Legacy(
                            init_tx_clone.clone(),
                        ),
                        100_000u64,
                    )),
                    1 => Ok((
                        crate::transaction_type::SolanaTransactionType::Legacy(
                            verify_tx_clone.clone(),
                        ),
                        100_000u64,
                    )),
                    _ => Ok((
                        crate::transaction_type::SolanaTransactionType::Legacy(
                            approve_tx_clone.clone(),
                        ),
                        100_000u64,
                    )),
                }
            });

        // costs: init(10) + verify(20) + approve(30)
        let send_calls = Arc::new(AtomicUsize::new(0));
        let send_calls_clone = Arc::clone(&send_calls);
        let send_responses = [(init_sig, 10u64), (verify_sig, 20u64), (approve_sig, 30u64)];

        mock_client
            .expect_send_transaction()
            .times(3)
            .returning(move |_| {
                let idx = send_calls_clone.fetch_add(1, Ordering::SeqCst);
                let (signature, cost) = send_responses[idx];
                Box::pin(async move { Ok((signature, Some(cost))) })
            });

        redis_conn
            .expect_write_gas_cost()
            .times(1)
            .withf(move |_, cost, tx_type| {
                *cost == 60 && matches!(tx_type, TransactionType::Approve)
            })
            .returning(|_, _, _| ());

        let includer = SolanaIncluder::new(
            Arc::new(mock_client),
            Arc::new(keypair),
            chain_name,
            transaction_builder,
            Arc::new(mock_gmp_api),
            redis_conn,
            Arc::new(mock_refunds_model),
        );

        let result = includer.handle_gateway_tx_task(task).await;

        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_handle_gateway_tx_task_approve_message_two_messages_two_signatures_success() {
        let (
            mock_gmp_api,
            keypair,
            chain_name,
            mut redis_conn,
            mock_refunds_model,
            mut mock_client,
            mut transaction_builder,
        ) = get_includer_fields();

        let payload_merkle_root = [1u8; 32];
        let signing_verifier_set_merkle_root = [2u8; 32];

        let verifier_info_1 = SigningVerifierSetInfo {
            leaf: VerifierSetLeaf {
                nonce: 0,
                quorum: 0,
                signer_pubkey: PublicKey::Secp256k1([1; 33]),
                signer_weight: 0,
                position: 0,
                set_size: 0,
                domain_separator: [0; 32],
            },
            merkle_proof: vec![0xAA],
            signature: [1; 65],
        };

        let verifier_info_2 = SigningVerifierSetInfo {
            leaf: VerifierSetLeaf {
                nonce: 1,
                quorum: 0,
                signer_pubkey: PublicKey::Secp256k1([2; 33]),
                signer_weight: 0,
                position: 1,
                set_size: 0,
                domain_separator: [0; 32],
            },
            merkle_proof: vec![0xBB],
            signature: [2; 65],
        };

        let msg_id_1 = "test-message-id-1".to_string();
        let msg_id_2 = "test-message-id-2".to_string();

        let merkle_msg_1 = MerklizedMessage {
            leaf: MessageLeaf {
                message: Message {
                    cc_id: CrossChainId {
                        chain: "test-chain".to_string(),
                        id: msg_id_1.clone(),
                    },
                    source_address: "test-source-address-1".to_string(),
                    destination_chain: "test-destination-chain-1".to_string(),
                    destination_address: "test-destination-address-1".to_string(),
                    payload_hash: [11; 32],
                },
                position: 0,
                set_size: 2,
                domain_separator: [0; 32],
            },
            proof: vec![0x01],
        };

        let merkle_msg_2 = MerklizedMessage {
            leaf: MessageLeaf {
                message: Message {
                    cc_id: CrossChainId {
                        chain: "test-chain".to_string(),
                        id: msg_id_2.clone(),
                    },
                    source_address: "test-source-address-2".to_string(),
                    destination_chain: "test-destination-chain-2".to_string(),
                    destination_address: "test-destination-address-2".to_string(),
                    payload_hash: [22; 32],
                },
                position: 1,
                set_size: 2,
                domain_separator: [0; 32],
            },
            proof: vec![0x02],
        };

        let execute_data = ExecuteData {
            payload_merkle_root,
            signing_verifier_set_merkle_root,
            signing_verifier_set_leaves: vec![verifier_info_1, verifier_info_2],
            payload_items: MerkleisedPayload::NewMessages {
                messages: vec![merkle_msg_1, merkle_msg_2],
            },
        };

        let execute_data_b64 =
            base64::prelude::BASE64_STANDARD.encode(execute_data.try_to_vec().unwrap());

        let task = GatewayTxTask {
            common: CommonTaskFields {
                id: "approve-message-two-msgs-two-sigs".into(),
                chain: "test-chain".into(),
                timestamp: Utc::now().to_string(),
                r#type: "gateway_tx".into(),
                meta: None,
            },
            task: GatewayTxTaskFields {
                execute_data: execute_data_b64,
            },
        };

        let mut init_tx = Transaction::new_with_payer(&[], Some(&keypair.pubkey()));
        init_tx.sign(&[&keypair], Hash::default());
        let init_sig = init_tx.signatures[0];

        let mut verify_tx_1 = Transaction::new_with_payer(&[], Some(&keypair.pubkey()));
        verify_tx_1.sign(&[&keypair], Hash::default());
        let verify_sig_1 = verify_tx_1.signatures[0];

        let mut verify_tx_2 = Transaction::new_with_payer(&[], Some(&keypair.pubkey()));
        verify_tx_2.sign(&[&keypair], Hash::default());
        let verify_sig_2 = verify_tx_2.signatures[0];

        let mut approve_tx_1 = Transaction::new_with_payer(&[], Some(&keypair.pubkey()));
        approve_tx_1.sign(&[&keypair], Hash::default());
        let approve_sig_1 = approve_tx_1.signatures[0];

        let mut approve_tx_2 = Transaction::new_with_payer(&[], Some(&keypair.pubkey()));
        approve_tx_2.sign(&[&keypair], Hash::default());
        let approve_sig_2 = approve_tx_2.signatures[0];

        let init_tx_clone = init_tx.clone();
        let verify_tx_1_clone = verify_tx_1.clone();
        let verify_tx_2_clone = verify_tx_2.clone();
        let approve_tx_1_clone = approve_tx_1.clone();
        let approve_tx_2_clone = approve_tx_2.clone();

        let build_calls = Arc::new(AtomicUsize::new(0));
        let build_calls_clone = Arc::clone(&build_calls);
        transaction_builder
            .expect_build()
            .times(5)
            .returning(move |_, _| {
                let idx = build_calls_clone.fetch_add(1, Ordering::SeqCst);
                match idx {
                    0 => Ok((
                        crate::transaction_type::SolanaTransactionType::Legacy(
                            init_tx_clone.clone(),
                        ),
                        100_000u64,
                    )),
                    1 => Ok((
                        crate::transaction_type::SolanaTransactionType::Legacy(
                            verify_tx_1_clone.clone(),
                        ),
                        100_000u64,
                    )),
                    2 => Ok((
                        crate::transaction_type::SolanaTransactionType::Legacy(
                            verify_tx_2_clone.clone(),
                        ),
                        100_000u64,
                    )),
                    3 => Ok((
                        crate::transaction_type::SolanaTransactionType::Legacy(
                            approve_tx_1_clone.clone(),
                        ),
                        100_000u64,
                    )),
                    4 => Ok((
                        crate::transaction_type::SolanaTransactionType::Legacy(
                            approve_tx_2_clone.clone(),
                        ),
                        100_000u64,
                    )),
                    _ => panic!("unexpected build call"),
                }
            });

        // total_overhead = 10 + 20 + 20 = 50
        // per-message overhead = 50 / 2 = 25
        // msg1_cost = 30 + 25 = 55
        // msg2_cost = 40 + 25 = 65
        let send_calls = Arc::new(AtomicUsize::new(0));
        let send_calls_clone = Arc::clone(&send_calls);
        let send_responses = [
            (init_sig, 10u64),
            (verify_sig_1, 20u64),
            (verify_sig_2, 20u64),
            (approve_sig_1, 30u64),
            (approve_sig_2, 40u64),
        ];

        mock_client
            .expect_send_transaction()
            .times(5)
            .returning(move |_| {
                let idx = send_calls_clone.fetch_add(1, Ordering::SeqCst);
                let (sig, cost) = send_responses[idx];
                Box::pin(async move { Ok((sig, Some(cost))) })
            });

        let expected_id_1 = msg_id_1.clone();
        let expected_id_2 = msg_id_2.clone();
        redis_conn
            .expect_write_gas_cost()
            .times(2)
            .withf(move |msg_id, cost, tx_type| {
                if msg_id == &expected_id_1 {
                    *cost == 55 && matches!(tx_type, TransactionType::Approve)
                } else if msg_id == &expected_id_2 {
                    *cost == 65 && matches!(tx_type, TransactionType::Approve)
                } else {
                    false
                }
            })
            .returning(|_, _, _| ());

        let includer = SolanaIncluder::new(
            Arc::new(mock_client),
            Arc::new(keypair),
            chain_name,
            transaction_builder,
            Arc::new(mock_gmp_api),
            redis_conn,
            Arc::new(mock_refunds_model),
        );

        let result = includer.handle_gateway_tx_task(task).await;

        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_handle_gateway_tx_task_approve_message_two_messages_one_fails_one_succeeds() {
        let (
            mut mock_gmp_api,
            keypair,
            chain_name,
            mut redis_conn,
            mock_refunds_model,
            mut mock_client,
            mut transaction_builder,
        ) = get_includer_fields();

        let payload_merkle_root = [1u8; 32];
        let signing_verifier_set_merkle_root = [2u8; 32];

        let verifier_info_1 = SigningVerifierSetInfo {
            leaf: VerifierSetLeaf {
                nonce: 0,
                quorum: 0,
                signer_pubkey: PublicKey::Secp256k1([1; 33]),
                signer_weight: 0,
                position: 0,
                set_size: 0,
                domain_separator: [0; 32],
            },
            merkle_proof: vec![0xAA],
            signature: [1; 65],
        };

        let verifier_info_2 = SigningVerifierSetInfo {
            leaf: VerifierSetLeaf {
                nonce: 1,
                quorum: 0,
                signer_pubkey: PublicKey::Secp256k1([2; 33]),
                signer_weight: 0,
                position: 1,
                set_size: 0,
                domain_separator: [0; 32],
            },
            merkle_proof: vec![0xBB],
            signature: [2; 65],
        };

        let msg_id_1 = "test-message-id-1".to_string();
        let msg_id_2 = "test-message-id-2".to_string();

        let merkle_msg_1 = MerklizedMessage {
            leaf: MessageLeaf {
                message: Message {
                    cc_id: CrossChainId {
                        chain: "test-chain".to_string(),
                        id: msg_id_1.clone(),
                    },
                    source_address: "test-source-address-1".to_string(),
                    destination_chain: "test-destination-chain-1".to_string(),
                    destination_address: "test-destination-address-1".to_string(),
                    payload_hash: [11; 32],
                },
                position: 0,
                set_size: 2,
                domain_separator: [0; 32],
            },
            proof: vec![0x01],
        };

        let merkle_msg_2 = MerklizedMessage {
            leaf: MessageLeaf {
                message: Message {
                    cc_id: CrossChainId {
                        chain: "test-chain".to_string(),
                        id: msg_id_2.clone(),
                    },
                    source_address: "test-source-address-2".to_string(),
                    destination_chain: "test-destination-chain-2".to_string(),
                    destination_address: "test-destination-address-2".to_string(),
                    payload_hash: [22; 32],
                },
                position: 1,
                set_size: 2,
                domain_separator: [0; 32],
            },
            proof: vec![0x02],
        };

        let execute_data = ExecuteData {
            payload_merkle_root,
            signing_verifier_set_merkle_root,
            signing_verifier_set_leaves: vec![verifier_info_1, verifier_info_2],
            payload_items: MerkleisedPayload::NewMessages {
                messages: vec![merkle_msg_1, merkle_msg_2],
            },
        };

        let execute_data_b64 =
            base64::prelude::BASE64_STANDARD.encode(execute_data.try_to_vec().unwrap());

        let task = GatewayTxTask {
            common: CommonTaskFields {
                id: "approve-message-two-msgs-one-fails".into(),
                chain: "test-chain".into(),
                timestamp: Utc::now().to_string(),
                r#type: "gateway_tx".into(),
                meta: None,
            },
            task: GatewayTxTaskFields {
                execute_data: execute_data_b64,
            },
        };

        let mut init_tx = Transaction::new_with_payer(&[], Some(&keypair.pubkey()));
        init_tx.sign(&[&keypair], Hash::default());
        let init_sig = init_tx.signatures[0];

        let mut verify_tx_1 = Transaction::new_with_payer(&[], Some(&keypair.pubkey()));
        verify_tx_1.sign(&[&keypair], Hash::default());
        let verify_sig_1 = verify_tx_1.signatures[0];

        let mut verify_tx_2 = Transaction::new_with_payer(&[], Some(&keypair.pubkey()));
        verify_tx_2.sign(&[&keypair], Hash::default());
        let verify_sig_2 = verify_tx_2.signatures[0];

        let mut approve_tx_1 = Transaction::new_with_payer(&[], Some(&keypair.pubkey()));
        approve_tx_1.sign(&[&keypair], Hash::default());
        let approve_sig_1 = approve_tx_1.signatures[0];

        let mut approve_tx_2 = Transaction::new_with_payer(&[], Some(&keypair.pubkey()));
        approve_tx_2.sign(&[&keypair], Hash::default());

        let init_tx_clone = init_tx.clone();
        let verify_tx_1_clone = verify_tx_1.clone();
        let verify_tx_2_clone = verify_tx_2.clone();
        let approve_tx_1_clone = approve_tx_1.clone();
        let _approve_tx_2_clone = approve_tx_2.clone(); // not used in build, since it fails at send

        // 5 builds: init, verify1, verify2, approve1, approve2
        let build_calls = Arc::new(AtomicUsize::new(0));
        let build_calls_clone = Arc::clone(&build_calls);
        transaction_builder
            .expect_build()
            .times(5)
            .returning(move |_, _| {
                let idx = build_calls_clone.fetch_add(1, Ordering::SeqCst);
                match idx {
                    0 => Ok((
                        crate::transaction_type::SolanaTransactionType::Legacy(
                            init_tx_clone.clone(),
                        ),
                        100_000u64,
                    )),
                    1 => Ok((
                        crate::transaction_type::SolanaTransactionType::Legacy(
                            verify_tx_1_clone.clone(),
                        ),
                        100_000u64,
                    )),
                    2 => Ok((
                        crate::transaction_type::SolanaTransactionType::Legacy(
                            verify_tx_2_clone.clone(),
                        ),
                        100_000u64,
                    )),
                    3 => Ok((
                        crate::transaction_type::SolanaTransactionType::Legacy(
                            approve_tx_1_clone.clone(),
                        ),
                        100_000u64,
                    )),
                    4 => Ok((
                        crate::transaction_type::SolanaTransactionType::Legacy(
                            approve_tx_2.clone(),
                        ),
                        100_000u64,
                    )),
                    _ => panic!("unexpected build call"),
                }
            });

        // Costs:
        // init:    10
        // verify1: 20
        // verify2: 30
        // approve1:40 (success)
        // approve2: (fails)
        //
        // overhead = 10 + 20 + 30 = 60
        // per-message overhead = 60 / 2 = 30
        // msg1_cost = 40 + 30 = 70
        let send_calls = Arc::new(AtomicUsize::new(0));
        let send_calls_clone = Arc::clone(&send_calls);
        mock_client
            .expect_send_transaction()
            .times(5)
            .returning(move |_| {
                let idx = send_calls_clone.fetch_add(1, Ordering::SeqCst);
                match idx {
                    0 => Box::pin(async move { Ok((init_sig, Some(10u64))) }),
                    1 => Box::pin(async move { Ok((verify_sig_1, Some(20u64))) }),
                    2 => Box::pin(async move { Ok((verify_sig_2, Some(30u64))) }),
                    3 => Box::pin(async move { Ok((approve_sig_1, Some(40u64))) }),
                    4 => Box::pin(async move {
                        Err(IncluderClientError::GenericError(
                            "approve-2 failed".to_string(),
                        ))
                    }),
                    _ => panic!("unexpected send_transaction call"),
                }
            });

        // write_gas_cost only for the successful message (msg_id_1) with cost 70
        let expected_success_id = msg_id_1.clone();
        redis_conn
            .expect_write_gas_cost()
            .times(1)
            .withf(move |msg_id, cost, tx_type| {
                *msg_id == expected_success_id
                    && *cost == 70
                    && matches!(tx_type, TransactionType::Approve)
            })
            .returning(|_, _, _| ());

        // Expect one cannot_execute_message for the failed message (msg_id_2)
        let expected_fail_id = msg_id_2.clone();
        mock_gmp_api
            .expect_cannot_execute_message()
            .times(1)
            .withf(move |task_id, msg_id, src_chain, details, reason| {
                *task_id == "approve-message-two-msgs-one-fails"
                    && *msg_id == expected_fail_id
                    && *src_chain == "test-chain"
                    && details.contains("approve-2 failed")
                    && matches!(reason, CannotExecuteMessageReason::Error)
            })
            .returning(|_, _, _, _, _| Event::CannotExecuteMessageV2 {
                common: CommonEventFields {
                    r#type: "CANNOT_EXECUTE_MESSAGE/V2".to_string(),
                    event_id: "evt-approve-failed".to_string(),
                    meta: None,
                },
                message_id: "dummy".to_string(),
                source_chain: "test-chain".to_string(),
                reason: CannotExecuteMessageReason::Error,
                details: "dummy".to_string(),
            });

        let includer = SolanaIncluder::new(
            Arc::new(mock_client),
            Arc::new(keypair),
            chain_name,
            transaction_builder,
            Arc::new(mock_gmp_api),
            redis_conn,
            Arc::new(mock_refunds_model),
        );

        let result = includer.handle_gateway_tx_task(task).await;

        assert!(result.is_ok());
        let events = result.unwrap();
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], Event::CannotExecuteMessageV2 { .. }));
    }
    #[tokio::test]
    async fn test_handle_gateway_tx_task_verify_signature_failure_all_messages_cannot_execute() {
        let (
            mut mock_gmp_api,
            keypair,
            chain_name,
            mut redis_conn,
            mock_refunds_model,
            mut mock_client,
            mut transaction_builder,
        ) = get_includer_fields();

        let payload_merkle_root = [9u8; 32];
        let signing_verifier_set_merkle_root = [8u8; 32];

        let verifier_info = SigningVerifierSetInfo {
            leaf: VerifierSetLeaf {
                nonce: 0,
                quorum: 0,
                signer_pubkey: PublicKey::Secp256k1([3; 33]),
                signer_weight: 0,
                position: 0,
                set_size: 1,
                domain_separator: [0; 32],
            },
            merkle_proof: vec![0xCC],
            signature: [3; 65],
        };

        let msg_id_1 = "verify-fail-msg-1".to_string();
        let msg_id_2 = "verify-fail-msg-2".to_string();
        let msg_id_3 = "verify-fail-msg-3".to_string();

        let merkle_msg_1 = MerklizedMessage {
            leaf: MessageLeaf {
                message: Message {
                    cc_id: CrossChainId {
                        chain: "test-chain".to_string(),
                        id: msg_id_1.clone(),
                    },
                    source_address: "src-1".to_string(),
                    destination_chain: "dst-chain".to_string(),
                    destination_address: "dst-addr-1".to_string(),
                    payload_hash: [1; 32],
                },
                position: 0,
                set_size: 3,
                domain_separator: [0; 32],
            },
            proof: vec![0x01],
        };

        let merkle_msg_2 = MerklizedMessage {
            leaf: MessageLeaf {
                message: Message {
                    cc_id: CrossChainId {
                        chain: "test-chain".to_string(),
                        id: msg_id_2.clone(),
                    },
                    source_address: "src-2".to_string(),
                    destination_chain: "dst-chain".to_string(),
                    destination_address: "dst-addr-2".to_string(),
                    payload_hash: [2; 32],
                },
                position: 1,
                set_size: 3,
                domain_separator: [0; 32],
            },
            proof: vec![0x02],
        };

        let merkle_msg_3 = MerklizedMessage {
            leaf: MessageLeaf {
                message: Message {
                    cc_id: CrossChainId {
                        chain: "test-chain".to_string(),
                        id: msg_id_3.clone(),
                    },
                    source_address: "src-3".to_string(),
                    destination_chain: "dst-chain".to_string(),
                    destination_address: "dst-addr-3".to_string(),
                    payload_hash: [3; 32],
                },
                position: 2,
                set_size: 3,
                domain_separator: [0; 32],
            },
            proof: vec![0x03],
        };

        let execute_data = ExecuteData {
            payload_merkle_root,
            signing_verifier_set_merkle_root,
            signing_verifier_set_leaves: vec![verifier_info],
            payload_items: MerkleisedPayload::NewMessages {
                messages: vec![merkle_msg_1, merkle_msg_2, merkle_msg_3],
            },
        };

        let execute_data_b64 =
            base64::prelude::BASE64_STANDARD.encode(execute_data.try_to_vec().unwrap());

        let task_id = "verify-signature-failure-all-cannot-execute".to_string();

        let task = GatewayTxTask {
            common: CommonTaskFields {
                id: task_id.clone(),
                chain: "test-chain".into(),
                timestamp: Utc::now().to_string(),
                r#type: "gateway_tx".into(),
                meta: None,
            },
            task: GatewayTxTaskFields {
                execute_data: execute_data_b64,
            },
        };

        let mut init_tx = Transaction::new_with_payer(&[], Some(&keypair.pubkey()));
        init_tx.sign(&[&keypair], Hash::default());
        let init_sig = init_tx.signatures[0];

        let mut verify_tx = Transaction::new_with_payer(&[], Some(&keypair.pubkey()));
        verify_tx.sign(&[&keypair], Hash::default());

        let init_tx_clone = init_tx.clone();
        let verify_tx_clone = verify_tx.clone();

        // 2 builds: init, verify
        let build_calls = Arc::new(AtomicUsize::new(0));
        let build_calls_clone = Arc::clone(&build_calls);
        transaction_builder
            .expect_build()
            .times(2)
            .returning(move |_, _| {
                let idx = build_calls_clone.fetch_add(1, Ordering::SeqCst);
                match idx {
                    0 => Ok((
                        crate::transaction_type::SolanaTransactionType::Legacy(
                            init_tx_clone.clone(),
                        ),
                        100_000u64,
                    )),
                    1 => Ok((
                        crate::transaction_type::SolanaTransactionType::Legacy(
                            verify_tx_clone.clone(),
                        ),
                        100_000u64,
                    )),
                    _ => panic!("unexpected build call"),
                }
            });

        let send_calls = Arc::new(AtomicUsize::new(0));
        let send_calls_clone = Arc::clone(&send_calls);
        mock_client
            .expect_send_transaction()
            .times(2)
            .returning(move |_| {
                let idx = send_calls_clone.fetch_add(1, Ordering::SeqCst);
                match idx {
                    0 => Box::pin(async move { Ok((init_sig, Some(10u64))) }),
                    1 => Box::pin(async move {
                        Err(IncluderClientError::GenericError(
                            "verify failed".to_string(),
                        ))
                    }),
                    _ => panic!("unexpected send_transaction call"),
                }
            });

        redis_conn.expect_write_gas_cost().times(0);

        let expected_ids = [msg_id_1.clone(), msg_id_2.clone(), msg_id_3.clone()];
        mock_gmp_api
            .expect_cannot_execute_message()
            .times(3)
            .withf(move |got_task_id, msg_id, src_chain, details, reason| {
                got_task_id == &task_id
                    && expected_ids.contains(msg_id)
                    && *src_chain == "test-chain"
                    && details.contains("verify failed")
                    && matches!(reason, CannotExecuteMessageReason::Error)
            })
            .returning(|_, msg_id, src_chain, _, _| Event::CannotExecuteMessageV2 {
                common: CommonEventFields {
                    r#type: "CANNOT_EXECUTE_MESSAGE/V2".to_string(),
                    event_id: format!("evt-{}", msg_id),
                    meta: None,
                },
                message_id: msg_id,
                source_chain: src_chain,
                reason: CannotExecuteMessageReason::Error,
                details: "verify failed".to_string(),
            });

        let includer = SolanaIncluder::new(
            Arc::new(mock_client),
            Arc::new(keypair),
            chain_name,
            transaction_builder,
            Arc::new(mock_gmp_api),
            redis_conn,
            Arc::new(mock_refunds_model),
        );

        let result = includer.handle_gateway_tx_task(task).await;

        assert!(result.is_ok());
        let events = result.unwrap();
        assert_eq!(events.len(), 3);
        for ev in events {
            assert!(matches!(ev, Event::CannotExecuteMessageV2 { .. }));
        }
    }
}
