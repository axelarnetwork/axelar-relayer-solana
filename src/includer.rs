use crate::config::SolanaConfig;
use crate::error::{IncluderClientError, SolanaIncluderError, TransactionBuilderError};
use crate::gas_calculator::GasCalculator;
use crate::gas_calculator::{InstructionKind, MAX_VERIFY_SIGNATURES_PER_TX};
use crate::includer_client::{IncluderClient, IncluderClientTrait};
use crate::models::refunds::RefundsModel;
use crate::redis::RedisConnectionTrait;
use crate::refund_manager::SolanaRefundManager;
use crate::transaction_builder::{TransactionBuilder, TransactionBuilderTrait};
#[cfg(not(feature = "devnet-amplifier"))]
use crate::utils::not_enough_gas_event;
use crate::utils::{get_gas_service_event_authority_pda, get_gateway_event_authority_pda};
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
use solana_axelar_std::execute_data::{ExecuteData, MerklizedPayload, PayloadType};
use solana_axelar_std::MerklizedMessage;
use solana_axelar_std::{CrossChainId, Message};
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

/// Verifies that the relayer keypair is registered as an operator in the gas service.
async fn ensure_relayer_is_operator(
    client: &IncluderClient,
    keypair: &Keypair,
) -> Result<(), IncluderError> {
    #[cfg(feature = "devnet-amplifier")]
    {
        let _ = client;
        let _ = keypair;
        Ok(())
    }

    #[cfg(not(feature = "devnet-amplifier"))]
    {
        let (operator_pda, _) = solana_axelar_operators::OperatorAccount::try_find_pda(
            &keypair.pubkey(),
        )
        .ok_or_else(|| IncluderError::GenericError("Failed to derive operator PDA".to_string()))?;

        client.get_account_data(&operator_pda).await.map_err(|_| {
            error!("Relayer key {} is not registered as a gas service operator (PDA {} not found on-chain)", keypair.pubkey(), operator_pda);
            IncluderError::GenericError(format!(
                "Relayer key {} is not registered as a gas service operator (PDA {} not found on-chain)",
                keypair.pubkey(),
                operator_pda
            ))
        })?;

        info!(
            "Relayer {} verified as gas service operator",
            keypair.pubkey()
        );
        Ok(())
    }
}

#[derive(Clone)]
pub struct SolanaIncluder<
    G: GmpApiTrait + Clone,
    R: RedisConnectionTrait + Clone,
    RF: RefundsModel + Clone,
    IC: IncluderClientTrait + Clone,
    TB: TransactionBuilderTrait<IC, R> + Clone,
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
        G: GmpApiTrait + Clone,
        R: RedisConnectionTrait + Clone,
        RF: RefundsModel + Clone,
        IC: IncluderClientTrait + Clone,
        TB: TransactionBuilderTrait<IC, R> + Clone,
    > SolanaIncluder<G, R, RF, IC, TB>
{
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
                TransactionBuilder<GasCalculator<IncluderClient>, IncluderClient, R>,
            >,
        >,
        IncluderError,
    > {
        let solana_rpc = config.solana_poll_rpc.clone();
        let solana_commitment = config.solana_includer_commitment();

        let client = Arc::new(
            IncluderClient::new(&solana_rpc, solana_commitment, 3)
                .map_err(|e| error_stack::report!(IncluderError::GenericError(e.to_string())))?,
        );

        let keypair = Arc::new(config.signing_keypair());

        let gas_calculator = GasCalculator::new(client.as_ref().clone());

        let transaction_builder = TransactionBuilder::new(
            Arc::clone(&keypair),
            gas_calculator,
            Arc::clone(&client),
            redis_conn.clone(),
        );

        ensure_relayer_is_operator(&client, &keypair)
            .await
            .map_err(|e| error_stack::report!(e))?;

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

    async fn build_and_send_transaction(
        &self,
        ixs: &[Instruction],
        execute_data: Option<&ExecuteData>,
        kind: InstructionKind,
    ) -> Result<(Signature, Option<u64>), SolanaIncluderError> {
        let (tx, _) = match self.transaction_builder.build(ixs, kind).await {
            Ok((tx, cost)) => (tx, cost),
            Err(e) => {
                return Err(SolanaIncluderError::GenericError(e.to_string()));
            }
        };

        let res = self.client.send_transaction(tx, execute_data).await;
        match res {
            Ok((signature, gas_cost)) => {
                debug!(
                    "Transaction sent successfully: {}, Cost: {}",
                    signature.to_string(),
                    gas_cost.unwrap_or(0)
                );
                Ok((signature, gas_cost))
            }
            Err(e) => match e {
                IncluderClientError::SlotAlreadyVerifiedError(e) => {
                    Err(SolanaIncluderError::SlotAlreadyVerifiedError(e))
                }
                IncluderClientError::AccountInUseError(e) => {
                    Err(SolanaIncluderError::AccountInUseError(e))
                }
                _ => Err(SolanaIncluderError::GenericError(e.to_string())),
            },
        }
    }

    async fn build_execute_transaction_and_send(
        &self,
        instruction: Instruction,
        task: ExecuteTask,
        entrypoint: crate::gas_estimation::ExecuteEntrypoint, // for the rent estimate
    ) -> Result<Vec<Event>, IncluderError> {
        let available_gas_balance = i64::from_str(&task.task.available_gas_balance.amount)
            .map_err(|e| IncluderError::GenericError(e.to_string()))?;

        // Rent the fee payer will fund for the accounts this execute creates (token manager,
        // mint, metadata, ATAs, roles). Known up front from the GMP entrypoint.
        let rent_estimate = crate::gas_estimation::execute_rent_lamports(entrypoint);

        let (transaction, estimated_tx_cost) = self
            .transaction_builder
            .build(std::slice::from_ref(&instruction), InstructionKind::Execute)
            .await
            .map_err(|e| IncluderError::GenericError(e.to_string()))?;

        // `estimated_tx_cost` is consensus-only (base + priority). Add the rent this execute funds
        // so the check reflects the true cost the relayer pays.
        let total_estimated_cost = estimated_tx_cost.saturating_add(rent_estimate);

        #[cfg(feature = "devnet-amplifier")]
        let _ = (available_gas_balance, total_estimated_cost);
        #[cfg(not(feature = "devnet-amplifier"))]
        if total_estimated_cost as i64 > available_gas_balance {
            return Ok(not_enough_gas_event(
                available_gas_balance,
                total_estimated_cost,
                task,
                Arc::clone(&self.gmp_api),
            ));
        }

        debug!(
            "Sending execute transaction - task_id: {}, destination_address: {}, accounts: {:?}, payload (base64): {}",
            task.common.id,
            task.task.message.destination_address,
            instruction.accounts,
            task.task.payload,
        );

        match self
            .client
            .send_transaction(transaction.clone(), None)
            .await
        {
            Ok((signature, actual_tx_cost)) => {
                info!("Transaction sent successfully: {}", signature.to_string());

                self.redis_conn
                    .write_gas_cost_for_message_id(
                        task.task.message.message_id.clone(),
                        actual_tx_cost.unwrap_or(0),
                        TransactionType::Execute,
                    )
                    .await;

                Ok(vec![])
            }
            Err(e) => match e {
                IncluderClientError::UnrecoverableTransactionError(e) => {
                    warn!("Transaction reverted: {}", e);
                    let signature = transaction.get_signature().copied().ok_or_else(|| {
                        IncluderError::GenericError("Failed to get signature".to_string())
                    })?;
                    let reverted_tx_cost = self
                        .client
                        .get_transaction_cost_from_signature(&signature)
                        .await
                        .unwrap_or(Some(estimated_tx_cost)); // if we fail to get the cost we still need to report the GMP

                    let total_reverted_cost = reverted_tx_cost.unwrap_or(estimated_tx_cost);
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

    async fn initialize_payload_verification_session(
        &self,
        execute_data: &ExecuteData,
    ) -> Result<Option<u64>, SolanaIncluderError> {
        let payload_type = match &execute_data.payload_items {
            MerklizedPayload::NewMessages { .. } => PayloadType::ApproveMessages,
            MerklizedPayload::VerifierSetRotation { .. } => PayloadType::RotateSigners,
        };

        let (verification_session_tracker_pda, _) =
            solana_axelar_gateway::SignatureVerificationSessionData::try_find_pda(
                &execute_data.payload_merkle_root,
                payload_type,
                &execute_data.signing_verifier_set_merkle_root,
            )
            .ok_or_else(|| {
                SolanaIncluderError::GenericError(
                    "Failed to derive verification session tracker PDA".to_string(),
                )
            })?;

        let ix_data = solana_axelar_gateway::instruction::InitializePayloadVerificationSession {
            merkle_root: execute_data.payload_merkle_root,
            payload_type,
        }
        .data();

        let (verifier_set_tracker_pda, _) =
            solana_axelar_gateway::VerifierSetTracker::try_find_pda(
                &execute_data.signing_verifier_set_merkle_root,
            )
            .ok_or_else(|| {
                SolanaIncluderError::GenericError(
                    "Failed to derive verifier set tracker PDA".to_string(),
                )
            })?;

        let (gateway_root_pda, _) = solana_axelar_gateway::GatewayConfig::try_find_pda()
            .ok_or_else(|| {
                SolanaIncluderError::GenericError(
                    "Failed to derive gateway root config PDA".to_string(),
                )
            })?;

        let accounts = solana_axelar_gateway::accounts::InitializePayloadVerificationSession {
            payer: self.keypair.pubkey(),
            gateway_root_pda,
            verification_session_account: verification_session_tracker_pda,
            verifier_set_tracker_pda,
            system_program: solana_sdk_ids::system_program::ID,
        };

        let ix = Instruction {
            program_id: solana_axelar_gateway::ID,
            accounts: accounts.to_account_metas(None),
            data: ix_data,
        };

        let (tx, _) = self
            .transaction_builder
            .build(&[ix], InstructionKind::InitPayloadVerification)
            .await
            .map_err(|e| SolanaIncluderError::GenericError(e.to_string()))?;

        let res = self.client.send_transaction(tx, Some(execute_data)).await;
        match res {
            Ok((signature, gas_cost)) => {
                debug!(
                    "Transaction for initializing payload verification session successfully: {}. Cost: {:?}",
                    signature,
                    gas_cost
                );
                Ok(gas_cost)
            }
            Err(e) => match e {
                IncluderClientError::AccountInUseError(e) => {
                    Err(SolanaIncluderError::AccountInUseError(e))
                }
                _ => Err(SolanaIncluderError::GenericError(e.to_string())),
            },
        }
    }

    /// verify the signatures of the signing session, up to MAX_VERIFY_SIGNATURES_PER_TX per transaction
    async fn verify_signatures(
        &self,
        execute_data: &ExecuteData,
    ) -> Result<u64, SolanaIncluderError> {
        let mut total_cost = 0;

        let payload_type = match &execute_data.payload_items {
            MerklizedPayload::NewMessages { .. } => PayloadType::ApproveMessages,
            MerklizedPayload::VerifierSetRotation { .. } => PayloadType::RotateSigners,
        };

        // Collect PDAs
        let (verification_session_tracker_pda, _) =
            solana_axelar_gateway::SignatureVerificationSessionData::try_find_pda(
                &execute_data.payload_merkle_root,
                payload_type,
                &execute_data.signing_verifier_set_merkle_root,
            )
            .ok_or_else(|| {
                SolanaIncluderError::GenericError(
                    "Failed to derive verification session tracker PDA".to_string(),
                )
            })?;

        let (verifier_set_tracker_pda, _) =
            solana_axelar_gateway::VerifierSetTracker::try_find_pda(
                &execute_data.signing_verifier_set_merkle_root,
            )
            .ok_or_else(|| {
                SolanaIncluderError::GenericError(
                    "Failed to derive verifier set tracker PDA".to_string(),
                )
            })?;

        let (gateway_root_pda, _) = solana_axelar_gateway::GatewayConfig::try_find_pda()
            .ok_or_else(|| {
                SolanaIncluderError::GenericError(
                    "Failed to derive gateway root config PDA".to_string(),
                )
            })?;

        let signing_verifier_set_leaves = execute_data.signing_verifier_set_leaves.clone();
        let instructions: Vec<Instruction> = signing_verifier_set_leaves
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

                Instruction {
                    program_id: solana_axelar_gateway::ID,
                    accounts: accounts.to_account_metas(None),
                    data: ix_data,
                }
            })
            .collect();

        // Build and submit verification txs
        let mut verification_futures = instructions
            .chunks(MAX_VERIFY_SIGNATURES_PER_TX)
            .map(|batch| self.verify_signature_batch(batch))
            .collect::<FuturesUnordered<_>>();

        // Wait for all verification txs to complete and collect the results
        while let Some(batch_cost) = verification_futures.next().await {
            total_cost += batch_cost?;
        }

        Ok(total_cost)
    }

    /// Verifies the signatures in one transaction and returns what it cost. A transaction
    /// is all or nothing, so if it fails (for example because an earlier attempt already
    /// verified one of them) each signature is verified in a transaction of its own.
    async fn verify_signature_batch(
        &self,
        instructions: &[Instruction],
    ) -> Result<u64, SolanaIncluderError> {
        if let [instruction] = instructions {
            return self.verify_signature(instruction).await;
        }

        match self
            .build_and_send_transaction(instructions, None, InstructionKind::VerifySignature)
            .await
        {
            Ok((signature, gas_cost)) => {
                debug!(
                    "Transaction for verifying {} signatures successfully: {}. Cost: {:?}",
                    instructions.len(),
                    signature,
                    gas_cost
                );
                Ok(gas_cost.unwrap_or(0))
            }
            Err(e) => {
                warn!(
                    "Failed to verify {} signatures in one transaction, verifying them separately: {}",
                    instructions.len(),
                    e
                );
                let mut verification_futures = instructions
                    .iter()
                    .map(|instruction| self.verify_signature(instruction))
                    .collect::<FuturesUnordered<_>>();

                let mut total_cost = 0;
                while let Some(cost) = verification_futures.next().await {
                    total_cost += cost?;
                }
                Ok(total_cost)
            }
        }
    }

    /// Verifies one signature in a transaction of its own and returns what it cost, which
    /// is nothing if the signature was already verified.
    async fn verify_signature(
        &self,
        instruction: &Instruction,
    ) -> Result<u64, SolanaIncluderError> {
        match self
            .build_and_send_transaction(
                std::slice::from_ref(instruction),
                None,
                InstructionKind::VerifySignature,
            )
            .await
        {
            Ok((signature, gas_cost)) => {
                debug!(
                    "Transaction for verifying signature successfully: {}. Cost: {:?}",
                    signature, gas_cost
                );
                Ok(gas_cost.unwrap_or(0))
            }
            Err(SolanaIncluderError::SlotAlreadyVerifiedError(e)) => {
                debug!("Signature already verified: {}", e);
                Ok(0)
            }
            Err(e) => Err(e),
        }
    }

    async fn rotate_signers(
        &self,
        execute_data: &ExecuteData,
        new_verifier_set_merkle_root: &[u8; 32],
    ) -> Result<Option<u64>, SolanaIncluderError> {
        // Current verifier set tracker (the signers approving the rotation)
        let (current_verifier_set_tracker_pda, _) =
            solana_axelar_gateway::VerifierSetTracker::try_find_pda(
                &execute_data.signing_verifier_set_merkle_root,
            )
            .ok_or_else(|| {
                SolanaIncluderError::GenericError(
                    "Failed to derive current verifier set tracker PDA".to_string(),
                )
            })?;

        // New verifier set tracker (the set we're rotating to)
        let (new_verifier_set_tracker_pda, _) =
            solana_axelar_gateway::VerifierSetTracker::try_find_pda(new_verifier_set_merkle_root)
                .ok_or_else(|| {
                SolanaIncluderError::GenericError(
                    "Failed to derive new verifier set tracker PDA".to_string(),
                )
            })?;

        let (gateway_root_pda, _) = solana_axelar_gateway::GatewayConfig::try_find_pda()
            .ok_or_else(|| {
                SolanaIncluderError::GenericError(
                    "Failed to derive gateway root config PDA".to_string(),
                )
            })?;

        let (verification_session_tracker_pda, _) =
            solana_axelar_gateway::SignatureVerificationSessionData::try_find_pda(
                &execute_data.payload_merkle_root,
                PayloadType::RotateSigners,
                &execute_data.signing_verifier_set_merkle_root,
            )
            .ok_or_else(|| {
                SolanaIncluderError::GenericError(
                    "Failed to derive verification session tracker PDA".to_string(),
                )
            })?;

        let (event_authority, _) = get_gateway_event_authority_pda()
            .map_err(|e| SolanaIncluderError::GenericError(e.to_string()))?;

        // Build RotateSigners instruction
        let ix_data = solana_axelar_gateway::instruction::RotateSigners {
            new_verifier_set_merkle_root: *new_verifier_set_merkle_root,
        }
        .data();
        let accounts = solana_axelar_gateway::accounts::RotateSigners {
            payer: self.keypair.pubkey(),
            program: solana_axelar_gateway::ID,
            system_program: solana_sdk_ids::system_program::ID,
            gateway_root_pda,
            verifier_set_tracker_pda: current_verifier_set_tracker_pda,
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

        // Build and send RotateSigners transaction
        let (signature, gas_cost) = self
            .build_and_send_transaction(&[ix], None, InstructionKind::Other)
            .await?;
        debug!(
            "Rotated signers transaction sent successfully: {}. Cost: {:?}",
            signature, gas_cost
        );
        Ok(gas_cost)
    }

    async fn approve_messages(
        &self,
        messages: Vec<MerklizedMessage>,
        execute_data: &ExecuteData,
    ) -> Result<Vec<(CrossChainId, u64)>, SolanaIncluderError> {
        let (gateway_root_pda, _) = solana_axelar_gateway::GatewayConfig::try_find_pda()
            .ok_or_else(|| {
                SolanaIncluderError::GenericError(
                    "Failed to derive gateway root config PDA".to_string(),
                )
            })?;

        let (verification_session_tracker_pda, _) =
            solana_axelar_gateway::SignatureVerificationSessionData::try_find_pda(
                &execute_data.payload_merkle_root,
                PayloadType::ApproveMessages,
                &execute_data.signing_verifier_set_merkle_root,
            )
            .ok_or_else(|| {
                SolanaIncluderError::GenericError(
                    "Failed to derive verification session tracker PDA".to_string(),
                )
            })?;

        let (event_authority, _) = get_gateway_event_authority_pda()
            .map_err(|e| SolanaIncluderError::GenericError(e.to_string()))?;

        // Build ApproveMessage instruction for each message
        let mut merkelised_message_futures = messages
            .into_iter()
            .map(|merklized_message| {
                let command_id = merklized_message.leaf.message.command_id();
                let (pda, _) = solana_axelar_gateway::IncomingMessage::try_find_pda(&command_id)
                    .ok_or_else(|| {
                        SolanaIncluderError::GenericError(
                            "Failed to derive incoming message PDA".to_string(),
                        )
                    })?;

                let ix_data = solana_axelar_gateway::instruction::ApproveMessage {
                    merklized_message: merklized_message.clone(),
                    payload_merkle_root: execute_data.payload_merkle_root,
                }
                .data();

                let accounts = solana_axelar_gateway::accounts::ApproveMessage {
                    funder: self.keypair.pubkey(),
                    incoming_message_pda: pda,
                    program: solana_axelar_gateway::ID,
                    system_program: solana_sdk_ids::system_program::ID,
                    gateway_root_pda,
                    verification_session_account: verification_session_tracker_pda,
                    event_authority,
                };

                let ix = Instruction {
                    program_id: solana_axelar_gateway::ID,
                    accounts: accounts.to_account_metas(None),
                    data: ix_data,
                };

                let cc_id = merklized_message.leaf.message.cc_id;
                Ok(async move {
                    (
                        cc_id,
                        self.build_and_send_transaction(
                            &[ix],
                            Some(execute_data),
                            InstructionKind::ApproveMessage,
                        )
                        .await,
                    )
                })
            })
            .collect::<Result<FuturesUnordered<_>, SolanaIncluderError>>()?;

        let mut approved_messages = vec![];
        while let Some((cc_id, result)) = merkelised_message_futures.next().await {
            match result {
                Ok((signature, gas_cost)) => {
                    approved_messages.push((cc_id.clone(), gas_cost.unwrap_or(0)));
                    debug!(
                        "Message approved successfully: cc_id={}:{}, signature: {}, cost: {:?}",
                        cc_id.chain, cc_id.id, signature, gas_cost
                    );
                }
                Err(e) => match e {
                    SolanaIncluderError::AccountInUseError(e) => {
                        debug!("Message already approved: {}", e);
                        approved_messages.push((cc_id, 0));
                        continue;
                    }
                    _ => {
                        error!("Failed to approve message: {}: {}", cc_id.id, e);
                    }
                },
            }
        }

        Ok(approved_messages)
    }
}

#[async_trait]
impl<
        G: GmpApiTrait + Clone,
        R: RedisConnectionTrait + Clone,
        RF: RefundsModel + Clone,
        IC: IncluderClientTrait + Clone,
        TB: TransactionBuilderTrait<IC, R> + Clone,
    > IncluderTrait for SolanaIncluder<G, R, RF, IC, TB>
{
    #[cfg_attr(
        feature = "instrumentation",
        tracing::instrument(skip(self), fields(message_id))
    )]
    async fn handle_gateway_tx_task(&self, task: GatewayTxTask) -> Result<(), IncluderError> {
        debug!("handle_gateway_tx_task: task_id={}", task.common.id);
        let execute_data_bytes = base64::prelude::BASE64_STANDARD
            .decode(task.task.execute_data)
            .map_err(|e| IncluderError::GenericError(e.to_string()))?;

        let execute_data = ExecuteData::try_from_slice(&execute_data_bytes)
            .map_err(|e| IncluderError::GenericError(e.to_string()))?;

        // Initialize payload verification session
        let init_verification_session_cost = match self
            .initialize_payload_verification_session(&execute_data)
            .await
        {
            Ok(verification) => verification,
            Err(e) => match e {
                SolanaIncluderError::AccountInUseError(e) => {
                    debug!("Account already in use: {}", e);
                    Some(0)
                }
                _ => return Err(IncluderError::GenericError(e.to_string())),
            },
        };

        self.redis_conn
            .add_gas_cost_for_task_id(
                task.common.id.clone(),
                init_verification_session_cost.unwrap_or(0),
                TransactionType::Approve,
            )
            .await;

        // Verify signatures
        let verify_signatures_result = self.verify_signatures(&execute_data).await;
        match verify_signatures_result {
            Ok(verify_signatures_cost) => {
                self.redis_conn
                    .add_gas_cost_for_task_id(
                        task.common.id.clone(),
                        verify_signatures_cost,
                        TransactionType::Approve,
                    )
                    .await;
            }
            Err(e) => match e {
                SolanaIncluderError::SlotAlreadyVerifiedError(e) => {
                    debug!("Signature already verified: {}", e);
                }
                _ => {
                    error!(
                        "Failed to verify signatures for task id: {}: {}",
                        task.common.id, e
                    );
                    return Err(IncluderError::GenericError(e.to_string()));
                }
            },
        }

        match &execute_data.payload_items {
            MerklizedPayload::VerifierSetRotation {
                new_verifier_set_merkle_root,
            } => {
                self.rotate_signers(&execute_data, new_verifier_set_merkle_root)
                    .await
                    .map_err(|e| IncluderError::GenericError(e.to_string()))?;
            }
            MerklizedPayload::NewMessages { messages } => {
                let approved_messages = self
                    .approve_messages(messages.clone(), &execute_data)
                    .await
                    .map_err(|e| IncluderError::GenericError(e.to_string()))?;

                // The overhead cost is the initialize payload verification session and the total cost of verifying all signatures
                // divided by the number of messages. The total cost for the message is the overhead plus its own cost.
                if !approved_messages.is_empty() {
                    let overhead_cost = self
                        .redis_conn
                        .get_gas_cost_for_task_id(task.common.id.clone(), TransactionType::Approve)
                        .await
                        .map_err(|e| IncluderError::GenericError(e.to_string()))?;
                    let overhead_cost_per_message = overhead_cost
                        .checked_div(messages.len() as u64)
                        .unwrap_or(0);
                    debug!(
                        "Distributing overhead: task_id={}, total_overhead={}, per_message={}, num_messages={}",
                        task.common.id, overhead_cost, overhead_cost_per_message, messages.len()
                    );
                    for (cc_id, gas_cost) in &approved_messages {
                        self.redis_conn
                            .write_gas_cost_for_message_id(
                                cc_id.id.clone(),
                                gas_cost.saturating_add(overhead_cost_per_message),
                                TransactionType::Approve,
                            )
                            .await;
                    }
                }

                if approved_messages.len() != messages.len() {
                    error!("Failed to approve all messages for task id: {}. Approved: {}, Expected: {}", task.common.id, approved_messages.len(), messages.len());
                    return Err(IncluderError::GenericError(format!("Failed to approve all messages for task id: {}. Approved: {}, Expected: {}", task.common.id, approved_messages.len(), messages.len())));
                }
            }
        };

        Ok(())
    }

    #[cfg_attr(
        feature = "instrumentation",
        tracing::instrument(skip(self), fields(message_id))
    )]
    async fn handle_execute_task(&self, task: ExecuteTask) -> Result<Vec<Event>, IncluderError> {
        debug!(
            "handle_execute_task: task_id={}, message_id={}, source_chain={}, destination_address={}",
            task.common.id, task.task.message.message_id,
            task.task.message.source_chain, task.task.message.destination_address
        );
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
        let (gateway_incoming_message_pda, _) =
            solana_axelar_gateway::IncomingMessage::try_find_pda(&command_id).ok_or_else(|| {
                IncluderError::GenericError("Failed to derive incoming message PDA".to_string())
            })?;

        if self
            .client
            .incoming_message_already_executed(&gateway_incoming_message_pda)
            .await
            .map_err(|e| IncluderError::GenericError(e.to_string()))?
        {
            tracing::warn!(
                "incoming message already executed: message_id={}, task_id={}",
                task.task.message.message_id,
                task.common.id
            );
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

        let (instruction, entrypoint) = match self
            .transaction_builder
            .build_execute_instruction(
                &message,
                base64::prelude::BASE64_STANDARD
                    .decode(task.task.payload.clone())
                    .map_err(|e| IncluderError::GenericError(e.to_string()))?
                    .as_slice(),
                destination_address,
            )
            .await
        {
            Ok((instruction, entrypoint)) => (instruction, entrypoint),
            Err(e) => match e {
                TransactionBuilderError::PayloadDecodeError(e) => {
                    error!(
                        task_id = %task.common.id,
                        message_id = %task.task.message.message_id,
                        source_chain = %task.task.message.source_chain,
                        error = %e,
                        "Failed to decode payload: cannot execute message"
                    );
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

        self.build_execute_transaction_and_send(instruction, task, entrypoint)
            .await
            .map_err(|e| IncluderError::GenericError(e.to_string()))
    }

    #[cfg_attr(
        feature = "instrumentation",
        tracing::instrument(skip(self), fields(refund_id))
    )]
    async fn handle_refund_task(&self, task: RefundTask) -> Result<(), IncluderError> {
        debug!(
            "handle_refund_task: refund_id={}, message_id={}",
            task.common.id, task.task.message.message_id
        );
        let refund_id = task.common.id.clone();
        if self.refund_already_processed(refund_id.clone()).await? {
            warn!("Refund already processed: {}", refund_id);
            return Ok(());
        }

        let receiver = Pubkey::from_str(&task.task.refund_recipient_address)
            .map_err(|e| IncluderError::GenericError(e.to_string()))?;
        let (operator_pda, _) =
            solana_axelar_operators::OperatorAccount::try_find_pda(&self.keypair.pubkey())
                .ok_or_else(|| {
                    IncluderError::GenericError("Failed to derive operator PDA".to_string())
                })?;
        let (treasury, _) =
            solana_axelar_gas_service::Treasury::try_find_pda().ok_or_else(|| {
                IncluderError::GenericError("Failed to derive treasury PDA".to_string())
            })?;
        let (event_authority, _) = get_gas_service_event_authority_pda()
            .map_err(|e| IncluderError::GenericError(e.to_string()))?;

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

        debug!(
            "Sending refund transaction - task_id: {}, receiver: {}, accounts: {:?}, refund_amount: {}",
            refund_id,
            receiver,
            ix.accounts,
            refund_amount,
        );

        let (tx, estimated_tx_cost) = self
            .transaction_builder
            .build(&[ix], InstructionKind::Other)
            .await
            .map_err(|e| IncluderError::GenericError(e.to_string()))?;

        if estimated_tx_cost >= refund_amount {
            warn!(
                "Cost is higher than remaining balance to refund. Cost: {}, Remaining balance: {}",
                estimated_tx_cost, refund_amount
            );
            return Ok(());
        }

        // write the signature to the database before sending the transaction to avoid
        // re processing it if we do not capture the success/failure response
        self.refunds_model
            .upsert(
                refund_id,
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
            .send_transaction(tx, None)
            .await
            .map_err(|e| IncluderError::GenericError(e.to_string()))?;

        info!(
            "Refund fees transaction sent successfully: {}",
            signature.to_string()
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gas_estimation::ExecuteEntrypoint;
    use crate::includer_client::MockIncluderClientTrait;
    use crate::models::refunds::MockRefundsModel;
    use crate::redis::MockRedisConnectionTrait;
    use crate::transaction_builder::MockTransactionBuilderTrait;
    use crate::transaction_type::SolanaTransactionType;
    use crate::utils::create_transaction;
    use base64::prelude::BASE64_STANDARD;

    use relayer_core::gmp_api::MockGmpApiTrait;

    use solana_axelar_std::{
        MerklizedMessage, MessageLeaf, PublicKey, SigningVerifierSetInfo, VerifierSetLeaf,
    };
    use solana_sdk::hash::Hash;
    use solana_sdk::instruction::AccountMeta;
    use solana_sdk::pubkey::Pubkey;
    use solana_sdk::transaction::{Transaction, TransactionError};
    use solana_transaction_parser::gmp_types::{
        Amount, CannotExecuteMessageReason, CommonEventFields, CommonTaskFields, Event,
        ExecuteTask, ExecuteTaskFields, GatewayV2Message, RefundTask,
    };
    use solana_transaction_parser::gmp_types::{GatewayTxTaskFields, RefundTaskFields};
    use std::str::FromStr;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    impl Clone for MockIncluderClientTrait {
        fn clone(&self) -> Self {
            Self::new()
        }
    }

    impl Clone for MockTransactionBuilderTrait<MockIncluderClientTrait, MockRedisConnectionTrait> {
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
        MockTransactionBuilderTrait<MockIncluderClientTrait, MockRedisConnectionTrait>,
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

    fn create_execute_task(
        task_id: &str,
        message_id: &str,
        source_chain: &str,
        destination_address: &str,
        available_gas: u64,
    ) -> ExecuteTask {
        ExecuteTask {
            common: CommonTaskFields {
                id: task_id.to_string(),
                chain: "test-chain".to_string(),
                timestamp: Utc::now().to_string(),
                r#type: "execute".to_string(),
                meta: None,
            },
            task: ExecuteTaskFields {
                message: GatewayV2Message {
                    message_id: message_id.to_string(),
                    source_chain: source_chain.to_string(),
                    destination_address: destination_address.to_string(),
                    payload_hash: BASE64_STANDARD.encode([0u8; 32]),
                    source_address: Pubkey::new_unique().to_string(),
                },
                payload: BASE64_STANDARD.encode(b"test-payload"),
                available_gas_balance: Amount {
                    amount: available_gas.to_string(),
                    token_id: None,
                },
            },
        }
    }

    fn create_gateway_tx_task(task_id: &str, execute_data: &ExecuteData) -> GatewayTxTask {
        GatewayTxTask {
            common: CommonTaskFields {
                id: task_id.to_string(),
                chain: "test-chain".to_string(),
                timestamp: Utc::now().to_string(),
                r#type: "gateway_tx".to_string(),
                meta: None,
            },
            task: GatewayTxTaskFields {
                execute_data: BASE64_STANDARD.encode(borsh::to_vec(&execute_data).unwrap()),
            },
        }
    }

    fn create_signed_tx(keypair: &Keypair) -> (Transaction, Signature) {
        let mut tx = Transaction::new_with_payer(&[], Some(&keypair.pubkey()));
        tx.sign(&[keypair], Hash::default());
        let sig = tx.signatures[0];
        (tx, sig)
    }

    fn create_test_includer(
        mock_client: MockIncluderClientTrait,
        keypair: Keypair,
        chain_name: String,
        transaction_builder: MockTransactionBuilderTrait<
            MockIncluderClientTrait,
            MockRedisConnectionTrait,
        >,
        mock_gmp_api: MockGmpApiTrait,
        redis_conn: MockRedisConnectionTrait,
        mock_refunds_model: MockRefundsModel,
    ) -> SolanaIncluder<
        MockGmpApiTrait,
        MockRedisConnectionTrait,
        MockRefundsModel,
        MockIncluderClientTrait,
        MockTransactionBuilderTrait<MockIncluderClientTrait, MockRedisConnectionTrait>,
    > {
        SolanaIncluder::new(
            Arc::new(mock_client),
            Arc::new(keypair),
            chain_name,
            transaction_builder,
            Arc::new(mock_gmp_api),
            redis_conn,
            Arc::new(mock_refunds_model),
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

        let includer = create_test_includer(
            mock_client,
            keypair,
            chain_name,
            transaction_builder,
            mock_gmp_api,
            redis_conn,
            mock_refunds_model,
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
        let includer = create_test_includer(
            mock_client,
            keypair,
            chain_name,
            transaction_builder,
            mock_gmp_api,
            redis_conn,
            mock_refunds_model,
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

        let includer = create_test_includer(
            mock_client,
            keypair,
            chain_name,
            transaction_builder,
            mock_gmp_api,
            redis_conn,
            mock_refunds_model,
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

        let includer = create_test_includer(
            mock_client,
            keypair,
            chain_name,
            transaction_builder,
            mock_gmp_api,
            redis_conn,
            mock_refunds_model,
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

        let includer = create_test_includer(
            mock_client,
            keypair,
            chain_name,
            transaction_builder,
            mock_gmp_api,
            redis_conn,
            mock_refunds_model,
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

        let includer = create_test_includer(
            mock_client,
            keypair,
            chain_name,
            transaction_builder,
            mock_gmp_api,
            redis_conn,
            mock_refunds_model,
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

        let (test_tx, test_signature) = create_signed_tx(&keypair);

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
            .returning(move |_, _| Box::pin(async move { Ok((test_signature, Some(5000u64))) }));

        let includer = create_test_includer(
            mock_client,
            keypair,
            chain_name,
            transaction_builder,
            mock_gmp_api,
            redis_conn,
            mock_refunds_model,
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

        // Mock transaction_builder.build to report a cost above the refund amount
        let (test_tx, _) = create_signed_tx(&keypair);
        transaction_builder
            .expect_build()
            .times(1)
            .returning(move |_, _| {
                Ok((
                    SolanaTransactionType::Legacy(test_tx.clone()),
                    2_000_000u64, // Higher than refund_amount of 1_000_000
                ))
            });

        // Should not reach send_transaction since the check fails
        mock_client.expect_send_transaction().times(0);

        let includer = create_test_includer(
            mock_client,
            keypair,
            chain_name,
            transaction_builder,
            mock_gmp_api,
            redis_conn,
            mock_refunds_model,
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

        assert!(result.is_ok());
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
        let source_chain = "ethereum".to_string();
        let destination_address = solana_axelar_governance::ID.to_string();
        let available_gas = 200_000u64; // enough lamports to cover transaction cost

        mock_client
            .expect_incoming_message_already_executed()
            .times(1)
            .returning(|_| Box::pin(async move { Ok(false) }));

        let test_instruction =
            Instruction::new_with_bytes(solana_axelar_governance::ID, &[1, 2, 3, 4], vec![]);
        let instruction_for_mock = test_instruction.clone();
        transaction_builder
            .expect_build_execute_instruction()
            .times(1)
            .returning(move |_, _, _| Ok((instruction_for_mock.clone(), ExecuteEntrypoint::Other)));

        let (test_tx, test_signature) = create_signed_tx(&keypair);
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

        mock_client
            .expect_send_transaction()
            .times(1)
            .returning(move |_, _| Box::pin(async move { Ok((test_signature, Some(5_000u64))) }));

        redis_conn
            .expect_write_gas_cost_for_message_id()
            .withf(move |id, cost, tx_type| {
                *id == message_id_clone
                    && *cost == 5_000u64
                    && matches!(tx_type, TransactionType::Execute)
            })
            .times(1)
            .returning(|_, _, _| ());

        let includer = create_test_includer(
            mock_client,
            keypair,
            chain_name,
            transaction_builder,
            mock_gmp_api,
            redis_conn,
            mock_refunds_model,
        );

        let task = create_execute_task(
            "test-execute-task-123",
            &message_id,
            &source_chain,
            &destination_address,
            available_gas,
        );
        let result = includer.handle_execute_task(task).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), vec![]);
    }

    #[cfg(not(feature = "devnet-amplifier"))]
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
        let source_chain = "ethereum".to_string();
        let destination_address = solana_axelar_governance::ID.to_string();
        let available_gas = 1_000u64;

        mock_client
            .expect_incoming_message_already_executed()
            .times(1)
            .returning(|_| Box::pin(async move { Ok(false) }));

        let test_instruction =
            Instruction::new_with_bytes(solana_axelar_governance::ID, &[1, 2, 3, 4], vec![]);
        let instruction_for_mock = test_instruction.clone();
        transaction_builder
            .expect_build_execute_instruction()
            .times(1)
            .returning(move |_, _, _| Ok((instruction_for_mock.clone(), ExecuteEntrypoint::Other)));

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

        mock_client.expect_send_transaction().times(0);
        redis_conn.expect_write_gas_cost_for_message_id().times(0);

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

        let includer = create_test_includer(
            mock_client,
            keypair,
            chain_name,
            transaction_builder,
            mock_gmp_api,
            redis_conn,
            mock_refunds_model,
        );

        let task = create_execute_task(
            "test-execute-task-456",
            &message_id,
            &source_chain,
            &destination_address,
            available_gas,
        );
        let result = includer.handle_execute_task(task).await;

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

        mock_client
            .expect_incoming_message_already_executed()
            .times(1)
            .returning(|_| Box::pin(async move { Ok(false) }));

        let executable_program = Pubkey::new_unique();
        let test_instruction =
            Instruction::new_with_bytes(executable_program, &[5, 6, 7, 8], vec![]);
        transaction_builder
            .expect_build_execute_instruction()
            .times(1)
            .returning(move |_, _, _| Ok((test_instruction.clone(), ExecuteEntrypoint::Other)));

        let (test_tx, test_signature) = create_signed_tx(&keypair);
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

        mock_client
            .expect_send_transaction()
            .times(1)
            .returning(move |_, _| Box::pin(async move { Ok((test_signature, Some(5_000u64))) }));

        redis_conn
            .expect_write_gas_cost_for_message_id()
            .withf(move |id, cost, tx_type| {
                *id == message_id_clone
                    && *cost == 5_000u64
                    && matches!(tx_type, TransactionType::Execute)
            })
            .times(1)
            .returning(|_, _, _| ());

        let includer = create_test_includer(
            mock_client,
            keypair,
            chain_name,
            transaction_builder,
            mock_gmp_api,
            redis_conn,
            mock_refunds_model,
        );

        let task = create_execute_task(
            "test-execute-task-789",
            &message_id,
            &source_chain,
            &destination_address,
            available_gas,
        );
        let result = includer.handle_execute_task(task).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), vec![]);
    }

    #[cfg(not(feature = "devnet-amplifier"))]
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

        mock_client
            .expect_incoming_message_already_executed()
            .times(1)
            .returning(|_| Box::pin(async move { Ok(false) }));

        let executable_program = Pubkey::new_unique();
        let test_instruction =
            Instruction::new_with_bytes(executable_program, &[9, 10, 11, 12], vec![]);
        let instruction_for_mock = test_instruction.clone();
        transaction_builder
            .expect_build_execute_instruction()
            .times(1)
            .returning(move |_, _, _| Ok((instruction_for_mock.clone(), ExecuteEntrypoint::Other)));

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

        // Should not reach send_transaction
        mock_client.expect_send_transaction().times(0);
        redis_conn.expect_write_gas_cost_for_message_id().times(0);
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

        let includer = create_test_includer(
            mock_client,
            keypair,
            chain_name,
            transaction_builder,
            mock_gmp_api,
            redis_conn,
            mock_refunds_model,
        );

        let task = create_execute_task(
            "test-execute-task-999",
            &message_id,
            &source_chain,
            &destination_address,
            available_gas,
        );
        let result = includer.handle_execute_task(task).await;

        assert!(result.is_ok());
        let events = result.unwrap();
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], Event::CannotExecuteMessageV2 { .. }));
    }

    /// A real `DeployInterchainToken` payload whose prepaid gas covers the consensus fee but not
    /// the rent for the accounts it creates must be rejected before any transaction is sent. The
    /// consensus-only estimate (`consensus_fee`) is *below* the available gas, so the pre-fix
    /// check (consensus only) would have executed this and the relayer would have eaten the PDA
    /// rent. The static rent the gate adds for a deploy is what tips the total past the available
    /// gas — so this fails if the gate ever stops adding rent for deploys.
    #[cfg(not(feature = "devnet-amplifier"))]
    #[tokio::test]
    async fn test_handle_execute_task_rejected_when_gas_covers_fee_but_not_rent() {
        let (
            mut mock_gmp_api,
            keypair,
            chain_name,
            mut redis_conn,
            mock_refunds_model,
            mut mock_client,
            mut transaction_builder,
        ) = get_includer_fields();

        let message_id = "test-execute-deploy-rent-001".to_string();
        let source_chain = "ethereum".to_string();
        // Sits in the gap the old consensus-only check missed: above the consensus fee, far
        // below consensus fee + deploy rent (~20.5M).
        let available_gas = 300_000u64;
        let consensus_fee = 28_470u64;

        mock_client
            .expect_incoming_message_already_executed()
            .times(1)
            .returning(|_| Box::pin(async move { Ok(false) }));

        let exec_ix = Instruction::new_with_bytes(
            solana_axelar_its::ID,
            &[1, 2, 3, 4],
            vec![AccountMeta::new(keypair.pubkey(), true)],
        );
        let exec_ix_for_builder = exec_ix.clone();
        // The builder classifies this as a deploy (resolving the entrypoint), so the gate adds
        // the static deploy rent on top of the consensus fee.
        transaction_builder
            .expect_build_execute_instruction()
            .times(1)
            .returning(move |_, _, _| {
                Ok((
                    exec_ix_for_builder.clone(),
                    ExecuteEntrypoint::DeployInterchainToken { has_minter: false },
                ))
            });

        let mut main_tx =
            Transaction::new_with_payer(std::slice::from_ref(&exec_ix), Some(&keypair.pubkey()));
        main_tx.sign(&[&keypair], Hash::default());
        let main_tx_clone = main_tx.clone();
        transaction_builder
            .expect_build()
            .times(1)
            .returning(move |_, _| {
                Ok((
                    SolanaTransactionType::Legacy(main_tx_clone.clone()),
                    consensus_fee,
                ))
            });

        // Rejected before sending: no transaction, no recorded cost.
        mock_client.expect_send_transaction().times(0);
        redis_conn.expect_write_gas_cost_for_message_id().times(0);

        let msg_id_for_event = message_id.clone();
        mock_gmp_api
            .expect_cannot_execute_message()
            .times(1)
            .withf(move |_id, msg_id, _src, details, reason| {
                *msg_id == msg_id_for_event
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

        let includer = create_test_includer(
            mock_client,
            keypair,
            chain_name,
            transaction_builder,
            mock_gmp_api,
            redis_conn,
            mock_refunds_model,
        );

        let task = create_execute_task(
            "test-execute-deploy-rent-task-001",
            &message_id,
            &source_chain,
            &solana_axelar_its::ID.to_string(),
            available_gas,
        );
        let result = includer.handle_execute_task(task).await;

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

        let includer = create_test_includer(
            mock_client,
            keypair,
            chain_name,
            transaction_builder,
            mock_gmp_api,
            redis_conn,
            mock_refunds_model,
        );

        let task = create_execute_task(
            "test-execute-task-invalid-destination-001",
            &message_id,
            &source_chain,
            &destination_address,
            1000,
        );
        let result = includer.handle_execute_task(task).await;

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
            redis_conn,
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

        let payload_decode_error_clone = payload_decode_error.clone();
        transaction_builder
            .expect_build_execute_instruction()
            .times(1)
            .returning(move |_, _, _| {
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

        let includer = create_test_includer(
            mock_client,
            keypair,
            chain_name,
            transaction_builder,
            mock_gmp_api,
            redis_conn,
            mock_refunds_model,
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
                    payload: BASE64_STANDARD.encode(b"malformed-payload"),
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
    async fn test_handle_gateway_tx_task_rotate_signers_with_one_signature_success() {
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
        let new_verifier_set_merkle_root = [3u8; 32];

        let verifier_info = SigningVerifierSetInfo {
            payload_type: PayloadType::RotateSigners,
            leaf: VerifierSetLeaf {
                nonce: 0,
                quorum: 0,
                signer_pubkey: PublicKey([0; 33]),
                signer_weight: 0,
                position: 0,
                set_size: 0,
                domain_separator: [0; 32],
            },
            merkle_proof: vec![0xDD, 0xEE, 0xFF],
            signature: solana_axelar_std::Signature([0; 65]),
        };

        let execute_data = ExecuteData {
            payload_merkle_root,
            signing_verifier_set_merkle_root,
            signing_verifier_set_leaves: vec![verifier_info],
            payload_items: MerklizedPayload::VerifierSetRotation {
                new_verifier_set_merkle_root,
            },
        };

        let execute_data_b64 =
            base64::prelude::BASE64_STANDARD.encode(borsh::to_vec(&execute_data).unwrap());

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

        let (init_tx, init_sig) = create_signed_tx(&keypair);
        let (verify_tx, verify_sig) = create_signed_tx(&keypair);
        let (rotate_tx, rotate_sig) = create_signed_tx(&keypair);

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
            .returning(move |_, _| {
                let idx = send_calls_clone.fetch_add(1, Ordering::SeqCst);
                let (signature, cost) = send_responses[idx];
                Box::pin(async move { Ok((signature, Some(cost))) })
            });

        // Expect add_gas_cost_for_task_id for init (10) and verify (20)
        redis_conn
            .expect_add_gas_cost_for_task_id()
            .times(2)
            .withf(move |task_id, cost, tx_type| {
                *task_id == "rotate-signer-happy"
                    && matches!(tx_type, TransactionType::Approve)
                    && (*cost == 10 || *cost == 20)
            })
            .returning(|_, _, _| ());

        let includer = create_test_includer(
            mock_client,
            keypair,
            chain_name,
            transaction_builder,
            mock_gmp_api,
            redis_conn,
            mock_refunds_model,
        );

        let result = includer.handle_gateway_tx_task(task).await;

        assert!(result.is_ok());
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
            payload_type: PayloadType::ApproveMessages,
            leaf: VerifierSetLeaf {
                nonce: 0,
                quorum: 0,
                signer_pubkey: PublicKey([0; 33]),
                signer_weight: 0,
                position: 0,
                set_size: 0,
                domain_separator: [0; 32],
            },
            merkle_proof: vec![0xDD, 0xEE, 0xFF],
            signature: solana_axelar_std::Signature([0; 65]),
        };

        let execute_data = ExecuteData {
            payload_merkle_root,
            signing_verifier_set_merkle_root,
            signing_verifier_set_leaves: vec![verifier_info],
            payload_items: MerklizedPayload::NewMessages {
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

        let task = create_gateway_tx_task("approve-message-happy", &execute_data);

        let (init_tx, init_sig) = create_signed_tx(&keypair);
        let (verify_tx, verify_sig) = create_signed_tx(&keypair);
        let (approve_tx, approve_sig) = create_signed_tx(&keypair);

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
            .returning(move |_, _| {
                let idx = send_calls_clone.fetch_add(1, Ordering::SeqCst);
                let (signature, cost) = send_responses[idx];
                Box::pin(async move { Ok((signature, Some(cost))) })
            });

        // Expect add_gas_cost_for_task_id for init (10) and verify (20)
        redis_conn
            .expect_add_gas_cost_for_task_id()
            .times(2)
            .withf(move |task_id, cost, tx_type| {
                *task_id == "approve-message-happy"
                    && matches!(tx_type, TransactionType::Approve)
                    && (*cost == 10 || *cost == 20)
            })
            .returning(|_, _, _| ());

        // Expect get_gas_cost_for_task_id to read overhead (10 + 20 = 30)
        redis_conn
            .expect_get_gas_cost_for_task_id()
            .times(1)
            .withf(move |task_id, tx_type| {
                *task_id == "approve-message-happy" && matches!(tx_type, TransactionType::Approve)
            })
            .returning(|_, _| Ok(30u64));

        // Expect write_gas_cost_for_message_id with overhead (30) + approve cost (30) = 60
        redis_conn
            .expect_write_gas_cost_for_message_id()
            .times(1)
            .withf(move |msg_id, cost, tx_type| {
                *msg_id == "test-message-id"
                    && *cost == 60
                    && matches!(tx_type, TransactionType::Approve)
            })
            .returning(|_, _, _| ());

        let includer = create_test_includer(
            mock_client,
            keypair,
            chain_name,
            transaction_builder,
            mock_gmp_api,
            redis_conn,
            mock_refunds_model,
        );

        let result = includer.handle_gateway_tx_task(task).await;

        assert!(result.is_ok());
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
            payload_type: PayloadType::ApproveMessages,
            leaf: VerifierSetLeaf {
                nonce: 0,
                quorum: 0,
                signer_pubkey: PublicKey([1; 33]),
                signer_weight: 0,
                position: 0,
                set_size: 0,
                domain_separator: [0; 32],
            },
            merkle_proof: vec![0xAA],
            signature: solana_axelar_std::Signature([1; 65]),
        };

        let verifier_info_2 = SigningVerifierSetInfo {
            payload_type: PayloadType::ApproveMessages,
            leaf: VerifierSetLeaf {
                nonce: 1,
                quorum: 0,
                signer_pubkey: PublicKey([2; 33]),
                signer_weight: 0,
                position: 1,
                set_size: 0,
                domain_separator: [0; 32],
            },
            merkle_proof: vec![0xBB],
            signature: solana_axelar_std::Signature([2; 65]),
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
            payload_items: MerklizedPayload::NewMessages {
                messages: vec![merkle_msg_1, merkle_msg_2],
            },
        };

        let execute_data_b64 =
            base64::prelude::BASE64_STANDARD.encode(borsh::to_vec(&execute_data).unwrap());

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

        // Both signatures are verified in one transaction
        let mut verify_tx = Transaction::new_with_payer(&[], Some(&keypair.pubkey()));
        verify_tx.sign(&[&keypair], Hash::default());
        let verify_sig = verify_tx.signatures[0];

        let mut approve_tx_1 = Transaction::new_with_payer(&[], Some(&keypair.pubkey()));
        approve_tx_1.sign(&[&keypair], Hash::default());
        let approve_sig_1 = approve_tx_1.signatures[0];

        let mut approve_tx_2 = Transaction::new_with_payer(&[], Some(&keypair.pubkey()));
        approve_tx_2.sign(&[&keypair], Hash::default());
        let approve_sig_2 = approve_tx_2.signatures[0];

        let init_tx_clone = init_tx.clone();
        let verify_tx_clone = verify_tx.clone();
        let approve_tx_1_clone = approve_tx_1.clone();
        let approve_tx_2_clone = approve_tx_2.clone();

        let build_calls = Arc::new(AtomicUsize::new(0));
        let build_calls_clone = Arc::clone(&build_calls);
        transaction_builder
            .expect_build()
            .times(4)
            .returning(move |ixs, _| {
                let idx = build_calls_clone.fetch_add(1, Ordering::SeqCst);
                match idx {
                    0 => Ok((
                        crate::transaction_type::SolanaTransactionType::Legacy(
                            init_tx_clone.clone(),
                        ),
                        100_000u64,
                    )),
                    1 => {
                        assert_eq!(ixs.len(), 2);
                        Ok((
                            crate::transaction_type::SolanaTransactionType::Legacy(
                                verify_tx_clone.clone(),
                            ),
                            100_000u64,
                        ))
                    }
                    2 => Ok((
                        crate::transaction_type::SolanaTransactionType::Legacy(
                            approve_tx_1_clone.clone(),
                        ),
                        100_000u64,
                    )),
                    3 => Ok((
                        crate::transaction_type::SolanaTransactionType::Legacy(
                            approve_tx_2_clone.clone(),
                        ),
                        100_000u64,
                    )),
                    _ => panic!("unexpected build call"),
                }
            });

        // total_overhead = 10 + 20 = 30
        // per-message overhead = 30 / 2 = 15
        // msg1_cost = 30 + 15 = 45
        // msg2_cost = 40 + 15 = 55
        let send_calls = Arc::new(AtomicUsize::new(0));
        let send_calls_clone = Arc::clone(&send_calls);
        let send_responses = [
            (init_sig, 10u64),
            (verify_sig, 20u64),
            (approve_sig_1, 30u64),
            (approve_sig_2, 40u64),
        ];

        mock_client
            .expect_send_transaction()
            .times(4)
            .returning(move |_, _| {
                let idx = send_calls_clone.fetch_add(1, Ordering::SeqCst);
                let (sig, cost) = send_responses[idx];
                Box::pin(async move { Ok((sig, Some(cost))) })
            });

        // add_gas_cost_for_task_id is called twice:
        // 1. init cost: 10
        // 2. verify cost: 20 (one transaction for both signatures)
        let add_calls = Arc::new(AtomicUsize::new(0));
        let add_calls_clone = Arc::clone(&add_calls);
        redis_conn
            .expect_add_gas_cost_for_task_id()
            .times(2)
            .withf(move |task_id, cost, tx_type| {
                let idx = add_calls_clone.fetch_add(1, Ordering::SeqCst);
                *task_id == "approve-message-two-msgs-two-sigs"
                    && matches!(tx_type, TransactionType::Approve)
                    && match idx {
                        0 => *cost == 10, // init cost
                        1 => *cost == 20, // verify cost
                        _ => false,
                    }
            })
            .returning(|_, _, _| ());

        // get_gas_cost_for_task_id returns overhead (10 + 20 = 30)
        redis_conn
            .expect_get_gas_cost_for_task_id()
            .times(1)
            .withf(move |task_id, tx_type| {
                *task_id == "approve-message-two-msgs-two-sigs"
                    && matches!(tx_type, TransactionType::Approve)
            })
            .returning(|_, _| Ok(30u64));

        // Expect write_gas_cost_for_message_id with overhead per message (30/2 = 15) + approve cost
        // msg1: 15 + 30 = 45, msg2: 15 + 40 = 55
        let expected_id_1 = msg_id_1.clone();
        let expected_id_2 = msg_id_2.clone();
        redis_conn
            .expect_write_gas_cost_for_message_id()
            .times(2)
            .withf(move |msg_id, cost, tx_type| {
                if msg_id == &expected_id_1 {
                    *cost == 45 && matches!(tx_type, TransactionType::Approve)
                } else if msg_id == &expected_id_2 {
                    *cost == 55 && matches!(tx_type, TransactionType::Approve)
                } else {
                    false
                }
            })
            .returning(|_, _, _| ());

        let includer = create_test_includer(
            mock_client,
            keypair,
            chain_name,
            transaction_builder,
            mock_gmp_api,
            redis_conn,
            mock_refunds_model,
        );

        let result = includer.handle_gateway_tx_task(task).await;

        assert!(result.is_ok());
    }

    /// A Merkle proof as long as mainnet's: 5 levels for a set of 18 verifiers.
    const MERKLE_PROOF_LEN: usize = 5 * 32;

    /// Execute data signed by `signatures` verifiers.
    fn execute_data_with_signatures(signatures: u16, merkle_proof_len: usize) -> ExecuteData {
        let signing_verifier_set_leaves = (0..signatures)
            .map(|position| SigningVerifierSetInfo {
                payload_type: PayloadType::ApproveMessages,
                leaf: VerifierSetLeaf {
                    nonce: 0,
                    quorum: 0,
                    signer_pubkey: PublicKey([1; 33]),
                    signer_weight: 0,
                    position,
                    set_size: 0,
                    domain_separator: [0; 32],
                },
                merkle_proof: vec![0; merkle_proof_len],
                signature: solana_axelar_std::Signature([1; 65]),
            })
            .collect();

        ExecuteData {
            payload_merkle_root: [1; 32],
            signing_verifier_set_merkle_root: [2; 32],
            signing_verifier_set_leaves,
            payload_items: MerklizedPayload::NewMessages { messages: vec![] },
        }
    }

    /// Makes the mocked builder put the verify instructions it is given in a real v1
    /// transaction, and the mocked client answer each transaction through `respond`.
    /// Returns how many signatures each transaction handed to the client carried.
    fn mock_verify_transactions(
        transaction_builder: &mut MockTransactionBuilderTrait<
            MockIncluderClientTrait,
            MockRedisConnectionTrait,
        >,
        mock_client: &mut MockIncluderClientTrait,
        mut respond: impl FnMut(u64) -> Result<u64, IncluderClientError> + Send + 'static,
    ) -> Arc<Mutex<Vec<u64>>> {
        let payer = Keypair::new();
        transaction_builder
            .expect_build()
            .returning(move |ixs, kind| {
                assert_eq!(kind, InstructionKind::VerifySignature);
                create_transaction(ixs, 0, 0, &payer, &[&payer], Hash::default())
                    .map(|tx| (tx, 0))
                    .map_err(|e| TransactionBuilderError::CreateTransactionError(e.to_string()))
            });

        let sent = Arc::new(Mutex::new(Vec::new()));
        let sent_clone = Arc::clone(&sent);
        mock_client
            .expect_send_transaction()
            .returning(move |tx, _| {
                let signatures = tx.get_num_instructions();
                sent_clone.lock().unwrap().push(signatures);
                let response = respond(signatures).map(|cost| (Signature::default(), Some(cost)));
                Box::pin(async move { response })
            });
        sent
    }

    #[tokio::test]
    async fn test_verify_signatures_sends_six_signatures_per_transaction() {
        let (
            mock_gmp_api,
            keypair,
            chain_name,
            redis_conn,
            mock_refunds_model,
            mut mock_client,
            mut transaction_builder,
        ) = get_includer_fields();

        let sent =
            mock_verify_transactions(&mut transaction_builder, &mut mock_client, |signatures| {
                Ok(if signatures == 1 { 5_220 } else { 6_320 })
            });

        let includer = create_test_includer(
            mock_client,
            keypair,
            chain_name,
            transaction_builder,
            mock_gmp_api,
            redis_conn,
            mock_refunds_model,
        );

        let total_cost = includer
            .verify_signatures(&execute_data_with_signatures(13, MERKLE_PROOF_LEN))
            .await
            .unwrap();

        let mut sent = sent.lock().unwrap().clone();
        sent.sort_unstable();
        assert_eq!(sent, vec![1, 6, 6]);
        // The cost is what the three transactions cost
        assert_eq!(total_cost, 6_320 + 6_320 + 5_220);
    }

    #[tokio::test]
    async fn test_verify_signatures_falls_back_to_one_signature_per_transaction() {
        let (
            mock_gmp_api,
            keypair,
            chain_name,
            redis_conn,
            mock_refunds_model,
            mut mock_client,
            mut transaction_builder,
        ) = get_includer_fields();

        // An earlier attempt verified one of the six signatures, so the transaction with all
        // of them fails; sent on their own, that one is reported as verified and five land.
        let mut sent_on_their_own = 0;
        let sent = mock_verify_transactions(
            &mut transaction_builder,
            &mut mock_client,
            move |signatures| {
                if signatures == 1 {
                    sent_on_their_own += 1;
                }
                if signatures > 1 || sent_on_their_own == 1 {
                    return Err(IncluderClientError::SlotAlreadyVerifiedError(
                        "Error Code: SlotAlreadyVerified".to_string(),
                    ));
                }
                Ok(5_220)
            },
        );

        let includer = create_test_includer(
            mock_client,
            keypair,
            chain_name,
            transaction_builder,
            mock_gmp_api,
            redis_conn,
            mock_refunds_model,
        );

        let total_cost = includer
            .verify_signatures(&execute_data_with_signatures(6, MERKLE_PROOF_LEN))
            .await
            .unwrap();

        assert_eq!(*sent.lock().unwrap(), vec![6, 1, 1, 1, 1, 1, 1]);
        assert_eq!(total_cost, 5 * 5_220);
    }

    #[tokio::test]
    async fn test_verify_signatures_falls_back_when_six_signatures_do_not_fit() {
        // Six signatures fit in a v1 transaction up to a 12-level Merkle proof (a set of 4096
        // verifiers). One level more and the transaction is too large to build, so each
        // signature goes in a transaction of its own.
        for (proof_levels, expected_sent) in [(12, vec![6]), (13, vec![1; 6])] {
            let (
                mock_gmp_api,
                keypair,
                chain_name,
                redis_conn,
                mock_refunds_model,
                mut mock_client,
                mut transaction_builder,
            ) = get_includer_fields();

            let sent =
                mock_verify_transactions(&mut transaction_builder, &mut mock_client, |_| Ok(1));

            let includer = create_test_includer(
                mock_client,
                keypair,
                chain_name,
                transaction_builder,
                mock_gmp_api,
                redis_conn,
                mock_refunds_model,
            );

            includer
                .verify_signatures(&execute_data_with_signatures(6, proof_levels * 32))
                .await
                .unwrap();

            assert_eq!(*sent.lock().unwrap(), expected_sent);
        }
    }

    #[tokio::test]
    async fn test_verify_signatures_fails_when_a_signature_cannot_be_verified() {
        let (
            mock_gmp_api,
            keypair,
            chain_name,
            redis_conn,
            mock_refunds_model,
            mut mock_client,
            mut transaction_builder,
        ) = get_includer_fields();

        // Neither the transaction with all the signatures nor the separate ones go through
        mock_verify_transactions(&mut transaction_builder, &mut mock_client, |_| {
            Err(IncluderClientError::GenericError("rejected".to_string()))
        });

        let includer = create_test_includer(
            mock_client,
            keypair,
            chain_name,
            transaction_builder,
            mock_gmp_api,
            redis_conn,
            mock_refunds_model,
        );

        let result = includer
            .verify_signatures(&execute_data_with_signatures(6, MERKLE_PROOF_LEN))
            .await;

        assert!(matches!(result, Err(SolanaIncluderError::GenericError(_))));
    }

    #[tokio::test]
    async fn test_handle_gateway_tx_task_approve_message_fails_on_error() {
        let (
            mock_gmp_api,
            keypair,
            chain_name,
            mut redis_conn,
            mock_refunds_model,
            mut mock_client,
            mut transaction_builder,
        ) = get_includer_fields();

        let payload_merkle_root = [5u8; 32];
        let signing_verifier_set_merkle_root = [6u8; 32];

        let verifier_info = SigningVerifierSetInfo {
            payload_type: PayloadType::ApproveMessages,
            leaf: VerifierSetLeaf {
                nonce: 0,
                quorum: 0,
                signer_pubkey: PublicKey([5; 33]),
                signer_weight: 0,
                position: 0,
                set_size: 1,
                domain_separator: [0; 32],
            },
            merkle_proof: vec![0xEE],
            signature: solana_axelar_std::Signature([5; 65]),
        };

        let msg_id = "fail-msg".to_string();

        let merkle_msg = MerklizedMessage {
            leaf: MessageLeaf {
                message: Message {
                    cc_id: CrossChainId {
                        chain: "test-chain".to_string(),
                        id: msg_id.clone(),
                    },
                    source_address: "test-source-address".to_string(),
                    destination_chain: "test-destination-chain".to_string(),
                    destination_address: "test-destination-address".to_string(),
                    payload_hash: [51; 32],
                },
                position: 0,
                set_size: 1,
                domain_separator: [0; 32],
            },
            proof: vec![0x05],
        };

        let execute_data = ExecuteData {
            payload_merkle_root,
            signing_verifier_set_merkle_root,
            signing_verifier_set_leaves: vec![verifier_info],
            payload_items: MerklizedPayload::NewMessages {
                messages: vec![merkle_msg],
            },
        };

        let execute_data_b64 =
            base64::prelude::BASE64_STANDARD.encode(borsh::to_vec(&execute_data).unwrap());

        let task = GatewayTxTask {
            common: CommonTaskFields {
                id: "approve-message-fails".into(),
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

        let init_tx_clone = init_tx.clone();
        let verify_tx_clone = verify_tx.clone();
        let approve_tx_clone = approve_tx.clone();

        // 3 builds: init, verify, approve
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
                    2 => Ok((
                        crate::transaction_type::SolanaTransactionType::Legacy(
                            approve_tx_clone.clone(),
                        ),
                        100_000u64,
                    )),
                    _ => panic!("unexpected build call"),
                }
            });

        // 3 sends: init (success), verify (success), approve (fails)
        let send_calls = Arc::new(AtomicUsize::new(0));
        let send_calls_clone = Arc::clone(&send_calls);
        mock_client
            .expect_send_transaction()
            .times(3)
            .returning(move |_, _| {
                let idx = send_calls_clone.fetch_add(1, Ordering::SeqCst);
                match idx {
                    0 => Box::pin(async move { Ok((init_sig, Some(10u64))) }),
                    1 => Box::pin(async move { Ok((verify_sig, Some(20u64))) }),
                    2 => Box::pin(async move {
                        Err(IncluderClientError::GenericError(
                            "approve failed".to_string(),
                        ))
                    }),
                    _ => panic!("unexpected send_transaction call"),
                }
            });

        // add_gas_cost_for_task_id is called twice:
        // 1. init cost: 10
        // 2. total verify cost: 20
        let add_calls = Arc::new(AtomicUsize::new(0));
        let add_calls_clone = Arc::clone(&add_calls);
        redis_conn
            .expect_add_gas_cost_for_task_id()
            .times(2)
            .withf(move |task_id, cost, tx_type| {
                let idx = add_calls_clone.fetch_add(1, Ordering::SeqCst);
                *task_id == "approve-message-fails"
                    && matches!(tx_type, TransactionType::Approve)
                    && match idx {
                        0 => *cost == 10, // init cost
                        1 => *cost == 20, // total verify cost
                        _ => false,
                    }
            })
            .returning(|_, _, _| ());

        // No get_gas_cost_for_task_id because approve_messages errors out
        redis_conn.expect_get_gas_cost_for_task_id().times(0);

        // No write_gas_cost_for_message_id because approve_messages errors out
        redis_conn.expect_write_gas_cost_for_message_id().times(0);

        let includer = create_test_includer(
            mock_client,
            keypair,
            chain_name,
            transaction_builder,
            mock_gmp_api,
            redis_conn,
            mock_refunds_model,
        );

        let result = includer.handle_gateway_tx_task(task).await;

        // Now errors out when any approve fails
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_handle_gateway_tx_task_account_in_use_error_init_session() {
        // Test that when AccountInUseError occurs in InitializePayloadVerificationSession,
        // add_gas_cost_for_task_id is called with 0, which should return early and not write to Redis
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
            payload_type: PayloadType::ApproveMessages,
            leaf: VerifierSetLeaf {
                nonce: 0,
                quorum: 0,
                signer_pubkey: PublicKey([0; 33]),
                signer_weight: 0,
                position: 0,
                set_size: 0,
                domain_separator: [0; 32],
            },
            merkle_proof: vec![0xDD, 0xEE, 0xFF],
            signature: solana_axelar_std::Signature([0; 65]),
        };

        let execute_data = ExecuteData {
            payload_merkle_root,
            signing_verifier_set_merkle_root,
            signing_verifier_set_leaves: vec![verifier_info],
            payload_items: MerklizedPayload::NewMessages {
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
            base64::prelude::BASE64_STANDARD.encode(borsh::to_vec(&execute_data).unwrap());

        let task = GatewayTxTask {
            common: CommonTaskFields {
                id: "account-in-use-init".into(),
                chain: "test-chain".into(),
                timestamp: Utc::now().to_string(),
                r#type: "gateway_tx".into(),
                meta: None,
            },
            task: GatewayTxTaskFields {
                execute_data: execute_data_b64,
            },
        };

        let mut verify_tx = Transaction::new_with_payer(&[], Some(&keypair.pubkey()));
        verify_tx.sign(&[&keypair], Hash::default());
        let verify_sig = verify_tx.signatures[0];

        let mut approve_tx = Transaction::new_with_payer(&[], Some(&keypair.pubkey()));
        approve_tx.sign(&[&keypair], Hash::default());
        let approve_sig = approve_tx.signatures[0];

        let init_tx_clone = Transaction::new_with_payer(&[], Some(&keypair.pubkey()));
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

        // init returns AccountInUseError, verify succeeds, approve succeeds
        let send_calls = Arc::new(AtomicUsize::new(0));
        let send_calls_clone = Arc::clone(&send_calls);
        mock_client
            .expect_send_transaction()
            .times(3)
            .returning(move |_, _| {
                let idx = send_calls_clone.fetch_add(1, Ordering::SeqCst);
                match idx {
                    0 => Box::pin(async move {
                        Err(IncluderClientError::AccountInUseError(
                            "account already in use".to_string(),
                        ))
                    }),
                    1 => Box::pin(async move { Ok((verify_sig, Some(20u64))) }),
                    2 => Box::pin(async move { Ok((approve_sig, Some(30u64))) }),
                    _ => panic!("unexpected send_transaction call"),
                }
            });

        // add_gas_cost_for_task_id is called twice:
        // 1. Called for init with 0 (AccountInUseError) - function returns early, doesn't write to Redis
        // 2. Called for verify with 20
        redis_conn
            .expect_add_gas_cost_for_task_id()
            .times(2)
            .withf(move |task_id, cost, tx_type| {
                *task_id == "account-in-use-init"
                    && matches!(tx_type, TransactionType::Approve)
                    && (*cost == 0 || *cost == 20)
            })
            .returning(|_, _, _| ());

        // get_gas_cost_for_task_id to read overhead (only verify cost since init was 0)
        redis_conn
            .expect_get_gas_cost_for_task_id()
            .times(1)
            .withf(move |task_id, tx_type| {
                *task_id == "account-in-use-init" && matches!(tx_type, TransactionType::Approve)
            })
            .returning(|_, _| Ok(20u64));

        // write_gas_cost_for_message_id with overhead (20) + approve cost (30) = 50
        redis_conn
            .expect_write_gas_cost_for_message_id()
            .times(1)
            .withf(move |msg_id, cost, tx_type| {
                *msg_id == "test-message-id"
                    && matches!(tx_type, TransactionType::Approve)
                    && *cost == 50
            })
            .returning(|_, _, _| ());

        let includer = create_test_includer(
            mock_client,
            keypair,
            chain_name,
            transaction_builder,
            mock_gmp_api,
            redis_conn,
            mock_refunds_model,
        );

        let result = includer.handle_gateway_tx_task(task).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_handle_gateway_tx_task_slot_already_verified_error() {
        // Test that when SlotAlreadyVerifiedError occurs in VerifySignatures,
        // if all signatures are already verified, total_cost is 0,
        // and add_gas_cost_for_task_id is called with 0, which should return early
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
            payload_type: PayloadType::ApproveMessages,
            leaf: VerifierSetLeaf {
                nonce: 0,
                quorum: 0,
                signer_pubkey: PublicKey([0; 33]),
                signer_weight: 0,
                position: 0,
                set_size: 0,
                domain_separator: [0; 32],
            },
            merkle_proof: vec![0xDD, 0xEE, 0xFF],
            signature: solana_axelar_std::Signature([0; 65]),
        };

        let execute_data = ExecuteData {
            payload_merkle_root,
            signing_verifier_set_merkle_root,
            signing_verifier_set_leaves: vec![verifier_info],
            payload_items: MerklizedPayload::NewMessages {
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
            base64::prelude::BASE64_STANDARD.encode(borsh::to_vec(&execute_data).unwrap());

        let task = GatewayTxTask {
            common: CommonTaskFields {
                id: "slot-already-verified".into(),
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

        let mut approve_tx = Transaction::new_with_payer(&[], Some(&keypair.pubkey()));
        approve_tx.sign(&[&keypair], Hash::default());
        let approve_sig = approve_tx.signatures[0];

        let init_tx_clone = init_tx.clone();
        let verify_tx_clone = Transaction::new_with_payer(&[], Some(&keypair.pubkey()));
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

        // init succeeds, verify returns SlotAlreadyVerifiedError, approve succeeds
        let send_calls = Arc::new(AtomicUsize::new(0));
        let send_calls_clone = Arc::clone(&send_calls);
        mock_client
            .expect_send_transaction()
            .times(3)
            .returning(move |_, _| {
                let idx = send_calls_clone.fetch_add(1, Ordering::SeqCst);
                match idx {
                    0 => Box::pin(async move { Ok((init_sig, Some(10u64))) }),
                    1 => Box::pin(async move {
                        Err(IncluderClientError::SlotAlreadyVerifiedError(
                            "slot already verified".to_string(),
                        ))
                    }),
                    2 => Box::pin(async move { Ok((approve_sig, Some(30u64))) }),
                    _ => panic!("unexpected send_transaction call"),
                }
            });

        // add_gas_cost_for_task_id is called twice:
        // 1. Called for init with 10
        // 2. Called for verify with 0 (SlotAlreadyVerifiedError) - function returns early, doesn't write to Redis
        redis_conn
            .expect_add_gas_cost_for_task_id()
            .times(2)
            .withf(move |task_id, cost, tx_type| {
                *task_id == "slot-already-verified"
                    && matches!(tx_type, TransactionType::Approve)
                    && (*cost == 10 || *cost == 0)
            })
            .returning(|_, _, _| ());

        // get_gas_cost_for_task_id to read overhead (only init cost since verify was 0)
        redis_conn
            .expect_get_gas_cost_for_task_id()
            .times(1)
            .withf(move |task_id, tx_type| {
                *task_id == "slot-already-verified" && matches!(tx_type, TransactionType::Approve)
            })
            .returning(|_, _| Ok(10u64));

        // write_gas_cost_for_message_id with overhead (10) + approve cost (30) = 40
        redis_conn
            .expect_write_gas_cost_for_message_id()
            .times(1)
            .withf(move |msg_id, cost, tx_type| {
                *msg_id == "test-message-id"
                    && matches!(tx_type, TransactionType::Approve)
                    && *cost == 40
            })
            .returning(|_, _, _| ());

        let includer = create_test_includer(
            mock_client,
            keypair,
            chain_name,
            transaction_builder,
            mock_gmp_api,
            redis_conn,
            mock_refunds_model,
        );

        let result = includer.handle_gateway_tx_task(task).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_handle_gateway_tx_task_account_in_use_error_approve() {
        // Test that when AccountInUseError occurs in ApproveMessages,
        // the message is added with cost 0, and if overhead is also 0,
        // write_gas_cost_for_message_id is called with 0, which should return early
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
            payload_type: PayloadType::ApproveMessages,
            leaf: VerifierSetLeaf {
                nonce: 0,
                quorum: 0,
                signer_pubkey: PublicKey([0; 33]),
                signer_weight: 0,
                position: 0,
                set_size: 0,
                domain_separator: [0; 32],
            },
            merkle_proof: vec![0xDD, 0xEE, 0xFF],
            signature: solana_axelar_std::Signature([0; 65]),
        };

        let execute_data = ExecuteData {
            payload_merkle_root,
            signing_verifier_set_merkle_root,
            signing_verifier_set_leaves: vec![verifier_info],
            payload_items: MerklizedPayload::NewMessages {
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
            base64::prelude::BASE64_STANDARD.encode(borsh::to_vec(&execute_data).unwrap());

        let task = GatewayTxTask {
            common: CommonTaskFields {
                id: "account-in-use-approve".into(),
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

        let init_tx_clone = init_tx.clone();
        let verify_tx_clone = verify_tx.clone();
        let approve_tx_clone = Transaction::new_with_payer(&[], Some(&keypair.pubkey()));

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

        // init succeeds, verify succeeds, approve returns AccountInUseError
        let send_calls = Arc::new(AtomicUsize::new(0));
        let send_calls_clone = Arc::clone(&send_calls);
        mock_client
            .expect_send_transaction()
            .times(3)
            .returning(move |_, _| {
                let idx = send_calls_clone.fetch_add(1, Ordering::SeqCst);
                match idx {
                    0 => Box::pin(async move { Ok((init_sig, Some(10u64))) }),
                    1 => Box::pin(async move { Ok((verify_sig, Some(20u64))) }),
                    2 => Box::pin(async move {
                        Err(IncluderClientError::AccountInUseError(
                            "account already in use".to_string(),
                        ))
                    }),
                    _ => panic!("unexpected send_transaction call"),
                }
            });

        // add_gas_cost_for_task_id should be called:
        // 1. Called for init (10)
        // 2. Called for verify (20)
        redis_conn
            .expect_add_gas_cost_for_task_id()
            .times(2)
            .withf(move |task_id, cost, tx_type| {
                *task_id == "account-in-use-approve"
                    && matches!(tx_type, TransactionType::Approve)
                    && (*cost == 10 || *cost == 20)
            })
            .returning(|_, _, _| ());

        // get_gas_cost_for_task_id to read overhead (10 + 20 = 30)
        redis_conn
            .expect_get_gas_cost_for_task_id()
            .times(1)
            .withf(move |task_id, tx_type| {
                *task_id == "account-in-use-approve" && matches!(tx_type, TransactionType::Approve)
            })
            .returning(|_, _| Ok(30u64));

        // write_gas_cost_for_message_id with overhead (30) + approve cost (0) = 30
        // Since the total is not 0, it should write
        redis_conn
            .expect_write_gas_cost_for_message_id()
            .times(1)
            .withf(move |msg_id, cost, tx_type| {
                *msg_id == "test-message-id"
                    && matches!(tx_type, TransactionType::Approve)
                    && *cost == 30
            })
            .returning(|_, _, _| ());

        let includer = create_test_includer(
            mock_client,
            keypair,
            chain_name,
            transaction_builder,
            mock_gmp_api,
            redis_conn,
            mock_refunds_model,
        );

        let result = includer.handle_gateway_tx_task(task).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_handle_gateway_tx_task_partial_message_approval_failure() {
        // Test that when some messages fail to approve but others succeed,
        // the task returns an error because not all messages were approved
        let (
            mock_gmp_api,
            keypair,
            chain_name,
            mut redis_conn,
            mock_refunds_model,
            mut mock_client,
            mut transaction_builder,
        ) = get_includer_fields();

        let payload_merkle_root = [10u8; 32];
        let signing_verifier_set_merkle_root = [11u8; 32];

        let verifier_info = SigningVerifierSetInfo {
            payload_type: PayloadType::ApproveMessages,
            leaf: VerifierSetLeaf {
                nonce: 0,
                quorum: 0,
                signer_pubkey: PublicKey([10; 33]),
                signer_weight: 0,
                position: 0,
                set_size: 1,
                domain_separator: [0; 32],
            },
            merkle_proof: vec![0xAA],
            signature: solana_axelar_std::Signature([10; 65]),
        };

        // Two messages: one will succeed, one will fail
        let msg1 = MerklizedMessage {
            leaf: MessageLeaf {
                message: Message {
                    cc_id: CrossChainId {
                        chain: "test-chain".to_string(),
                        id: "msg-success".to_string(),
                    },
                    source_address: "test-source".to_string(),
                    destination_chain: "test-dest".to_string(),
                    destination_address: "test-dest-addr".to_string(),
                    payload_hash: [100; 32],
                },
                position: 0,
                set_size: 2,
                domain_separator: [0; 32],
            },
            proof: vec![0x10],
        };

        let msg2 = MerklizedMessage {
            leaf: MessageLeaf {
                message: Message {
                    cc_id: CrossChainId {
                        chain: "test-chain".to_string(),
                        id: "msg-fail".to_string(),
                    },
                    source_address: "test-source".to_string(),
                    destination_chain: "test-dest".to_string(),
                    destination_address: "test-dest-addr".to_string(),
                    payload_hash: [101; 32],
                },
                position: 1,
                set_size: 2,
                domain_separator: [0; 32],
            },
            proof: vec![0x11],
        };

        let execute_data = ExecuteData {
            payload_merkle_root,
            signing_verifier_set_merkle_root,
            signing_verifier_set_leaves: vec![verifier_info],
            payload_items: MerklizedPayload::NewMessages {
                messages: vec![msg1, msg2],
            },
        };

        let execute_data_b64 =
            base64::prelude::BASE64_STANDARD.encode(borsh::to_vec(&execute_data).unwrap());

        let task = GatewayTxTask {
            common: CommonTaskFields {
                id: "partial-approval-failure".into(),
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

        let mut approve_tx1 = Transaction::new_with_payer(&[], Some(&keypair.pubkey()));
        approve_tx1.sign(&[&keypair], Hash::default());
        let approve_sig1 = approve_tx1.signatures[0];

        let mut approve_tx2 = Transaction::new_with_payer(&[], Some(&keypair.pubkey()));
        approve_tx2.sign(&[&keypair], Hash::default());

        let init_tx_clone = init_tx.clone();
        let verify_tx_clone = verify_tx.clone();
        let approve_tx1_clone = approve_tx1.clone();
        let approve_tx2_clone = approve_tx2.clone();

        // 4 builds: init, verify, approve1, approve2
        let build_calls = Arc::new(AtomicUsize::new(0));
        let build_calls_clone = Arc::clone(&build_calls);
        transaction_builder
            .expect_build()
            .times(4)
            .returning(move |_, _| {
                let idx = build_calls_clone.fetch_add(1, Ordering::SeqCst);
                match idx {
                    0 => Ok((
                        SolanaTransactionType::Legacy(init_tx_clone.clone()),
                        100_000,
                    )),
                    1 => Ok((
                        SolanaTransactionType::Legacy(verify_tx_clone.clone()),
                        100_000,
                    )),
                    2 => Ok((
                        SolanaTransactionType::Legacy(approve_tx1_clone.clone()),
                        100_000,
                    )),
                    3 => Ok((
                        SolanaTransactionType::Legacy(approve_tx2_clone.clone()),
                        100_000,
                    )),
                    _ => panic!("unexpected build call"),
                }
            });

        // 4 sends: init (success), verify (success), approve1 (success), approve2 (fails with generic error)
        let send_calls = Arc::new(AtomicUsize::new(0));
        let send_calls_clone = Arc::clone(&send_calls);
        mock_client
            .expect_send_transaction()
            .times(4)
            .returning(move |_, _| {
                let idx = send_calls_clone.fetch_add(1, Ordering::SeqCst);
                match idx {
                    0 => Box::pin(async move { Ok((init_sig, Some(10u64))) }),
                    1 => Box::pin(async move { Ok((verify_sig, Some(20u64))) }),
                    2 => Box::pin(async move { Ok((approve_sig1, Some(30u64))) }),
                    3 => Box::pin(async move {
                        Err(IncluderClientError::GenericError(
                            "approve2 failed".to_string(),
                        ))
                    }),
                    _ => panic!("unexpected send_transaction call"),
                }
            });

        redis_conn
            .expect_add_gas_cost_for_task_id()
            .times(2)
            .returning(|_, _, _| ());

        // Even though we error, we first process approved_messages (which has 1 item)
        redis_conn
            .expect_get_gas_cost_for_task_id()
            .times(1)
            .returning(|_, _| Ok(30u64)); // init (10) + verify (20) = 30

        redis_conn
            .expect_write_gas_cost_for_message_id()
            .times(1)
            .returning(|_, _, _| ());

        let includer = create_test_includer(
            mock_client,
            keypair,
            chain_name,
            transaction_builder,
            mock_gmp_api,
            redis_conn,
            mock_refunds_model,
        );

        let result = includer.handle_gateway_tx_task(task).await;

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Failed to approve all messages"),
            "Expected error about partial approval, got: {}",
            err_msg
        );
    }

    #[tokio::test]
    async fn test_handle_execute_task_message_already_executed() {
        // Test that when incoming_message_already_executed returns true,
        // the task returns early with empty events
        let (
            mock_gmp_api,
            keypair,
            chain_name,
            redis_conn,
            mock_refunds_model,
            mut mock_client,
            transaction_builder,
        ) = get_includer_fields();

        let message_id = "already-executed-msg-001".to_string();
        let source_chain = "ethereum".to_string();
        let destination_address = solana_axelar_its::ID.to_string();
        let payload_hash = BASE64_STANDARD.encode([20u8; 32]);

        mock_client
            .expect_incoming_message_already_executed()
            .times(1)
            .returning(|_| Box::pin(async move { Ok(true) }));

        let includer = create_test_includer(
            mock_client,
            keypair,
            chain_name,
            transaction_builder,
            mock_gmp_api,
            redis_conn,
            mock_refunds_model,
        );

        let result = includer
            .handle_execute_task(ExecuteTask {
                common: CommonTaskFields {
                    id: "test-execute-already-executed-001".to_string(),
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
                    payload: BASE64_STANDARD.encode(b"test-payload"),
                    available_gas_balance: Amount {
                        amount: "1000000".to_string(),
                        token_id: None,
                    },
                },
            })
            .await;

        assert!(result.is_ok());
        let events = result.unwrap();
        assert!(
            events.is_empty(),
            "Expected empty events for already executed message, got: {:?}",
            events
        );
    }

    #[cfg(not(feature = "devnet-amplifier"))]
    #[tokio::test]
    async fn test_handle_execute_task_negative_gas_balance() {
        // Test that when available_gas_balance contains a negative value,
        // the transaction is considered to have insufficient gas (since any cost > negative balance)
        let (
            mut mock_gmp_api,
            keypair,
            chain_name,
            redis_conn,
            mock_refunds_model,
            mut mock_client,
            mut transaction_builder,
        ) = get_includer_fields();

        let message_id = "negative-gas-msg-001".to_string();
        let source_chain = "ethereum".to_string();
        let destination_address = solana_axelar_governance::ID.to_string();
        let payload_hash = BASE64_STANDARD.encode([30u8; 32]);

        mock_client
            .expect_incoming_message_already_executed()
            .times(1)
            .returning(|_| Box::pin(async move { Ok(false) }));

        // Build the instruction
        let exec_ix = Instruction::new_with_bytes(
            solana_axelar_governance::ID,
            &[1, 2, 3, 4],
            vec![AccountMeta::new(keypair.pubkey(), true)],
        );

        transaction_builder
            .expect_build_execute_instruction()
            .times(1)
            .returning(move |_, _, _| Ok((exec_ix.clone(), ExecuteEntrypoint::Other)));

        // Build transaction - will be called and return estimated cost
        let mut test_tx = Transaction::new_with_payer(&[], Some(&keypair.pubkey()));
        test_tx.sign(&[&keypair], Hash::default());
        let test_tx_clone = test_tx.clone();
        transaction_builder
            .expect_build()
            .times(1)
            .returning(move |_, _| {
                Ok((SolanaTransactionType::Legacy(test_tx_clone.clone()), 100u64))
            });

        // Expect cannot_execute_message to be called since -1000 < 100 (insufficient gas)
        let message_id_clone = message_id.clone();
        let source_chain_clone = source_chain.clone();
        mock_gmp_api
            .expect_cannot_execute_message()
            .withf(move |_task_id, msg_id, src_chain, details, reason| {
                *msg_id == message_id_clone
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
                details: "Not enough gas".to_string(),
            });

        let includer = create_test_includer(
            mock_client,
            keypair,
            chain_name,
            transaction_builder,
            mock_gmp_api,
            redis_conn,
            mock_refunds_model,
        );

        let result = includer
            .handle_execute_task(ExecuteTask {
                common: CommonTaskFields {
                    id: "test-execute-negative-gas-001".to_string(),
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
                    payload: BASE64_STANDARD.encode(b"test-payload"),
                    available_gas_balance: Amount {
                        amount: "-1000".to_string(), // Negative value - always insufficient
                        token_id: None,
                    },
                },
            })
            .await;

        // Should succeed with CannotExecuteMessageV2 event since -1000 < any positive estimated cost
        assert!(result.is_ok());
        let events = result.unwrap();
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], Event::CannotExecuteMessageV2 { .. }));
    }

    #[tokio::test]
    async fn test_handle_execute_task_invalid_gas_balance_format() {
        // Test that when available_gas_balance contains an invalid (non-numeric) value,
        // the task returns a parse error
        let (
            mock_gmp_api,
            keypair,
            chain_name,
            redis_conn,
            mock_refunds_model,
            mut mock_client,
            mut transaction_builder,
        ) = get_includer_fields();

        let message_id = "invalid-gas-msg-001".to_string();
        let destination_address = solana_axelar_governance::ID.to_string();
        let payload_hash = BASE64_STANDARD.encode([31u8; 32]);

        mock_client
            .expect_incoming_message_already_executed()
            .times(1)
            .returning(|_| Box::pin(async move { Ok(false) }));

        let exec_ix = Instruction::new_with_bytes(
            solana_axelar_governance::ID,
            &[1, 2, 3, 4],
            vec![AccountMeta::new(keypair.pubkey(), true)],
        );

        transaction_builder
            .expect_build_execute_instruction()
            .times(1)
            .returning(move |_, _, _| Ok((exec_ix.clone(), ExecuteEntrypoint::Other)));

        let includer = create_test_includer(
            mock_client,
            keypair,
            chain_name,
            transaction_builder,
            mock_gmp_api,
            redis_conn,
            mock_refunds_model,
        );

        let result = includer
            .handle_execute_task(ExecuteTask {
                common: CommonTaskFields {
                    id: "test-execute-invalid-gas-001".to_string(),
                    chain: "test-chain".to_string(),
                    timestamp: Utc::now().to_string(),
                    r#type: "execute".to_string(),
                    meta: None,
                },
                task: ExecuteTaskFields {
                    message: GatewayV2Message {
                        message_id: message_id.clone(),
                        source_chain: "ethereum".to_string(),
                        destination_address: destination_address.clone(),
                        payload_hash: payload_hash.clone(),
                        source_address: Pubkey::new_unique().to_string(),
                    },
                    payload: BASE64_STANDARD.encode(b"test-payload"),
                    available_gas_balance: Amount {
                        amount: "not_a_number".to_string(), // Invalid format
                        token_id: None,
                    },
                },
            })
            .await;

        // Should fail with parse error
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("invalid digit") || err_msg.contains("parse"),
            "Expected parse error, got: {}",
            err_msg
        );
    }

    #[tokio::test]
    async fn test_handle_execute_task_reverted_transaction_reports_cost() {
        let (
            mut mock_gmp_api,
            keypair,
            chain_name,
            mut redis_conn,
            mock_refunds_model,
            mut mock_client,
            mut transaction_builder,
        ) = get_includer_fields();

        let message_id = "revert-cost-001".to_string();
        let estimated_cost = 12_000u64;
        let reverted_cost = 11_000u64;

        mock_client
            .expect_incoming_message_already_executed()
            .times(1)
            .returning(|_| Box::pin(async { Ok(false) }));

        let exec_ix = Instruction::new_with_bytes(solana_axelar_its::ID, &[1, 2, 3], vec![]);
        transaction_builder
            .expect_build_execute_instruction()
            .times(1)
            .returning(move |_, _, _| Ok((exec_ix.clone(), ExecuteEntrypoint::Other)));

        let (test_tx, test_signature) = create_signed_tx(&keypair);
        transaction_builder
            .expect_build()
            .times(1)
            .returning(move |_, _| {
                Ok((
                    SolanaTransactionType::Legacy(test_tx.clone()),
                    estimated_cost,
                ))
            });

        mock_client
            .expect_send_transaction()
            .times(1)
            .returning(|_, _| {
                Box::pin(async {
                    Err(IncluderClientError::UnrecoverableTransactionError(
                        TransactionError::InstructionError(
                            0,
                            solana_sdk::instruction::InstructionError::Custom(42),
                        ),
                    ))
                })
            });

        mock_client
            .expect_get_transaction_cost_from_signature()
            .times(1)
            .withf(move |signature| *signature == test_signature)
            .returning(move |_| Box::pin(async move { Ok(Some(reverted_cost)) }));

        // A reverted execute records no cost in Redis; the cost is reported in the event
        redis_conn.expect_write_gas_cost_for_message_id().times(0);

        let message_id_for_event = message_id.clone();
        mock_gmp_api
            .expect_execute_message()
            .times(1)
            .withf(move |id, _source_chain, status, cost| {
                *id == message_id_for_event
                    && matches!(status, MessageExecutionStatus::REVERTED)
                    && cost.amount == reverted_cost.to_string()
            })
            .returning(
                |message_id, source_chain, status, cost| Event::MessageExecuted {
                    common: CommonEventFields {
                        r#type: "MESSAGE_EXECUTED/V2".to_string(),
                        event_id: "test-event".to_string(),
                        meta: None,
                    },
                    message_id,
                    source_chain,
                    status,
                    cost,
                },
            );

        let includer = create_test_includer(
            mock_client,
            keypair,
            chain_name,
            transaction_builder,
            mock_gmp_api,
            redis_conn,
            mock_refunds_model,
        );

        let task = create_execute_task(
            "test-revert-cost-001",
            &message_id,
            "ethereum",
            &solana_axelar_its::ID.to_string(),
            50_000,
        );
        let events = includer.handle_execute_task(task).await.unwrap();

        let [Event::MessageExecuted { status, cost, .. }] = events.as_slice() else {
            panic!("expected one MessageExecuted event, got: {events:?}");
        };
        assert!(matches!(status, MessageExecutionStatus::REVERTED));
        assert_eq!(cost.amount, reverted_cost.to_string());
    }
}
