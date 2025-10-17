// /*!

// Broadcaster implementation for Solana. Listens to GATEWAY_TX (essentially APPROVE messages) and REFUND.

// # Note

// Relayer code assumes there is one message per transaction. This might not be a safe assumption,
// and broadcaster should potentially be returning a vector of BroadcastResults.

// */
// use crate::config::SolanaConfig;
// use crate::includer_client::IncluderClientTrait;
// use crate::transaction_builder::TransactionBuilderTrait;
// use crate::utils::get_event_authority_pda;
// use crate::utils::get_incoming_message_pda;
// use crate::utils::get_signature_verification_pda;
// use crate::utils::get_verifier_set_tracker_pda;
// use crate::v2_program_types::ExecuteData;
// use crate::v2_program_types::MerkleisedPayload;
// use anchor_lang::InstructionData;
// use anchor_lang::ToAccountMetas;
// use async_trait::async_trait;
// use axelar_solana_gateway_v2::CrossChainId;
// use axelar_solana_gateway_v2::Message;
// use base64::engine::general_purpose;
// use base64::Engine;
// use borsh::BorshDeserialize;
// use futures::stream::FuturesUnordered;
// use futures::StreamExt as _;
// use relayer_core::gmp_api::gmp_types::{ExecuteTaskFields, RefundTaskFields};
// use relayer_core::utils::ThreadSafe;
// use relayer_core::{
//     error::BroadcasterError,
//     includer::{BroadcastResult, Broadcaster},
// };
// use solana_sdk::instruction::Instruction;
// use solana_sdk::pubkey::Pubkey;
// use solana_sdk::signer::keypair::Keypair;
// use solana_sdk::signer::Signer;
// use std::str::FromStr;
// use std::sync::Arc;
// use tracing::{debug, error, info, warn};

// #[derive(Clone)]
// pub struct SolanaBroadcaster<TB, IC>
// where
//     TB: TransactionBuilderTrait + ThreadSafe,
//     IC: IncluderClientTrait + ThreadSafe,
// {
//     client: Arc<IC>,
//     keypair: Arc<Keypair>,
//     gateway_address: Pubkey,
//     gas_service_address: Pubkey,
//     chain_name: String,
//     transaction_builder: TB,
//     max_retries: usize,
//     config: SolanaConfig,
// }

// impl<TB: TransactionBuilderTrait + ThreadSafe, IC: IncluderClientTrait + ThreadSafe>
//     SolanaBroadcaster<TB, IC>
// {
//     pub fn new(
//         client: Arc<IC>,
//         keypair: Arc<Keypair>,
//         gateway_address: Pubkey,
//         gas_service_address: Pubkey,
//         chain_name: String,
//         transaction_builder: TB,
//         max_retries: usize,
//         config: SolanaConfig,
//     ) -> anyhow::Result<Self, BroadcasterError> {
//         Ok(Self {
//             client: Arc::clone(&client),
//             keypair: Arc::clone(&keypair),
//             gateway_address,
//             gas_service_address,
//             chain_name,
//             transaction_builder,
//             max_retries,
//             config,
//         })
//     }

//     async fn send_to_chain(&self, ix: Instruction) -> Result<String, BroadcasterError> {
//         let mut retries: usize = 0;
//         loop {
//             let tx = self
//                 .transaction_builder
//                 .build(ix.clone())
//                 .await
//                 .map_err(|e| BroadcasterError::GenericError(e.to_string()))?;
//             let res = self.client.send_transaction(tx).await;
//             match res {
//                 Ok(signature) => {
//                     let tx_hash = signature.to_string();
//                     debug!("Transaction sent successfully: {}", tx_hash);
//                     match self.client.get_signature_status(&signature).await {
//                         Ok(()) => {
//                             debug!("Transaction confirmed successfully: {}", tx_hash);
//                             return Ok(tx_hash);
//                         }
//                         Err(e) => {
//                             retries += 1;
//                             if retries >= self.max_retries {
//                                 return Err(BroadcasterError::GenericError(format!(
//                                     "Transaction status check failed after {} retries: {}",
//                                     self.max_retries, e
//                                 )));
//                             }
//                             warn!(
//                                 "Transaction status check failed, retrying ({}/{}): {}",
//                                 retries, self.max_retries, e
//                             );
//                         }
//                     }
//                 }
//                 Err(e) => {
//                     retries += 1;
//                     if retries >= self.max_retries {
//                         return Err(BroadcasterError::GenericError(e.to_string()));
//                     }
//                     warn!(
//                         "Transaction failed, retrying ({}/{}): {}",
//                         retries, self.max_retries, e
//                     );
//                 }
//             }
//         }
//     }
// }

// #[derive(Clone)]
// pub struct SolanaTransaction;

// #[async_trait]
// impl<TB: TransactionBuilderTrait + ThreadSafe, IC: IncluderClientTrait + ThreadSafe> Broadcaster
//     for SolanaBroadcaster<TB, IC>
// {
//     type Transaction = SolanaTransaction;

//     #[tracing::instrument(skip(self), fields(message_id))]
//     async fn broadcast_prover_message(
//         &self,
//         tx_blob: String,
//     ) -> Result<Vec<BroadcastResult<Self::Transaction>>, BroadcasterError> {
//         let mut broadcast_results = vec![];

//         let execute_data_bytes = tx_blob.into_bytes();
//         let execute_data = ExecuteData::try_from_slice(&execute_data_bytes).map_err(|_err| {
//             BroadcasterError::GenericError("cannot decode execute data".to_string())
//         })?;

//         let (verification_session_tracker_pda, _) =
//             get_signature_verification_pda(&execute_data.payload_merkle_root);

//         let ix_data = axelar_solana_gateway_v2::instruction::InitializePayloadVerificationSession {
//             merkle_root: execute_data.payload_merkle_root,
//         }
//         .data();

//         let accounts = axelar_solana_gateway_v2::accounts::InitializePayloadVerificationSession {
//             payer: self.keypair.pubkey(),
//             gateway_root_pda: self.gateway_address,
//             verification_session_account: verification_session_tracker_pda,
//             system_program: solana_program::system_program::id(),
//         };

//         let ix = Instruction {
//             program_id: axelar_solana_gateway_v2::ID,
//             accounts: accounts.to_account_metas(None),
//             data: ix_data,
//         };

//         let (message_id, source_chain) = match &execute_data.payload_items {
//             crate::v2_program_types::MerkleisedPayload::NewMessages { messages } => {
//                 if let Some(first_message) = messages.first() {
//                     let chain = first_message.leaf.message.cc_id.chain.clone();
//                     (
//                         Some(first_message.leaf.message.cc_id.id.clone()),
//                         Some(chain),
//                     )
//                 } else {
//                     (None, None)
//                 }
//             }
//             crate::v2_program_types::MerkleisedPayload::VerifierSetRotation { .. } => {
//                 // For verifier set rotation, we don't have message info
//                 (None, None)
//             }
//         };

//         let tx_hash = match self.send_to_chain(ix).await {
//             Ok(tx_hash) => {
//                 broadcast_results.push(BroadcastResult {
//                     transaction: SolanaTransaction,
//                     tx_hash: tx_hash.clone(),
//                     message_id: message_id.clone(),
//                     source_chain: source_chain.clone(),
//                     status: Ok(()),
//                 });
//                 tx_hash
//             }
//             Err(e) => {
//                 // Wrap the error to indicate it's from verification session initialization
//                 let verification_error = BroadcasterError::GenericError(format!(
//                     "Failed to initialize verification session: {}",
//                     e
//                 ));
//                 return Ok(vec![BroadcastResult {
//                     transaction: SolanaTransaction,
//                     tx_hash: String::new(),
//                     message_id,
//                     source_chain,
//                     status: Err(verification_error),
//                 }]);
//             }
//         };

//         debug!(
//             "Transaction for initializing payload verification session successfully: {}",
//             tx_hash
//         );

//         let verifier_set_tracker_pda =
//             get_verifier_set_tracker_pda(execute_data.signing_verifier_set_merkle_root).0;

//         // verify each signature in the signing session
//         let mut verifier_ver_future_set = execute_data
//             .signing_verifier_set_leaves
//             .into_iter()
//             .filter_map(|verifier_info| {
//                 let ix_data = axelar_solana_gateway_v2::instruction::VerifySignature {
//                     payload_merkle_root: execute_data.payload_merkle_root,
//                     verifier_info,
//                 }
//                 .data();

//                 let accounts = axelar_solana_gateway_v2::accounts::VerifySignature {
//                     gateway_root_pda: self.gateway_address,
//                     verification_session_account: verification_session_tracker_pda,
//                     verifier_set_tracker_pda,
//                 };
//                 let ix = Instruction {
//                     program_id: axelar_solana_gateway_v2::ID,
//                     accounts: accounts.to_account_metas(None),
//                     data: ix_data,
//                 };

//                 Some(self.send_to_chain(ix))
//             })
//             .collect::<FuturesUnordered<_>>();
//         while let Some(result) = verifier_ver_future_set.next().await {
//             if let Err(e) = result {
//                 // Wrap the error to indicate it's from signature verification
//                 let sig_verification_error =
//                     BroadcasterError::GenericError(format!("Failed to verify signature: {}", e));
//                 return Ok(vec![BroadcastResult {
//                     transaction: SolanaTransaction,
//                     tx_hash,
//                     message_id,
//                     source_chain,
//                     status: Err(sig_verification_error),
//                 }]);
//             }
//         }

//         let (event_authority, _) = get_event_authority_pda();

//         match execute_data.payload_items {
//             MerkleisedPayload::VerifierSetRotation {
//                 new_verifier_set_merkle_root,
//             } => {
//                 let (new_verifier_set_tracker_pda, _) =
//                     get_verifier_set_tracker_pda(new_verifier_set_merkle_root);
//                 let ix_data = axelar_solana_gateway_v2::instruction::RotateSigners {
//                     new_verifier_set_merkle_root,
//                 }
//                 .data();
//                 let accounts = axelar_solana_gateway_v2::accounts::RotateSigners {
//                     payer: self.keypair.pubkey(),
//                     program: axelar_solana_gateway_v2::ID,
//                     system_program: solana_program::system_program::id(),
//                     gateway_root_pda: self.gateway_address,
//                     verifier_set_tracker_pda,
//                     operator: Some(self.keypair.pubkey()),
//                     new_verifier_set_tracker: new_verifier_set_tracker_pda,
//                     verification_session_account: verification_session_tracker_pda,
//                     event_authority,
//                 };
//                 let ix = Instruction {
//                     program_id: axelar_solana_gateway_v2::ID,
//                     accounts: accounts.to_account_metas(None),
//                     data: ix_data,
//                 };
//                 match self.send_to_chain(ix).await {
//                     Ok(_) => {
//                         broadcast_results.push(BroadcastResult {
//                             transaction: SolanaTransaction,
//                             tx_hash: tx_hash.clone(),
//                             message_id: message_id.clone(),
//                             source_chain: source_chain.clone(),
//                             status: Ok(()),
//                         });
//                         debug!("Rotated signers transaction sent successfully");
//                     }
//                     Err(e) => {
//                         // Wrap the error to indicate it's from rotate signers
//                         let rotate_error = BroadcasterError::GenericError(format!(
//                             "Failed to rotate signers: {}",
//                             e
//                         ));
//                         return Ok(vec![BroadcastResult {
//                             transaction: SolanaTransaction,
//                             tx_hash,
//                             message_id,
//                             source_chain,
//                             status: Err(rotate_error),
//                         }]);
//                     }
//                 }
//             }
//             MerkleisedPayload::NewMessages { messages } => {
//                 let mut message_data = Vec::new();
//                 let mut merkelised_message_futures = messages
//                     .into_iter()
//                     .filter_map(|merkleised_message| {
//                         let command_id = merkleised_message.leaf.message.command_id();
//                         let (pda, _) = get_incoming_message_pda(&command_id);

//                         // Store message metadata for later use
//                         let msg_id = merkleised_message.leaf.message.cc_id.id.clone();
//                         let chain = merkleised_message.leaf.message.cc_id.chain.clone();
//                         message_data.push((msg_id, chain));

//                         let ix_data = axelar_solana_gateway_v2::instruction::ApproveMessage {
//                             merkleised_message,
//                             payload_merkle_root: execute_data.payload_merkle_root,
//                         }
//                         .data();
//                         let accounts = axelar_solana_gateway_v2::accounts::ApproveMessage {
//                             funder: self.keypair.pubkey(),
//                             incoming_message_pda: pda,
//                             program: axelar_solana_gateway_v2::ID,
//                             system_program: solana_program::system_program::id(),
//                             gateway_root_pda: self.gateway_address,
//                             verification_session_account: verification_session_tracker_pda,
//                             event_authority,
//                         };
//                         let ix = Instruction {
//                             program_id: axelar_solana_gateway_v2::ID,
//                             accounts: accounts.to_account_metas(None),
//                             data: ix_data,
//                         };
//                         Some(self.send_to_chain(ix))
//                     })
//                     .collect::<FuturesUnordered<_>>();

//                 let mut idx = 0;
//                 while let Some(result) = merkelised_message_futures.next().await {
//                     let (msg_id, chain) = &message_data[idx];
//                     match result {
//                         Ok(_) => {
//                             broadcast_results.push(BroadcastResult {
//                                 transaction: SolanaTransaction,
//                                 tx_hash: tx_hash.clone(),
//                                 message_id: Some(msg_id.clone()),
//                                 source_chain: Some(chain.clone()),
//                                 status: Ok(()),
//                             });
//                         }
//                         Err(e) => {
//                             // Wrap the error to indicate it's from approve message
//                             let approve_error = BroadcasterError::GenericError(format!(
//                                 "Failed to approve message {}: {}",
//                                 msg_id, e
//                             ));
//                             broadcast_results.push(BroadcastResult {
//                                 transaction: SolanaTransaction,
//                                 tx_hash: tx_hash.clone(),
//                                 message_id: Some(msg_id.clone()),
//                                 source_chain: Some(chain.clone()),
//                                 status: Err(approve_error),
//                             });
//                         }
//                     }
//                     idx += 1;
//                 }
//                 debug!("Approved messages transaction sent successfully");
//             }
//         };

//         Ok(broadcast_results)
//     }

//     #[tracing::instrument(skip(self))]
//     async fn broadcast_refund(&self, _data: String) -> Result<String, BroadcasterError> {
//         Ok(String::new())
//     }

//     #[tracing::instrument(skip(self), fields(message_id))]
//     async fn broadcast_execute_message(
//         &self,
//         message: ExecuteTaskFields,
//     ) -> Result<BroadcastResult<Self::Transaction>, BroadcasterError> {
//         // compose the message
//         let message = Message {
//             cc_id: CrossChainId {
//                 chain: message.message.source_chain,
//                 id: message.message.message_id,
//             },
//             source_address: message.message.source_address,
//             // is this correct?
//             destination_chain: self.config.common_config.chain_name,
//             destination_address: message.message.destination_address,
//             payload_hash: message
//                 .message
//                 .payload_hash
//                 .into_bytes()
//                 .as_slice()
//                 .try_into()
//                 .map_err(|_| {
//                     BroadcasterError::GenericError(
//                         "Failed to convert payload hash to [u8; 32]".to_string(),
//                     )
//                 })?,
//         };
//         let command_id = message.command_id();
//         let (gateway_incoming_message_pda, ..) = get_incoming_message_pda(&command_id);

//         if self
//             .client
//             .incoming_message_already_executed(&gateway_incoming_message_pda)
//             .await?
//         {
//             tracing::warn!("incoming message already executed");
//             return Ok(());
//         }

//         // Parse destination address
//         let destination_address = message
//             .destination_address
//             .parse::<Pubkey>()
//             .context("Failed to parse destination address")?;

//         // Verify destination and communicate with the destination program
//         verify_destination(destination_address, config.allow_third_party_contract_calls)?;

//         gas_estimator
//             .ensure_enough_gas(
//                 solana_rpc_client,
//                 keypair,
//                 metadata.gateway_root_pda,
//                 &message,
//                 &payload,
//                 destination_address,
//                 gateway_incoming_message_pda,
//                 available_gas_balance,
//             )
//             .await?;

//         // Upload the message payload to a Gateway-owned PDA account and get its address back.
//         let gateway_message_payload_pda = message_payload::upload(
//             solana_rpc_client,
//             keypair,
//             metadata.gateway_root_pda,
//             &message,
//             &payload,
//         )
//         .await?;

//         let execute_call_status = send_to_destination_program(
//             destination_address,
//             signer,
//             gateway_incoming_message_pda,
//             gateway_message_payload_pda,
//             metadata.gateway_root_pda,
//             &message,
//             payload,
//             solana_rpc_client,
//             keypair,
//         )
//         .await;

//         let destination_address: Pubkey =
//             message.message.destination_address.parse().map_err(|e| {
//                 BroadcasterError::GenericError(format!("TonAddressParseError: {e:?}"))
//             })?;

//         let decoded_bytes = general_purpose::STANDARD
//             .decode(message.payload.clone())
//             .map_err(|e| {
//                 BroadcasterError::GenericError(format!("Failed decoding payload: {e:?}"))
//             })?;

//         let payload_len = decoded_bytes.len();

//         let hex_payload = hex::encode(decoded_bytes.clone());

//         let message_id = message.message.message_id;
//         let source_chain = message.message.source_chain;

//         tracing::Span::current().record("message_id", &message_id);

//         let available_gas = u64::from_str(&message.available_gas_balance.amount).unwrap_or(0);
//         let required_gas = self.gas_estimator.execute_estimate(payload_len).await;

//         info!(
//             "Considering execute message: message_id={}, source_chain={}, available_gas={}, required_gas={}, payload_len={}",
//             message_id, source_chain, available_gas, required_gas, payload_len
//         );
//         if available_gas < required_gas {
//             return Ok(BroadcastResult {
//                 transaction: SolanaTransaction,
//                 tx_hash: String::new(),
//                 message_id: Some(message_id),
//                 source_chain: Some(source_chain),
//                 status: Err(BroadcasterError::InsufficientGas(
//                     "Cannot proceed to execute".to_string(),
//                 )),
//             });
//         }

//         let result = async {
//             let relayer_execute_msg = RelayerExecuteMessage::new(
//                 message_id.clone(),
//                 source_chain.clone(),
//                 message.message.source_address,
//                 self.chain_name.clone(),
//                 destination_address,
//                 hex_payload,
//                 self.keypair.pubkey(),
//             );

//             let boc = relayer_execute_msg
//                 .to_cell()
//                 .map_err(|e| BroadcasterError::GenericError(e.to_string()))?
//                 .to_boc_hex(true)
//                 .map_err(|e| {
//                     BroadcasterError::GenericError(format!(
//                         "Failed to serialize relayer execute message: {e:?}"
//                     ))
//                 })?;

//             let execute_message_value: BigUint =
//                 BigUint::from(self.gas_estimator.execute_send(payload_len).await);

//             let actions: Vec<OutAction> = vec![out_action(
//                 &boc,
//                 execute_message_value.clone(),
//                 self.gateway_address.clone(),
//             )
//             .map_err(|e| BroadcasterError::GenericError(e.to_string()))?];

//             let res = self.send_to_chain(keypair, actions.clone(), None).await;
//             let (tx_hash, status) = match res {
//                 Ok(response) => (response.message_hash, Ok(())),
//                 Err(err) => (String::new(), Err(err)),
//             };

//             Ok(BroadcastResult {
//                 transaction: SolanaTransaction,
//                 tx_hash,
//                 message_id: Some(message_id.clone()),
//                 source_chain: Some(source_chain.clone()),
//                 status,
//             })
//         }
//         .await;

//         self.keypair.release(keypair).await;

//         result
//     }

//     #[tracing::instrument(skip(self), fields(message_id))]
//     async fn broadcast_refund_message(
//         &self,
//         refund_task: RefundTaskFields,
//     ) -> Result<String, BroadcasterError> {
//         if refund_task.remaining_gas_balance.token_id.is_some() {
//             return Err(BroadcasterError::GenericError(
//                 "Refund task with token_id is not supported".to_string(),
//             ));
//         }

//         let cleaned_hash = refund_task
//             .message
//             .message_id
//             .strip_prefix("0x")
//             .unwrap_or(&refund_task.message.message_id);

//         tracing::Span::current().record("message_id", &refund_task.message.message_id);

//         let tx_hash = TonHash::from_hex(cleaned_hash)
//             .map_err(|e| BroadcasterError::GenericError(e.to_string()))?;

//         let address = Pubkey::from_str(&refund_task.refund_recipient_address)
//             .map_err(|err| BroadcasterError::GenericError(err.to_string()))?;

//         let original_amount = BigUint::from_str(&refund_task.remaining_gas_balance.amount)
//             .map_err(|err| BroadcasterError::GenericError(err.to_string()))?;
//         let gas_estimate = self.gas_estimator.native_gas_refund_estimate().await;

//         info!(
//             "Considering refund message: message_id={}, address={}, original_amount={}, gas_estimate={}",
//             refund_task.message.message_id, address, refund_task.remaining_gas_balance.amount, gas_estimate
//         );

//         if original_amount < BigUint::from(gas_estimate) {
//             info!(
//                 "Not enough balance to cover gas for refund: message_id={}",
//                 refund_task.message.message_id
//             );
//             return Err(BroadcasterError::InsufficientGas(
//                 "Not enough balance to cover gas for refund".to_string(),
//             ));
//         }

//         let amount = original_amount - BigUint::from(gas_estimate);

//         let native_refund = NativeRefundMessage::new(tx_hash, address, amount);

//         let boc = native_refund
//             .to_cell()
//             .map_err(|e| BroadcasterError::GenericError(e.to_string()))?
//             .to_boc_hex(true)
//             .map_err(|e| {
//                 BroadcasterError::GenericError(format!(
//                     "Failed to serialize relayer execute message: {e:?}"
//                 ))
//             })?;

//         let wallet = self.wallet.acquire().await.map_err(|e| {
//             error!("Error acquiring wallet: {e:?}");
//             BroadcasterError::GenericError(format!("Wallet acquire failed: {e:?}"))
//         })?;

//         let result = async {
//             let msg_value: BigUint = BigUint::from(REFUND_DUST);

//             let actions: Vec<OutAction> =
//                 vec![
//                     out_action(&boc, msg_value.clone(), self.gas_service_address.clone())
//                         .map_err(|e| BroadcasterError::GenericError(e.to_string()))?,
//                 ];

//             let res = self.send_to_chain(wallet, actions.clone(), None).await;
//             let (tx_hash, _status) = match res {
//                 Ok(response) => (response.message_hash, Ok(())),
//                 Err(err) => (String::new(), Err(err)),
//             };

//             Ok(tx_hash)
//         }
//         .await;

//         self.wallet.release(wallet).await;

//         result
//     }
// }
