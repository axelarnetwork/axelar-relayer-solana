use crate::gas_calculator::GasCalculatorTrait;
use crate::includer::ALTInfo;
use crate::includer_client::IncluderClientTrait;
use crate::redis::RedisConnectionTrait;
use crate::utils::{
    calculate_total_cost_lamports, create_transaction, extract_proposal_hash_from_payload,
    get_ata_with_program, get_gateway_event_authority_pda, get_governance_event_authority_pda,
    get_its_event_authority_pda, get_minter_roles_pda, get_operator_proposal_pda,
    get_proposal_pda, get_token_mint_pda,
};
use crate::{error::TransactionBuilderError, transaction_type::SolanaTransactionType};
use anchor_lang::AccountDeserialize;
use anchor_lang::InstructionData;
use anchor_lang::ToAccountMetas;
use anchor_spl::{associated_token::spl_associated_token_account, token_2022::spl_token_2022};
use async_trait::async_trait;
use borsh::BorshDeserialize;
use mpl_token_metadata;
use relayer_core::utils::ThreadSafe;
use solana_address_lookup_table_interface::instruction::{
    create_lookup_table, extend_lookup_table,
};
use solana_address_lookup_table_interface::state::AddressLookupTable;
use solana_axelar_gateway::executable::ExecutablePayload;
use solana_axelar_gateway::Message;
use solana_axelar_its::encoding::HubMessage;
use solana_axelar_its::instructions::{
    execute_deploy_interchain_token_extra_accounts, execute_interchain_transfer_extra_accounts,
    execute_link_token_extra_accounts,
};
use solana_sdk::instruction::{AccountMeta, Instruction};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signer::keypair::Keypair;
use solana_sdk::signer::Signer as _;
use std::sync::Arc;
use tracing::{debug, error};

#[derive(Clone)]
pub struct TransactionBuilder<
    GE: GasCalculatorTrait,
    IC: IncluderClientTrait,
    R: RedisConnectionTrait + Clone,
> {
    keypair: Arc<Keypair>,
    gas_calculator: GE,
    includer_client: Arc<IC>,
    redis_conn: R,
}

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait TransactionBuilderTrait<IC: IncluderClientTrait, R: RedisConnectionTrait + Clone>:
    ThreadSafe
{
    async fn build(
        &self,
        ixs: &[Instruction],
        alt_info: Option<ALTInfo>,
        extra_signing_keypairs: Option<Vec<Keypair>>,
    ) -> Result<(SolanaTransactionType, u64), TransactionBuilderError>;

    async fn build_execute_instruction(
        &self,
        message: &Message,
        payload: &[u8],
        destination_address: Pubkey,
    ) -> Result<(Instruction, Vec<AccountMeta>), TransactionBuilderError>;

    async fn build_its_instruction(
        &self,
        message: &Message,
        payload: &[u8],
        incoming_message_pda: Pubkey,
    ) -> Result<(Instruction, Vec<AccountMeta>), TransactionBuilderError>;

    async fn build_governance_instruction(
        &self,
        message: &Message,
        payload: &[u8],
        incoming_message_pda: Pubkey,
    ) -> Result<(Instruction, Vec<AccountMeta>), TransactionBuilderError>;

    async fn build_executable_instruction(
        &self,
        message: &Message,
        payload: &[u8],
        incoming_message_pda: Pubkey,
        destination_address: Pubkey,
    ) -> Result<(Instruction, Vec<AccountMeta>), TransactionBuilderError>;

    async fn build_lookup_table_instructions(
        &self,
        recent_slot: u64,
        execute_accounts: &[AccountMeta],
    ) -> Result<(Instruction, Instruction, Pubkey, String), TransactionBuilderError>;
}

impl<GE: GasCalculatorTrait, IC: IncluderClientTrait, R: RedisConnectionTrait + Clone>
    TransactionBuilder<GE, IC, R>
{
    pub fn new(
        keypair: Arc<Keypair>,
        gas_calculator: GE,
        includer_client: Arc<IC>,
        redis_conn: R,
    ) -> Self {
        Self {
            keypair,
            gas_calculator,
            includer_client,
            redis_conn,
        }
    }

    /// Determines the token program (SPL Token or Token-2022) for a given mint address
    /// by checking the account owner on-chain.
    async fn get_token_program_for_mint(
        &self,
        mint: &Pubkey,
    ) -> Result<Pubkey, TransactionBuilderError> {
        match self.includer_client.get_account_owner(mint).await {
            Ok(owner) => {
                if owner == spl_token_2022::ID {
                    Ok(spl_token_2022::ID)
                } else {
                    Ok(anchor_spl::token::ID)
                }
            }
            Err(_) => Err(TransactionBuilderError::GenericError(
                "Failed to get token program owner".to_string(),
            )),
        }
    }
}

#[async_trait]
impl<GE: GasCalculatorTrait, IC: IncluderClientTrait, R: RedisConnectionTrait + Clone>
    TransactionBuilderTrait<IC, R> for TransactionBuilder<GE, IC, R>
{
    async fn build(
        &self,
        ixs: &[Instruction],
        alt_info: Option<ALTInfo>,
        extra_signing_keypairs: Option<Vec<Keypair>>,
    ) -> Result<(SolanaTransactionType, u64), TransactionBuilderError> {
        let alt_addresses = if let Some(alt_info) = alt_info.clone() {
            let alt_pubkey = alt_info.alt_pubkey.ok_or_else(|| {
                TransactionBuilderError::GenericError("ALTInfo provided without pubkey".to_string())
            })?;

            if let Some(addresses) = alt_info.alt_addresses {
                addresses
            } else {
                // In case ALT exists already (retries): fetch from chain
                debug!("Fetching ALT addresses from chain");
                let alt_account_data = self
                    .includer_client
                    .get_account_data(&alt_pubkey)
                    .await
                    .map_err(|e| TransactionBuilderError::ClientError(e.to_string()))?;
                let alt_state = AddressLookupTable::deserialize(&alt_account_data)
                    .map_err(|e| TransactionBuilderError::GenericError(e.to_string()))?;
                alt_state.addresses.to_vec()
            }
        } else {
            vec![]
        };

        let unit_price = self
            .redis_conn
            .get_cu_price()
            .await
            .map_err(|e| TransactionBuilderError::ClientError(e.to_string()))?;

        let recent_hash = self
            .includer_client
            .get_latest_blockhash()
            .await
            .map_err(|e| TransactionBuilderError::ClientError(e.to_string()))?;

        let mut signing_keypairs: Vec<&Keypair> = vec![&self.keypair];
        if let Some(keypairs) = &extra_signing_keypairs {
            signing_keypairs.extend(keypairs);
        }

        // Proto transaction where the compute budget is set to a high value to ensure the transaction is simulated successfully
        // We will override the compute budget in the final transaction.
        let proto_transaction = create_transaction(
            ixs.to_vec(),
            alt_info.clone(),
            alt_addresses.clone(),
            unit_price.unwrap_or(0),
            500_0000,
            &self.keypair,
            signing_keypairs.clone(),
            recent_hash,
        )
        .await
        .map_err(|e| TransactionBuilderError::CreateTransactionError(e.to_string()))?;

        // Compute the actual compute budget required for the transaction
        let compute_budget = self
            .gas_calculator
            .compute_budget(proto_transaction.clone())
            .await
            .map_err(|e| TransactionBuilderError::ClientError(e.to_string()))?;

        // Create the final transaction with the actual compute budget
        let final_transaction = create_transaction(
            ixs.to_vec(),
            alt_info,
            alt_addresses,
            unit_price.unwrap_or(0),
            compute_budget,
            &self.keypair,
            signing_keypairs,
            recent_hash,
        )
        .await
        .map_err(|e| TransactionBuilderError::CreateTransactionError(e.to_string()))?;

        let final_cost = calculate_total_cost_lamports(&final_transaction, compute_budget)
            .map_err(|e| TransactionBuilderError::GenericError(e.to_string()))?;

        Ok((final_transaction, final_cost))
    }

    async fn build_execute_instruction(
        &self,
        message: &Message,
        payload: &[u8],
        destination_address: Pubkey,
    ) -> Result<(Instruction, Vec<AccountMeta>), TransactionBuilderError> {
        let (incoming_message_pda, _) =
            solana_axelar_gateway::IncomingMessage::try_find_pda(&message.command_id())
                .ok_or_else(|| {
                    TransactionBuilderError::GenericError(
                        "Failed to derive incoming message PDA".to_string(),
                    )
                })?;

        match destination_address {
            x if x == solana_axelar_its::ID => {
                self.build_its_instruction(message, payload, incoming_message_pda)
                    .await
            }
            x if x == solana_axelar_governance::ID => {
                self.build_governance_instruction(message, payload, incoming_message_pda)
                    .await
            }
            _ => {
                self.build_executable_instruction(
                    message,
                    payload,
                    incoming_message_pda,
                    destination_address,
                )
                .await
            }
        }
    }

    async fn build_its_instruction(
        &self,
        message: &Message,
        payload: &[u8],
        incoming_message_pda: Pubkey,
    ) -> Result<(Instruction, Vec<AccountMeta>), TransactionBuilderError> {
        // Use a copy for deserialization to preserve the original payload bytes
        let mut payload_reader = payload;
        let gmp_decoded_payload = HubMessage::deserialize(&mut payload_reader)
            .map_err(|e| TransactionBuilderError::PayloadDecodeError(e.to_string()))?;

        let token_id = match gmp_decoded_payload {
            HubMessage::ReceiveFromHub { ref message, .. } => match message {
                solana_axelar_its::encoding::Message::InterchainTransfer(transfer) => {
                    transfer.token_id
                }
                solana_axelar_its::encoding::Message::DeployInterchainToken(deploy) => {
                    deploy.token_id
                }
                solana_axelar_its::encoding::Message::LinkToken(link) => link.token_id,
            },
            _ => {
                return Err(TransactionBuilderError::PayloadDecodeError(
                    "Unexpected GMP payload type".to_string(),
                ));
            }
        };

        let (signing_pda, _) = solana_axelar_gateway::ValidateMessageSigner::try_find_pda(
            &message.command_id(),
            &solana_axelar_its::ID,
        )
        .ok_or_else(|| {
            TransactionBuilderError::GenericError(
                "Failed to derive validate message signing PDA".to_string(),
            )
        })?;
        let (event_authority, _) = get_gateway_event_authority_pda()
            .map_err(|e| TransactionBuilderError::GenericError(e.to_string()))?;
        let (gateway_root_pda, _) = solana_axelar_gateway::GatewayConfig::try_find_pda()
            .ok_or_else(|| {
                TransactionBuilderError::GenericError(
                    "Failed to derive gateway root config PDA".to_string(),
                )
            })?;

        let executable = solana_axelar_its::accounts::AxelarExecuteAccounts {
            incoming_message_pda,
            signing_pda,
            axelar_gateway_program: solana_axelar_gateway::ID,
            event_authority,
            gateway_root_pda,
        };

        let (its_root_pda, _) = solana_axelar_its::InterchainTokenService::try_find_pda()
            .ok_or_else(|| {
                TransactionBuilderError::GenericError("Failed to derive ITS root PDA".to_string())
            })?;
        let (token_manager_pda, _) = solana_axelar_its::TokenManager::try_find_pda(
            token_id,
            its_root_pda,
        )
        .ok_or_else(|| {
            TransactionBuilderError::GenericError("Failed to derive token manager PDA".to_string())
        })?;

        let (token_mint, token_program) = match &gmp_decoded_payload {
            HubMessage::ReceiveFromHub {
                message: solana_axelar_its::encoding::Message::LinkToken(ref link),
                ..
            } => {
                // For LinkToken, the mint is in the payload
                let mint_pubkey = Pubkey::try_from(link.destination_token_address.as_slice())
                    .map_err(|e| {
                        TransactionBuilderError::PayloadDecodeError(format!(
                            "Invalid destination_token_address: {}",
                            e,
                        ))
                    })?;

                let token_program_id = self.get_token_program_for_mint(&mint_pubkey).await?;

                (mint_pubkey, token_program_id)
            }
            HubMessage::ReceiveFromHub {
                message: solana_axelar_its::encoding::Message::InterchainTransfer(_),
                ..
            } => {
                // For InterchainTransfer, fetch the token manager to get the actual mint.
                // This is necessary because linked/canonical tokens have their mint stored
                // in the token manager, not derived from the ITS root PDA.
                let token_manager_data = self
                    .includer_client
                    .get_account_data(&token_manager_pda)
                    .await
                    .map_err(|e| TransactionBuilderError::GenericError(e.to_string()))?;

                let token_manager =
                    solana_axelar_its::TokenManager::try_deserialize(&mut &token_manager_data[..])
                        .map_err(|e| {
                            TransactionBuilderError::GenericError(format!(
                                "Failed to deserialize TokenManager: {}",
                                e
                            ))
                        })?;

                let mint_pubkey = token_manager.token_address;

                let token_program_id = self.get_token_program_for_mint(&mint_pubkey).await?;

                (mint_pubkey, token_program_id)
            }
            _ => (
                // For DeployInterchainToken and other cases, derive the mint PDA
                get_token_mint_pda(&its_root_pda, &token_id)
                    .map_err(|e| TransactionBuilderError::GenericError(e.to_string()))?
                    .0,
                spl_token_2022::ID,
            ),
        };

        let (token_manager_ata, _) =
            get_ata_with_program(&token_manager_pda, &token_mint, &token_program);

        let mut accounts = solana_axelar_its::accounts::Execute {
            executable,
            payer: self.keypair.pubkey(),
            system_program: solana_sdk_ids::system_program::ID,
            event_authority: get_its_event_authority_pda()
                .map_err(|e| TransactionBuilderError::GenericError(e.to_string()))?
                .0,
            its_root_pda,
            token_manager_pda,
            token_mint,
            token_manager_ata,
            token_program,
            associated_token_program: spl_associated_token_account::program::ID,
            program: solana_axelar_its::ID,
        }
        .to_account_metas(None);

        debug!("GMP decoded payload: {:?}", gmp_decoded_payload);

        match &gmp_decoded_payload {
            HubMessage::ReceiveFromHub { message, .. } => match message {
                solana_axelar_its::encoding::Message::InterchainTransfer(transfer) => {
                    let destination_address =
                        Pubkey::try_from(transfer.destination_address.as_slice()).map_err(|e| {
                            TransactionBuilderError::PayloadDecodeError(e.to_string())
                        })?;
                    let (destination_ata, _) = get_ata_with_program(
                        &destination_address,
                        &token_mint,
                        &token_program,
                    );
                    accounts.extend(execute_interchain_transfer_extra_accounts(
                        destination_address,
                        destination_ata,
                        Some(transfer.data.is_some()),
                    ));
                    if let Some(data) = &transfer.data {
                        match ExecutablePayload::decode(data) {
                            Ok(executable_payload) => {
                                let gmp_accounts = executable_payload.account_meta();
                                for account in gmp_accounts.clone() {
                                    // signers are not supported for arbitrary executables
                                    if account.is_signer {
                                        return Err(TransactionBuilderError::PayloadDecodeError(
                                            "Signer account cannot be provided".to_string(),
                                        ));
                                    };
                                }
                                accounts.extend(gmp_accounts);
                            }
                            Err(e) => {
                                error!("Failed to decode ExecutablePayload: {:?}", e);
                                return Err(TransactionBuilderError::PayloadDecodeError(
                                    e.to_string(),
                                ));
                            }
                        }
                    }
                }
                solana_axelar_its::encoding::Message::DeployInterchainToken(deploy) => {
                    let minter = if let Some(minter) = &deploy.minter {
                        Some(Pubkey::try_from(minter.as_slice()).map_err(|e| {
                            TransactionBuilderError::PayloadDecodeError(format!(
                                "Invalid minter pubkey: {}",
                                e
                            ))
                        })?)
                    } else {
                        None
                    };
                    let minter_roles_pda = minter
                        .map(|minter| {
                            get_minter_roles_pda(&token_manager_pda, &minter)
                                .map_err(|e| TransactionBuilderError::GenericError(e.to_string()))
                                .map(|(pda, _)| pda)
                        })
                        .transpose()?;

                    let (mpl_token_metadata_account, _) =
                        mpl_token_metadata::accounts::Metadata::find_pda(&token_mint);

                    accounts.extend(execute_deploy_interchain_token_extra_accounts(
                        solana_program::sysvar::instructions::ID,
                        mpl_token_metadata::ID,
                        mpl_token_metadata_account,
                        minter,
                        minter_roles_pda,
                    ));
                }
                solana_axelar_its::encoding::Message::LinkToken(link) => {
                    let minter = link
                        .params
                        .as_ref()
                        // Check if we should be erroring here or ignoring the value if invalid
                        .and_then(|p| Pubkey::try_from(p.as_slice()).ok());
                    let minter_roles_pda = minter
                        .map(|minter| {
                            get_minter_roles_pda(&token_manager_pda, &minter)
                                .map_err(|e| TransactionBuilderError::GenericError(e.to_string()))
                                .map(|(pda, _)| pda)
                        })
                        .transpose()?;

                    accounts.extend(execute_link_token_extra_accounts(minter, minter_roles_pda))
                }
            },
            HubMessage::SendToHub { .. } | HubMessage::RegisterTokenMetadata(_) => {
                return Err(TransactionBuilderError::PayloadDecodeError(
                    "Unexpected GMP payload type".to_string(),
                ));
            }
        }

        let data = solana_axelar_its::instruction::Execute {
            message: message.clone(),
            payload: payload.to_vec(),
        }
        .data();

        Ok((
            Instruction {
                program_id: solana_axelar_its::ID,
                accounts: accounts.clone(),
                data,
            },
            accounts,
        ))
    }

    async fn build_governance_instruction(
        &self,
        message: &Message,
        payload: &[u8],
        incoming_message_pda: Pubkey,
    ) -> Result<(Instruction, Vec<AccountMeta>), TransactionBuilderError> {
        let (signing_pda, _) = solana_axelar_gateway::ValidateMessageSigner::try_find_pda(
            &message.command_id(),
            &solana_axelar_governance::ID,
        )
        .ok_or_else(|| {
            TransactionBuilderError::GenericError(
                "Failed to derive validate message signing PDA".to_string(),
            )
        })?;
        let (event_authority, _) = get_gateway_event_authority_pda()
            .map_err(|e| TransactionBuilderError::GenericError(e.to_string()))?;
        let (gateway_root_pda, _) = solana_axelar_gateway::GatewayConfig::try_find_pda()
            .ok_or_else(|| {
                TransactionBuilderError::GenericError(
                    "Failed to derive gateway root config PDA".to_string(),
                )
            })?;
        let executable = solana_axelar_governance::accounts::AxelarExecuteAccounts {
            incoming_message_pda,
            signing_pda,
            axelar_gateway_program: solana_axelar_gateway::ID,
            event_authority,
            gateway_root_pda,
        };

        let (governance_config, _) = solana_axelar_governance::GovernanceConfig::try_find_pda()
            .ok_or_else(|| {
                TransactionBuilderError::GenericError(
                    "Failed to derive governance config PDA".to_string(),
                )
            })?;
        let (governance_event_authority, _) = get_governance_event_authority_pda()
            .map_err(|e| TransactionBuilderError::GenericError(e.to_string()))?;

        let proposal_hash = extract_proposal_hash_from_payload(payload)
            .map_err(|e| TransactionBuilderError::GenericError(e.to_string()))?;
        let (proposal_pda, _) = get_proposal_pda(&proposal_hash)
            .map_err(|e| TransactionBuilderError::GenericError(e.to_string()))?;
        let (operator_proposal_pda, _) = get_operator_proposal_pda(&proposal_hash)
            .map_err(|e| TransactionBuilderError::GenericError(e.to_string()))?;

        let accounts = solana_axelar_governance::accounts::ProcessGmp {
            executable,
            payer: self.keypair.pubkey(),
            governance_config,
            proposal_pda,
            operator_proposal_pda,
            governance_event_authority,
            axelar_governance_program: solana_axelar_governance::ID,
            system_program: solana_sdk_ids::system_program::ID,
        }
        .to_account_metas(None);

        let data = solana_axelar_governance::instruction::ProcessGmp {
            message: message.clone(),
            payload: payload.to_vec(),
        }
        .data();

        Ok((
            Instruction {
                program_id: solana_axelar_governance::ID,
                accounts,
                data,
            },
            vec![], // we do not want to create an ALT for governance instructions
        ))
    }

    async fn build_executable_instruction(
        &self,
        message: &Message,
        payload: &[u8],
        incoming_message_pda: Pubkey,
        destination_address: Pubkey,
    ) -> Result<(Instruction, Vec<AccountMeta>), TransactionBuilderError> {
        let decoded_payload = ExecutablePayload::decode(payload)
            .map_err(|e| TransactionBuilderError::PayloadDecodeError(e.to_string()))?; // return custom error to send cannot_execute_message

        // signers are not supported for arbitrary executables
        let user_provided_accounts = decoded_payload.account_meta();
        for account in user_provided_accounts.clone() {
            if account.is_signer {
                return Err(TransactionBuilderError::PayloadDecodeError(
                    "Signer account cannot be provided".to_string(),
                ));
            };
        }

        let (signing_pda, _) = solana_axelar_gateway::ValidateMessageSigner::try_find_pda(
            &message.command_id(),
            &destination_address,
        )
        .ok_or_else(|| {
            TransactionBuilderError::GenericError(
                "Failed to derive validate message signing PDA".to_string(),
            )
        })?;
        let (event_authority, _) = get_gateway_event_authority_pda()
            .map_err(|e| TransactionBuilderError::GenericError(e.to_string()))?;
        let (gateway_root_pda, _) = solana_axelar_gateway::GatewayConfig::try_find_pda()
            .ok_or_else(|| {
                TransactionBuilderError::GenericError(
                    "Failed to derive gateway root config PDA".to_string(),
                )
            })?;

        let mut accounts = solana_axelar_gateway::executable::helpers::AxelarExecuteAccounts {
            incoming_message_pda,
            signing_pda,
            axelar_gateway_program: solana_axelar_gateway::ID,
            event_authority,
            gateway_root_pda,
        }
        .to_account_metas(None);

        accounts.extend(user_provided_accounts);

        let data = solana_axelar_gateway::executable::helpers::AxelarExecuteInstruction {
            message: message.clone(),
            payload_without_accounts: decoded_payload.payload_without_accounts().to_vec(),
            encoding_scheme: decoded_payload.encoding_scheme(),
        }
        .data();

        Ok((
            Instruction {
                program_id: destination_address,
                accounts,
                data,
            },
            vec![], // we do not want to create an ALT for executable instructions
        ))
    }

    async fn build_lookup_table_instructions(
        &self,
        recent_slot: u64,
        execute_accounts: &[AccountMeta],
    ) -> Result<(Instruction, Instruction, Pubkey, String), TransactionBuilderError> {
        // create a new keypair as the authority to avoid conflicts with concurrent ALT creations using the same recent hash
        let authority_keypair = Keypair::new();
        let (ix_alt_create, alt_pubkey) = create_lookup_table(
            authority_keypair.pubkey(),
            self.keypair.pubkey(),
            recent_slot,
        );

        let alt_accounts = execute_accounts.iter().map(|acc| acc.pubkey).collect();

        let ix_alt_extend = extend_lookup_table(
            alt_pubkey,
            authority_keypair.pubkey(),
            Some(self.keypair.pubkey()),
            alt_accounts,
        );

        let authority_keypair_str = authority_keypair.to_base58_string();
        Ok((
            ix_alt_create,
            ix_alt_extend,
            alt_pubkey,
            authority_keypair_str,
        ))
    }
}

#[cfg(test)]
mod tests {
    use crate::error::TransactionBuilderError;
    use crate::gas_calculator::MockGasCalculatorTrait;
    use crate::includer::ALTInfo;
    use crate::includer_client::MockIncluderClientTrait;
    use crate::redis::MockRedisConnectionTrait;
    use crate::transaction_builder::{TransactionBuilder, TransactionBuilderTrait};
    use crate::transaction_type::SolanaTransactionType;
    use alloy_sol_types::SolValue;
    use anchor_lang::prelude::AccountMeta;
    use anchor_lang::AccountSerialize;

    use solana_axelar_gateway::executable::ExecutablePayload;
    use solana_axelar_gateway::payload::EncodingScheme;
    use solana_axelar_governance;
    use solana_axelar_governance::ExecuteProposalCallData;
    use solana_axelar_its;
    use solana_axelar_its::encoding::{HubMessage, InterchainTransfer, Message as ItsMessage};
    use solana_axelar_std::{CrossChainId, Message, U256};
    use solana_sdk::hash::Hash;
    use solana_sdk::instruction::Instruction;
    use solana_sdk::message::VersionedMessage;
    use solana_sdk::pubkey::Pubkey;
    use solana_sdk::signature::Keypair;
    use solana_sdk::signer::Signer;
    use std::sync::Arc;

    /// Helper function to create mock TokenManager account data for tests
    fn create_mock_token_manager_data(token_id: [u8; 32], token_address: Pubkey) -> Vec<u8> {
        use solana_axelar_its::{state::FlowState, TokenManager};

        let token_manager = TokenManager {
            ty: solana_axelar_its::state::Type::NativeInterchainToken,
            token_id,
            token_address,
            associated_token_account: Pubkey::new_unique(),
            flow_slot: FlowState {
                flow_limit: None,
                flow_in: 0,
                flow_out: 0,
                epoch: 0,
            },
            bump: 0,
        };

        let mut data = Vec::new();
        token_manager
            .try_serialize(&mut data)
            .expect("Failed to serialize TokenManager");
        data
    }

    #[tokio::test]
    async fn test_transaction_builder_build_with_alt_produces_versioned_tx() {
        let keypair = Arc::new(Keypair::new());
        let mut mock_gas = MockGasCalculatorTrait::new();
        let mut mock_client = MockIncluderClientTrait::new();
        let mut mock_redis = MockRedisConnectionTrait::new();

        let alt_pubkey = Pubkey::new_unique();
        let alt_account_1 = Pubkey::new_unique();
        let alt_account_2 = Pubkey::new_unique();
        let alt_addresses = vec![alt_account_1, alt_account_2];

        let user_program = Pubkey::new_unique();
        let user_ix = Instruction::new_with_bytes(
            user_program,
            &[1, 2, 3],
            vec![
                AccountMeta::new(keypair.pubkey(), true),
                AccountMeta::new_readonly(alt_account_1, false),
            ],
        );

        let recent_blockhash = Hash::new_unique();

        mock_redis
            .expect_get_cu_price()
            .times(1)
            .returning(move || Ok(Some(100_000u64)));

        mock_gas
            .expect_compute_budget()
            .times(1)
            .returning(|_| Ok(100_000u64));

        mock_client
            .expect_get_latest_blockhash()
            .times(1)
            .returning(move || {
                let hash = recent_blockhash;
                Box::pin(async move { Ok(hash) })
            });

        let alt_info = ALTInfo::new(Some(alt_pubkey)).with_addresses(alt_addresses);

        let builder = TransactionBuilder::new(
            Arc::clone(&keypair),
            mock_gas,
            Arc::new(mock_client),
            mock_redis,
        );

        let (tx, _cost) = builder
            .build(std::slice::from_ref(&user_ix), Some(alt_info), None)
            .await
            .expect("build with ALT should succeed");

        match tx {
            SolanaTransactionType::Versioned(versioned_tx) => match versioned_tx.message {
                VersionedMessage::V0(msg) => {
                    assert_eq!(msg.instructions.len(), 3);

                    assert_eq!(msg.address_table_lookups.len(), 1);
                    assert_eq!(msg.address_table_lookups[0].account_key, alt_pubkey);

                    assert_eq!(versioned_tx.signatures.len(), 1);
                }
                _ => panic!("expected v0 message for ALT-backed build"),
            },
            _ => panic!("expected Versioned transaction when ALTInfo is provided"),
        }
    }

    #[tokio::test]
    async fn test_transaction_builder_build_without_alt_produces_legacy_tx() {
        let keypair = Arc::new(Keypair::new());
        let mut mock_gas = MockGasCalculatorTrait::new();
        let mut mock_client = MockIncluderClientTrait::new();
        let mut mock_redis = MockRedisConnectionTrait::new();

        let user_program = Pubkey::new_unique();
        let user_ix = Instruction::new_with_bytes(
            user_program,
            &[4, 5, 6],
            vec![AccountMeta::new(keypair.pubkey(), true)],
        );

        let recent_blockhash = Hash::new_unique();

        mock_redis
            .expect_get_cu_price()
            .times(1)
            .returning(move || Ok(Some(100_000u64)));

        mock_gas
            .expect_compute_budget()
            .times(1)
            .returning(|_| Ok(100_000u64));

        mock_client
            .expect_get_latest_blockhash()
            .times(1)
            .returning(move || {
                let hash = recent_blockhash;
                Box::pin(async move { Ok(hash) })
            });

        let builder = TransactionBuilder::new(
            Arc::clone(&keypair),
            mock_gas,
            Arc::new(mock_client),
            mock_redis,
        );

        let (tx, _cost) = builder
            .build(std::slice::from_ref(&user_ix), None, None)
            .await
            .expect("build without ALT should succeed");

        match tx {
            SolanaTransactionType::Legacy(legacy_tx) => {
                assert_eq!(legacy_tx.message.instructions.len(), 3);
                assert_eq!(legacy_tx.message.account_keys[0], keypair.pubkey());
                assert_eq!(legacy_tx.message.recent_blockhash, recent_blockhash);
                assert_eq!(legacy_tx.signatures.len(), 1);
            }
            _ => panic!("expected Legacy transaction when no ALTInfo is provided"),
        }
    }

    #[tokio::test]
    async fn test_transaction_builder_build_with_extra_signing_keypairs() {
        let keypair = Arc::new(Keypair::new());
        let mut mock_gas = MockGasCalculatorTrait::new();
        let mut mock_client = MockIncluderClientTrait::new();
        let mut mock_redis = MockRedisConnectionTrait::new();

        let extra_keypair1 = Keypair::new();
        let extra_keypair2 = Keypair::new();
        let extra_keypair1_pubkey = extra_keypair1.pubkey();
        let extra_keypair2_pubkey = extra_keypair2.pubkey();
        let extra_signing_keypairs = vec![extra_keypair1, extra_keypair2];

        let user_program = Pubkey::new_unique();
        let user_ix = Instruction::new_with_bytes(
            user_program,
            &[7, 8, 9],
            vec![
                AccountMeta::new(keypair.pubkey(), true),
                AccountMeta::new(extra_keypair1_pubkey, true),
                AccountMeta::new(extra_keypair2_pubkey, true),
            ],
        );

        let recent_blockhash = Hash::new_unique();

        mock_redis
            .expect_get_cu_price()
            .times(1)
            .returning(move || Ok(Some(100_000u64)));

        mock_gas
            .expect_compute_budget()
            .times(1)
            .returning(|_| Ok(100_000u64));

        mock_client
            .expect_get_latest_blockhash()
            .times(1)
            .returning(move || {
                let hash = recent_blockhash;
                Box::pin(async move { Ok(hash) })
            });

        let builder = TransactionBuilder::new(
            Arc::clone(&keypair),
            mock_gas,
            Arc::new(mock_client),
            mock_redis,
        );

        let (tx, _cost) = builder
            .build(
                std::slice::from_ref(&user_ix),
                None,
                Some(extra_signing_keypairs),
            )
            .await
            .expect("build with extra signing keypairs should succeed");

        match tx {
            SolanaTransactionType::Legacy(legacy_tx) => {
                assert_eq!(legacy_tx.message.instructions.len(), 3);
                assert_eq!(legacy_tx.message.account_keys[0], keypair.pubkey());
                assert_eq!(legacy_tx.message.recent_blockhash, recent_blockhash);
                // Should have 3 signatures: main keypair + 2 extra keypairs
                assert_eq!(legacy_tx.signatures.len(), 3);
            }
            _ => panic!("expected Legacy transaction when no ALTInfo is provided"),
        }
    }

    #[tokio::test]
    async fn test_build_execute_instruction_with_three_addresses() {
        use anchor_spl::token_2022::spl_token_2022;

        let keypair = Arc::new(Keypair::new());
        let mock_gas = MockGasCalculatorTrait::new();
        let mut mock_client = MockIncluderClientTrait::new();
        let mock_redis = MockRedisConnectionTrait::new();

        let token_id = [1u8; 32];

        // For InterchainTransfer, we need to mock the token manager account fetch
        // First compute the token manager PDA
        let (its_root_pda, _) = solana_axelar_its::InterchainTokenService::try_find_pda()
            .expect("Failed to derive ITS root PDA");
        let (token_manager_pda, _) =
            solana_axelar_its::TokenManager::try_find_pda(token_id, its_root_pda)
                .expect("Failed to derive token manager PDA");

        // Create a mock token mint (using derived PDA for native ITS tokens)
        let (token_mint_pda, _) =
            solana_axelar_its::TokenManager::find_token_mint(token_id, its_root_pda);

        // Create mock token manager data
        let mock_token_manager_data = create_mock_token_manager_data(token_id, token_mint_pda);

        // Set up mock expectations
        mock_client
            .expect_get_account_data()
            .withf(move |pubkey| *pubkey == token_manager_pda)
            .returning(move |_| {
                let data = mock_token_manager_data.clone();
                Box::pin(async move { Ok(data) })
            });

        // Mock get_account_owner for the token mint (return Token-2022 for native ITS tokens)
        mock_client
            .expect_get_account_owner()
            .withf(move |pubkey| *pubkey == token_mint_pda)
            .returning(move |_| Box::pin(async move { Ok(spl_token_2022::ID) }));

        let message = Message {
            cc_id: CrossChainId {
                chain: "ethereum".to_string(),
                id: "test-message-id-123".to_string(),
            },
            source_address: "0x1234567890123456789012345678901234567890".to_string(),
            destination_chain: "solana".to_string(),
            destination_address: "test-destination".to_string(),
            payload_hash: [0u8; 32],
        };

        let builder = TransactionBuilder::new(
            Arc::clone(&keypair),
            mock_gas,
            Arc::new(mock_client),
            mock_redis,
        );

        let its_destination = solana_axelar_its::ID;

        let destination_pubkey = Pubkey::new_unique();
        let destination_address_bytes = destination_pubkey.to_bytes().to_vec();

        // Create InterchainTransfer message
        let interchain_transfer = InterchainTransfer {
            token_id,
            source_address: vec![3u8; 20],
            destination_address: destination_address_bytes,
            amount: 0u64,
            data: None,
        };

        let its_message = ItsMessage::InterchainTransfer(interchain_transfer);

        let hub_message = HubMessage::ReceiveFromHub {
            source_chain: "ethereum".to_string(),
            message: its_message,
        };

        // Serialize using borsh
        let its_payload = borsh::to_vec(&hub_message).expect("Failed to serialize HubMessage");

        let (its_instruction, its_accounts) = builder
            .build_execute_instruction(&message, &its_payload, its_destination)
            .await
            .expect("ITS build_execute_instruction should succeed");

        assert_eq!(its_instruction.program_id, solana_axelar_its::ID);
        // ITS should return non-empty accounts for ALT creation
        assert!(
            !its_accounts.is_empty(),
            "ITS should return accounts for ALT creation"
        );

        // Governance address should return empty accounts (no ALT)
        let governance_destination = solana_axelar_governance::ID;

        // Create a valid governance payload
        let call_data = ExecuteProposalCallData {
            solana_accounts: vec![],
            solana_native_value_receiver_account: None,
            call_data: vec![1, 2, 3], // Simple instruction data
        };
        let target_bytes: [u8; 32] = Pubkey::new_unique().to_bytes();
        let gmp_payload = governance_gmp::GovernanceCommandPayload {
            command: governance_gmp::GovernanceCommand::ScheduleTimeLockProposal,
            target: target_bytes.to_vec().into(),
            call_data: borsh::to_vec(&call_data).unwrap().into(),
            native_value: U256::from(0u64).into(),
            eta: U256::from(0u64).into(),
        };
        let governance_payload = gmp_payload.abi_encode();

        let (governance_instruction, governance_accounts) = builder
            .build_execute_instruction(&message, &governance_payload, governance_destination)
            .await
            .expect("Governance build_execute_instruction should succeed");

        assert_eq!(
            governance_instruction.program_id,
            solana_axelar_governance::ID
        );
        assert!(
            governance_accounts.is_empty(),
            "Governance should return empty accounts (no ALT)"
        );

        let arbitrary_destination = Pubkey::new_unique();

        let executable_payload = ExecutablePayload::new::<AccountMeta>(
            &[1, 2, 3, 4, 5],
            &[AccountMeta::new(arbitrary_destination, false)],
            EncodingScheme::AbiEncoding,
        );
        let executable_payload_bytes = executable_payload.encode().unwrap();

        let (arbitrary_instruction, arbitrary_accounts) = builder
            .build_execute_instruction(&message, &executable_payload_bytes, arbitrary_destination)
            .await
            .expect("Arbitrary program build_execute_instruction should succeed");

        assert_eq!(arbitrary_instruction.program_id, arbitrary_destination);
        assert!(
            arbitrary_accounts.is_empty(),
            "Arbitrary program should return empty accounts (no ALT)"
        );
    }

    #[tokio::test]
    async fn test_build_its_instruction_interchain_transfer_with_executable_payload() {
        use anchor_spl::token_2022::spl_token_2022;

        let keypair = Arc::new(Keypair::new());
        let mock_gas = MockGasCalculatorTrait::new();
        let mut mock_client = MockIncluderClientTrait::new();
        let mock_redis = MockRedisConnectionTrait::new();

        let token_id = [2u8; 32];

        // Compute PDAs for mocking
        let (its_root_pda, _) = solana_axelar_its::InterchainTokenService::try_find_pda()
            .expect("Failed to derive ITS root PDA");
        let (token_manager_pda, _) =
            solana_axelar_its::TokenManager::try_find_pda(token_id, its_root_pda)
                .expect("Failed to derive token manager PDA");
        let (token_mint_pda, _) =
            solana_axelar_its::TokenManager::find_token_mint(token_id, its_root_pda);

        // Create mock token manager data
        let mock_token_manager_data = create_mock_token_manager_data(token_id, token_mint_pda);

        // Set up mock expectations
        mock_client
            .expect_get_account_data()
            .withf(move |pubkey| *pubkey == token_manager_pda)
            .returning(move |_| {
                let data = mock_token_manager_data.clone();
                Box::pin(async move { Ok(data) })
            });

        mock_client
            .expect_get_account_owner()
            .withf(move |pubkey| *pubkey == token_mint_pda)
            .returning(move |_| Box::pin(async move { Ok(spl_token_2022::ID) }));

        let message = Message {
            cc_id: CrossChainId {
                chain: "ethereum".to_string(),
                id: "test-message-id-executable-payload".to_string(),
            },
            source_address: "0x1234567890123456789012345678901234567890".to_string(),
            destination_chain: "solana".to_string(),
            destination_address: "test-destination".to_string(),
            payload_hash: [0u8; 32],
        };

        let builder = TransactionBuilder::new(
            Arc::clone(&keypair),
            mock_gas,
            Arc::new(mock_client),
            mock_redis,
        );

        let its_destination = solana_axelar_its::ID;
        let destination_pubkey = Pubkey::new_unique();
        let destination_address_bytes = destination_pubkey.to_bytes().to_vec();

        let gmp_account1 = Pubkey::new_unique();
        let gmp_account2 = Pubkey::new_unique();
        let executable_payload = ExecutablePayload::new::<AccountMeta>(
            &[10, 20, 30, 40], // payload data
            &[
                AccountMeta::new(gmp_account1, false),
                AccountMeta::new_readonly(gmp_account2, false),
            ],
            EncodingScheme::Borsh,
        );
        let executable_payload_bytes = executable_payload.encode().unwrap();

        // Create InterchainTransfer with ExecutablePayload in data field
        let interchain_transfer = InterchainTransfer {
            token_id,
            source_address: vec![4u8; 20],
            destination_address: destination_address_bytes,
            amount: 1000u64,
            data: Some(executable_payload_bytes),
        };

        // Wrap in ITS Message enum
        let its_message = ItsMessage::InterchainTransfer(interchain_transfer);

        // Wrap in HubMessage::ReceiveFromHub
        let hub_message = HubMessage::ReceiveFromHub {
            source_chain: "ethereum".to_string(),
            message: its_message,
        };

        // Serialize using borsh
        let its_payload = borsh::to_vec(&hub_message).expect("Failed to serialize HubMessage");

        let (its_instruction, its_accounts) = builder
            .build_execute_instruction(&message, &its_payload, its_destination)
            .await
            .expect("ITS build_execute_instruction with ExecutablePayload should succeed");

        assert_eq!(its_instruction.program_id, solana_axelar_its::ID);

        let instruction_accounts = &its_instruction.accounts;

        let account_pubkeys: Vec<Pubkey> =
            instruction_accounts.iter().map(|acc| acc.pubkey).collect();
        assert!(
            account_pubkeys.contains(&gmp_account1),
            "gmp_account1 should be in the instruction accounts"
        );
        assert!(
            account_pubkeys.contains(&gmp_account2),
            "gmp_account2 should be in the instruction accounts"
        );

        // Verify accounts are returned for ALT creation
        assert!(
            !its_accounts.is_empty(),
            "ITS should return accounts for ALT"
        );
    }

    #[tokio::test]
    async fn test_build_its_instruction_interchain_transfer_with_malformed_data() {
        use anchor_spl::token_2022::spl_token_2022;

        let keypair = Arc::new(Keypair::new());
        let mock_gas = MockGasCalculatorTrait::new();
        let mut mock_client = MockIncluderClientTrait::new();
        let mock_redis = MockRedisConnectionTrait::new();

        let token_id = [3u8; 32];

        // Compute PDAs for mocking
        let (its_root_pda, _) = solana_axelar_its::InterchainTokenService::try_find_pda()
            .expect("Failed to derive ITS root PDA");
        let (token_manager_pda, _) =
            solana_axelar_its::TokenManager::try_find_pda(token_id, its_root_pda)
                .expect("Failed to derive token manager PDA");
        let (token_mint_pda, _) =
            solana_axelar_its::TokenManager::find_token_mint(token_id, its_root_pda);

        // Create mock token manager data
        let mock_token_manager_data = create_mock_token_manager_data(token_id, token_mint_pda);

        // Set up mock expectations
        mock_client
            .expect_get_account_data()
            .withf(move |pubkey| *pubkey == token_manager_pda)
            .returning(move |_| {
                let data = mock_token_manager_data.clone();
                Box::pin(async move { Ok(data) })
            });

        mock_client
            .expect_get_account_owner()
            .withf(move |pubkey| *pubkey == token_mint_pda)
            .returning(move |_| Box::pin(async move { Ok(spl_token_2022::ID) }));

        let message = Message {
            cc_id: CrossChainId {
                chain: "ethereum".to_string(),
                id: "test-message-id-malformed".to_string(),
            },
            source_address: "0x1234567890123456789012345678901234567890".to_string(),
            destination_chain: "solana".to_string(),
            destination_address: "test-destination".to_string(),
            payload_hash: [0u8; 32],
        };

        let builder = TransactionBuilder::new(
            Arc::clone(&keypair),
            mock_gas,
            Arc::new(mock_client),
            mock_redis,
        );

        let its_destination = solana_axelar_its::ID;
        let destination_pubkey = Pubkey::new_unique();
        let destination_address_bytes = destination_pubkey.to_bytes().to_vec();

        // Create InterchainTransfer with malformed/random bytes in data field
        let malformed_data = vec![0xFF, 0xEE, 0xDD, 0xCC, 0xBB, 0xAA]; // Random bytes that won't decode as ExecutablePayload
        let interchain_transfer = InterchainTransfer {
            token_id,
            source_address: vec![5u8; 20],
            destination_address: destination_address_bytes,
            amount: 2000u64,
            data: Some(malformed_data),
        };

        // Wrap in ITS Message enum
        let its_message = ItsMessage::InterchainTransfer(interchain_transfer);

        // Wrap in HubMessage::ReceiveFromHub
        let hub_message = HubMessage::ReceiveFromHub {
            source_chain: "ethereum".to_string(),
            message: its_message,
        };

        // Serialize using borsh
        let its_payload = borsh::to_vec(&hub_message).expect("Failed to serialize HubMessage");

        // This should fail with PayloadDecodeError because the data field contains malformed bytes
        let result = builder
            .build_execute_instruction(&message, &its_payload, its_destination)
            .await;

        assert!(
            result.is_err(),
            "build_execute_instruction should fail with malformed ExecutablePayload"
        );

        match result {
            Err(TransactionBuilderError::PayloadDecodeError(_)) => {
                // Expected error type
            }
            Err(e) => panic!(
                "Expected PayloadDecodeError, but got different error: {:?}",
                e
            ),
            Ok(_) => panic!("Expected error but got success"),
        }
    }

    #[tokio::test]
    async fn test_build_its_instruction_interchain_transfer_rejects_signer_accounts() {
        use anchor_spl::token_2022::spl_token_2022;

        let keypair = Arc::new(Keypair::new());
        let mock_gas = MockGasCalculatorTrait::new();
        let mut mock_client = MockIncluderClientTrait::new();
        let mock_redis = MockRedisConnectionTrait::new();

        let token_id = [3u8; 32];

        // Compute PDAs for mocking
        let (its_root_pda, _) = solana_axelar_its::InterchainTokenService::try_find_pda()
            .expect("Failed to derive ITS root PDA");
        let (token_manager_pda, _) =
            solana_axelar_its::TokenManager::try_find_pda(token_id, its_root_pda)
                .expect("Failed to derive token manager PDA");
        let (token_mint_pda, _) =
            solana_axelar_its::TokenManager::find_token_mint(token_id, its_root_pda);

        // Create mock token manager data
        let mock_token_manager_data = create_mock_token_manager_data(token_id, token_mint_pda);

        // Set up mock expectations
        mock_client
            .expect_get_account_data()
            .withf(move |pubkey| *pubkey == token_manager_pda)
            .returning(move |_| {
                let data = mock_token_manager_data.clone();
                Box::pin(async move { Ok(data) })
            });

        mock_client
            .expect_get_account_owner()
            .withf(move |pubkey| *pubkey == token_mint_pda)
            .returning(move |_| Box::pin(async move { Ok(spl_token_2022::ID) }));

        let message = Message {
            cc_id: CrossChainId {
                chain: "ethereum".to_string(),
                id: "test-message-id-reject-signers".to_string(),
            },
            source_address: "0x1234567890123456789012345678901234567890".to_string(),
            destination_chain: "solana".to_string(),
            destination_address: "test-destination".to_string(),
            payload_hash: [0u8; 32],
        };

        let builder = TransactionBuilder::new(
            Arc::clone(&keypair),
            mock_gas,
            Arc::new(mock_client),
            mock_redis,
        );

        let its_destination = solana_axelar_its::ID;
        let destination_pubkey = Pubkey::new_unique();
        let destination_address_bytes = destination_pubkey.to_bytes().to_vec();

        // Create an ExecutablePayload with accounts that have is_signer: true
        let gmp_account1 = Pubkey::new_unique();
        let gmp_account2 = Pubkey::new_unique();
        let executable_payload = ExecutablePayload::new::<AccountMeta>(
            &[10, 20, 30, 40], // payload data
            &[
                AccountMeta::new(gmp_account1, true), // is_signer: true - should be rejected
                AccountMeta::new_readonly(gmp_account2, true), // is_signer: true, readonly - should be rejected
            ],
            EncodingScheme::Borsh,
        );
        let executable_payload_bytes = executable_payload.encode().unwrap();

        // Create InterchainTransfer with ExecutablePayload in data field
        let interchain_transfer = InterchainTransfer {
            token_id,
            source_address: vec![5u8; 20],
            destination_address: destination_address_bytes,
            amount: 1000u64,
            data: Some(executable_payload_bytes),
        };

        // Wrap in ITS Message enum
        let its_message = ItsMessage::InterchainTransfer(interchain_transfer);

        // Wrap in HubMessage::ReceiveFromHub
        let hub_message = HubMessage::ReceiveFromHub {
            source_chain: "ethereum".to_string(),
            message: its_message,
        };

        // Serialize using borsh
        let its_payload = borsh::to_vec(&hub_message).expect("Failed to serialize HubMessage");

        // Should fail with PayloadDecodeError when signer accounts are detected
        let result = builder
            .build_execute_instruction(&message, &its_payload, its_destination)
            .await;

        assert!(
            result.is_err(),
            "build_execute_instruction should fail when signer accounts are provided"
        );

        match result {
            Err(TransactionBuilderError::PayloadDecodeError(msg)) => {
                assert!(
                    msg.contains("Signer account cannot be provided"),
                    "Error message should indicate signer accounts are not allowed"
                );
            }
            Err(e) => panic!(
                "Expected PayloadDecodeError, but got different error: {:?}",
                e
            ),
            Ok(_) => panic!("Expected error but got success"),
        }
    }

    #[tokio::test]
    async fn test_build_executable_instruction_rejects_signer_accounts() {
        let keypair = Arc::new(Keypair::new());
        let mock_gas = MockGasCalculatorTrait::new();
        let mock_client = MockIncluderClientTrait::new();
        let mock_redis = MockRedisConnectionTrait::new();

        let message = Message {
            cc_id: CrossChainId {
                chain: "ethereum".to_string(),
                id: "test-message-id-executable-reject-signers".to_string(),
            },
            source_address: "0x1234567890123456789012345678901234567890".to_string(),
            destination_chain: "solana".to_string(),
            destination_address: "test-destination".to_string(),
            payload_hash: [0u8; 32],
        };

        let builder = TransactionBuilder::new(
            Arc::clone(&keypair),
            mock_gas,
            Arc::new(mock_client),
            mock_redis,
        );

        let destination_program = Pubkey::new_unique();

        // Create an ExecutablePayload with accounts that have is_signer: true
        let user_account1 = Pubkey::new_unique();
        let user_account2 = Pubkey::new_unique();
        let executable_payload = ExecutablePayload::new::<AccountMeta>(
            &[10, 20, 30, 40, 50], // payload data
            &[
                AccountMeta::new(user_account1, true), // is_signer: true - should be rejected
                AccountMeta::new_readonly(user_account2, true), // is_signer: true, readonly - should be rejected
            ],
            EncodingScheme::Borsh,
        );
        let executable_payload_bytes = executable_payload.encode().unwrap();

        // Should fail with PayloadDecodeError when signer accounts are detected
        let result = builder
            .build_executable_instruction(
                &message,
                &executable_payload_bytes,
                Pubkey::new_unique(),
                destination_program,
            )
            .await;

        assert!(
            result.is_err(),
            "build_executable_instruction should fail when signer accounts are provided"
        );

        match result {
            Err(TransactionBuilderError::PayloadDecodeError(msg)) => {
                assert!(
                    msg.contains("Signer account cannot be provided"),
                    "Error message should indicate signer accounts are not allowed"
                );
            }
            Err(e) => panic!(
                "Expected PayloadDecodeError, but got different error: {:?}",
                e
            ),
            Ok(_) => panic!("Expected error but got success"),
        }
    }

    /// Test that interchain transfer with a linked/canonical token using regular SPL Token
    /// (not Token-2022) properly derives the destination ATA with the correct token program.
    /// This is the key bug fix test - previously the code always used Token-2022 for ATA derivation.
    #[tokio::test]
    async fn test_build_its_instruction_interchain_transfer_with_linked_spl_token() {
        use crate::utils::get_ata_with_program;
        use anchor_spl::token::spl_token;

        let keypair = Arc::new(Keypair::new());
        let mock_gas = MockGasCalculatorTrait::new();
        let mut mock_client = MockIncluderClientTrait::new();
        let mock_redis = MockRedisConnectionTrait::new();

        let token_id = [0xAB; 32]; // Token ID for a linked/canonical token

        // Compute PDAs
        let (its_root_pda, _) = solana_axelar_its::InterchainTokenService::try_find_pda()
            .expect("Failed to derive ITS root PDA");
        let (token_manager_pda, _) =
            solana_axelar_its::TokenManager::try_find_pda(token_id, its_root_pda)
                .expect("Failed to derive token manager PDA");

        // For a linked token, the mint is NOT derived from the ITS root PDA.
        // It's an existing SPL token that was linked via LinkToken instruction.
        // Create a unique pubkey to simulate a linked token mint.
        let linked_token_mint = Pubkey::new_unique();

        // Create mock token manager data with the LINKED token mint (not the derived PDA)
        let mock_token_manager_data = create_mock_token_manager_data(token_id, linked_token_mint);

        // Set up mock expectations
        mock_client
            .expect_get_account_data()
            .withf(move |pubkey| *pubkey == token_manager_pda)
            .returning(move |_| {
                let data = mock_token_manager_data.clone();
                Box::pin(async move { Ok(data) })
            });

        // IMPORTANT: Return regular SPL Token program as the owner (not Token-2022)
        // This simulates a linked canonical token that uses the regular SPL Token program
        mock_client
            .expect_get_account_owner()
            .withf(move |pubkey| *pubkey == linked_token_mint)
            .returning(move |_| Box::pin(async move { Ok(spl_token::ID) }));

        let message = Message {
            cc_id: CrossChainId {
                chain: "axelar".to_string(),
                id: "linked-token-transfer-001".to_string(),
            },
            source_address: "axelar157hl7gpuknjmhtac2qnphuazv2yerfagva7lsu9vuj2pgn32z22qa26dk4"
                .to_string(),
            destination_chain: "solana".to_string(),
            destination_address: "test-destination".to_string(),
            payload_hash: [0u8; 32],
        };

        let builder = TransactionBuilder::new(
            Arc::clone(&keypair),
            mock_gas,
            Arc::new(mock_client),
            mock_redis,
        );

        let its_destination = solana_axelar_its::ID;
        let destination_pubkey = Pubkey::new_unique();
        let destination_address_bytes = destination_pubkey.to_bytes().to_vec();

        // Create InterchainTransfer message
        let interchain_transfer = InterchainTransfer {
            token_id,
            source_address: vec![0xBA; 20], // EVM source address
            destination_address: destination_address_bytes.clone(),
            amount: 100_000_000_000u64, // 100 tokens
            data: None,
        };

        let its_message = ItsMessage::InterchainTransfer(interchain_transfer);

        let hub_message = HubMessage::ReceiveFromHub {
            source_chain: "avalanche-fuji".to_string(),
            message: its_message,
        };

        let its_payload = borsh::to_vec(&hub_message).expect("Failed to serialize HubMessage");

        let (its_instruction, _its_accounts) = builder
            .build_execute_instruction(&message, &its_payload, its_destination)
            .await
            .expect("ITS build_execute_instruction should succeed for linked SPL token");

        assert_eq!(its_instruction.program_id, solana_axelar_its::ID);

        // Verify that the destination ATA in the instruction accounts is derived using
        // the regular SPL Token program (not Token-2022).
        // The destination ATA should be computed with spl_token::ID as the token program.
        let (expected_destination_ata, _) = get_ata_with_program(
            &destination_pubkey,
            &linked_token_mint,
            &spl_token::ID, // Regular SPL Token program
        );

        // Find the destination ATA in the instruction accounts
        let instruction_pubkeys: Vec<Pubkey> =
            its_instruction.accounts.iter().map(|a| a.pubkey).collect();

        assert!(
            instruction_pubkeys.contains(&expected_destination_ata),
            "Instruction should contain the destination ATA derived with regular SPL Token program. \
             Expected ATA: {}, but instruction accounts: {:?}",
            expected_destination_ata,
            instruction_pubkeys
        );

        // Also verify it does NOT contain an ATA derived with Token-2022
        use anchor_spl::token_2022::spl_token_2022;
        let (wrong_destination_ata, _) = get_ata_with_program(
            &destination_pubkey,
            &linked_token_mint,
            &spl_token_2022::ID, // Token-2022 program
        );

        // If the fix is correct, the wrong ATA should NOT be in the instruction
        // (unless it happens to match for some reason, which is unlikely with different program IDs)
        if expected_destination_ata != wrong_destination_ata {
            assert!(
                !instruction_pubkeys.contains(&wrong_destination_ata),
                "Instruction should NOT contain the ATA derived with Token-2022 program for linked SPL tokens"
            );
        }
    }
}
