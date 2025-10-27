use crate::gas_calculator::GasCalculatorTrait;
use crate::utils::{
    get_deployer_ata, get_gateway_root_config_internal, get_governance_config_pda,
    get_governance_event_authority_pda, get_incoming_message_pda, get_its_root_pda,
    get_minter_roles_pda, get_mpl_token_metadata_account, get_operator_proposal_pda,
    get_proposal_pda, get_token_manager_ata, get_token_manager_pda, get_token_mint_pda,
    get_validate_message_signing_pda,
};
use crate::{error::TransactionBuilderError, utils::get_gateway_event_authority_pda};
use anchor_lang::InstructionData;
use anchor_lang::ToAccountMetas;
use anchor_spl::{associated_token::spl_associated_token_account, token_2022::spl_token_2022};
use async_trait::async_trait;
use axelar_solana_gateway_v2::{executable::ExecutablePayload, state::incoming_message::Message};
use interchain_token_transfer_gmp::GMPPayload;
use mpl_token_metadata;
use relayer_core::utils::ThreadSafe;
use solana_sdk::instruction::Instruction;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signer::keypair::Keypair;
use solana_sdk::signer::Signer as _;
use solana_sdk::transaction::Transaction;
use std::sync::Arc;

#[derive(Clone)]
pub struct TransactionBuilder<GE: GasCalculatorTrait + ThreadSafe> {
    keypair: Arc<Keypair>,
    gas_calculator: GE,
}

#[async_trait]
pub trait TransactionBuilderTrait {
    async fn build(
        &self,
        ix: Instruction,
        gas_exceeded_count: u64,
    ) -> Result<Transaction, TransactionBuilderError>;

    async fn build_execute_instruction(
        &self,
        message: &Message,
        payload: &[u8],
        destination_address: Pubkey,
    ) -> Result<Instruction, TransactionBuilderError>;
}

impl<GE: GasCalculatorTrait + ThreadSafe> TransactionBuilder<GE> {
    pub fn new(keypair: Arc<Keypair>, gas_calculator: GE) -> Self {
        Self {
            keypair,
            gas_calculator,
        }
    }
}

#[async_trait]
impl<GE: GasCalculatorTrait + ThreadSafe> TransactionBuilderTrait for TransactionBuilder<GE> {
    async fn build(
        &self,
        ix: Instruction,
        gas_exceeded_count: u64,
    ) -> Result<Transaction, TransactionBuilderError> {
        let compute_unit_price_ix = self
            .gas_calculator
            .compute_unit_price(&[ix.clone()])
            .await
            .map_err(|e| TransactionBuilderError::ClientError(e.to_string()))?;

        // Since simulation gets the latest blockhash we can directly use it for the tx construction
        let (compute_budget_ix, hash) = self
            .gas_calculator
            .compute_budget(&[ix.clone()], gas_exceeded_count)
            .await
            .map_err(|e| TransactionBuilderError::ClientError(e.to_string()))?;

        let instructions = vec![compute_unit_price_ix, compute_budget_ix, ix];

        Ok(Transaction::new_signed_with_payer(
            &instructions,
            Some(&self.keypair.pubkey()),
            &[&self.keypair],
            hash,
        ))
    }

    async fn build_execute_instruction(
        &self,
        message: &Message,
        payload: &[u8],
        destination_address: Pubkey,
    ) -> Result<Instruction, TransactionBuilderError> {
        let (incoming_message_pda, _) = get_incoming_message_pda(&message.command_id());

        match destination_address {
            x if x == axelar_solana_its_v2::ID => {
                let gmp_decoded_payload = GMPPayload::decode(payload)
                    .map_err(|e| TransactionBuilderError::GenericError(e.to_string()))?;

                let token_id = gmp_decoded_payload
                    .token_id()
                    .map_err(|e| TransactionBuilderError::GenericError(e.to_string()))?;

                let minter = match gmp_decoded_payload {
                    GMPPayload::DeployInterchainToken(deploy) if !deploy.minter.is_empty() => {
                        Some(Pubkey::try_from(deploy.minter.as_ref()).map_err(|e| {
                            TransactionBuilderError::GenericError(format!(
                                "Invalid minter pubkey: {}",
                                e
                            ))
                        })?)
                    }
                    _ => None,
                };

                let (signing_pda, _) = get_validate_message_signing_pda(
                    &message.command_id(),
                    &axelar_solana_gateway_v2::ID,
                );
                let (event_authority, _) = get_gateway_event_authority_pda();
                let (gateway_root_pda, _) = get_gateway_root_config_internal();

                let executable = axelar_solana_its_v2::accounts::AxelarExecuteAccounts {
                    incoming_message_pda,
                    signing_pda,
                    axelar_gateway_program: axelar_solana_gateway_v2::ID,
                    event_authority,
                    gateway_root_pda,
                };

                let (its_root_pda, _) = get_its_root_pda();
                let (token_manager_pda, _) = get_token_manager_pda(&its_root_pda, &token_id);
                let (token_mint, _) = get_token_mint_pda(&its_root_pda, &token_id);
                let (token_manager_ata, _) = get_token_manager_ata(&its_root_pda, &token_mint);

                let (deployer_ata, _) = get_deployer_ata(&self.keypair.pubkey(), &token_mint);

                let (mpl_token_metadata_account, _) = get_mpl_token_metadata_account(&token_mint);

                let minter_roles_pda =
                    minter.map(|minter| get_minter_roles_pda(&token_manager_pda, &minter).0);

                let accounts = axelar_solana_its_v2::accounts::Execute {
                    executable,
                    payer: self.keypair.pubkey(),
                    system_program: solana_program::system_program::id(),
                    event_authority,
                    its_root_pda,
                    token_manager_pda,
                    token_mint,
                    token_manager_ata,
                    token_program: spl_token_2022::ID,
                    associated_token_program: spl_associated_token_account::ID,
                    rent: solana_program::sysvar::rent::id(),
                    deployer_ata: Some(deployer_ata),
                    minter,
                    minter_roles_pda,
                    mpl_token_metadata_account: Some(mpl_token_metadata_account),
                    mpl_token_metadata_program: Some(mpl_token_metadata::ID),
                    sysvar_instructions: Some(solana_program::sysvar::instructions::ID),
                    // TODO: Is this right?
                    deployer: Some(self.keypair.pubkey()),
                    authority: None,
                    destination: None,
                    destination_ata: None,
                    program: axelar_solana_its_v2::ID,
                };

                let ix_data = axelar_solana_its_v2::instruction::Execute {
                    message: message.clone(),
                    payload: payload.to_vec(),
                }
                .data();

                Ok(Instruction {
                    program_id: axelar_solana_its_v2::ID,
                    accounts: accounts.to_account_metas(None),
                    data: ix_data,
                })
            }
            x if x == axelar_solana_governance_v2::ID => {
                let (signing_pda, _) = get_validate_message_signing_pda(
                    &message.command_id(),
                    &axelar_solana_gateway_v2::ID,
                );
                let (event_authority, _) = get_gateway_event_authority_pda();
                let (gateway_root_pda, _) = get_gateway_root_config_internal();
                let executable = axelar_solana_governance_v2::accounts::AxelarExecuteAccounts {
                    incoming_message_pda,
                    signing_pda,
                    axelar_gateway_program: axelar_solana_gateway_v2::ID,
                    event_authority,
                    gateway_root_pda,
                };

                let (governance_config, _) = get_governance_config_pda();
                let (governance_event_authority, _) = get_governance_event_authority_pda();
                let (proposal_pda, _) = get_proposal_pda(&message.command_id());
                let (operator_proposal_pda, _) = get_operator_proposal_pda(&message.command_id());

                let accounts = axelar_solana_governance_v2::accounts::ProcessGmp {
                    executable,
                    payer: self.keypair.pubkey(),
                    governance_config,
                    proposal_pda,
                    operator_proposal_pda,
                    governance_event_authority,
                    axelar_governance_program: axelar_solana_governance_v2::ID,
                    system_program: solana_program::system_program::id(),
                };

                let ix_data = axelar_solana_governance_v2::instruction::ProcessGmp {
                    message: message.clone(),
                    payload: payload.to_vec(),
                }
                .data();

                Ok(Instruction {
                    program_id: axelar_solana_governance_v2::ID,
                    accounts: accounts.to_account_metas(None),
                    data: ix_data,
                })
            }
            _ => {
                let decoded_payload = ExecutablePayload::decode(payload)
                    .map_err(|e| TransactionBuilderError::GenericError(e.to_string()))?;
                let user_provided_accounts = decoded_payload.account_meta();

                let (signing_pda, _) = get_validate_message_signing_pda(
                    &message.command_id(),
                    // TODO: Is this gateway or dst program?
                    &axelar_solana_gateway_v2::ID,
                );
                let (event_authority, _) = get_gateway_event_authority_pda();
                let (gateway_root_pda, _) = get_gateway_root_config_internal();

                let mut accounts =
                    axelar_solana_gateway_v2::executable::helpers::AxelarExecuteAccounts {
                        incoming_message_pda,
                        signing_pda,
                        axelar_gateway_program: axelar_solana_gateway_v2::ID,
                        event_authority,
                        gateway_root_pda,
                    }
                    .to_account_metas(None);

                accounts.extend(user_provided_accounts);

                let data =
                    axelar_solana_gateway_v2::executable::helpers::AxelarExecuteInstruction {
                        message: message.clone(),
                        payload_without_accounts: decoded_payload
                            .payload_without_accounts()
                            .to_vec(),
                        encoding_scheme: decoded_payload.encoding_scheme(),
                    }
                    .data();

                Ok(Instruction {
                    program_id: axelar_solana_gateway_v2::ID,
                    accounts,
                    data,
                })
            }
        }
    }
}
