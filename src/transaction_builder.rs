use crate::gas_calculator::GasCalculatorTrait;
use crate::utils::{
    get_gateway_root_config_internal, get_governance_config_pda,
    get_governance_event_authority_pda, get_incoming_message_pda, get_operator_proposal_pda,
    get_proposal_pda, get_validate_message_signing_pda,
};
use crate::{error::TransactionBuilderError, utils::get_gateway_event_authority_pda};
use anchor_lang::InstructionData;
use anchor_lang::ToAccountMetas;
use async_trait::async_trait;
use axelar_solana_gateway_v2::{executable::ExecutablePayload, Message};
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
    async fn build(&self, ix: Instruction) -> Result<Transaction, TransactionBuilderError>;

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
    async fn build(&self, ix: Instruction) -> Result<Transaction, TransactionBuilderError> {
        let compute_unit_price_ix = self
            .gas_calculator
            .compute_unit_price(&[ix.clone()])
            .await
            .map_err(|e| TransactionBuilderError::ClientError(e.to_string()))?;

        // Since simulation gets the latest blockhash we can directly use it for the tx construction
        let (compute_budget_ix, hash) = self
            .gas_calculator
            .compute_budget(&[ix.clone()])
            .await
            .map_err(|e| TransactionBuilderError::ClientError(e.to_string()))?;

        // Add the two budget instructions first, the the instruction we want to execute on Solana
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
                // let (signing_pda, _) = Pubkey::find_program_address(
                //     &[
                //         axelar_solana_gateway_v2::seed_prefixes::VALIDATE_MESSAGE_SIGNING_SEED,
                //         &message.command_id(),
                //     ],
                //     &axelar_solana_gateway_v2::ID,
                // );
                // let (event_authority, _) = get_gateway_event_authority_pda();
                // let (gateway_root_pda, _) = get_gateway_root_config_internal();
                // let executable = axelar_solana_its_v2::accounts::AxelarExecuteAccounts {
                //     incoming_message_pda,
                //     signing_pda,
                //     axelar_gateway_program: axelar_solana_gateway_v2::ID,
                //     event_authority,
                //     system_program: solana_program::system_program::id(),
                // };

                // let (governance_config, _) = get_governance_config_pda();
                // let (governance_event_authority, _) = get_governance_event_authority_pda();
                // let (proposal_pda, _) = get_proposal_pda(&message.command_id());
                // let (operator_proposal_pda, _) = get_operator_proposal_pda(&message.command_id());

                // let accounts = axelar_solana_its_v2::accounts::Execute {
                //     executable,
                //     payer: self.keypair.pubkey(),
                //     system_program: solana_program::system_program::id(),
                //     its_root_pda,
                //     token_manager_pda,
                //     token_mint,
                //     token_manager_ata,
                //     token_program,
                //     associated_token_program,
                //     rent,
                //     deployer_ata,
                //     minter,
                //     minter_roles_pda,
                //     mpl_token_metadata_account,
                //     mpl_token_metadata_program,
                //     sysvar_instructions,
                //     event_authority,
                //     program,
                // };

                // let ix_data = axelar_solana_its_v2::instruction::Execute {
                //     message: message.clone(),
                //     payload: payload.to_vec(),
                // }
                // .data();

                // Ok(Instruction {
                //     program_id: axelar_solana_its_v2::ID,
                //     accounts: accounts.to_account_metas(None),
                //     data: ix_data,
                // })

                Err(TransactionBuilderError::GenericError(
                    "ITS is not supported".to_string(),
                ))
            }
            x if x == axelar_solana_governance_v2::ID => {
                let (signing_pda, _) = Pubkey::find_program_address(
                    &[
                        axelar_solana_gateway_v2::seed_prefixes::VALIDATE_MESSAGE_SIGNING_SEED,
                        &message.command_id(),
                    ],
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
                let decoded_payload = ExecutablePayload::decode(&payload)
                    .map_err(|e| TransactionBuilderError::GenericError(e.to_string()))?;
                let user_provided_accounts = decoded_payload.account_meta();

                let (signing_pda, _) = get_validate_message_signing_pda(&message.command_id());
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
