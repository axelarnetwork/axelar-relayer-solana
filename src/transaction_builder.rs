use crate::gas_calculator::GasCalculatorTrait;
use crate::includer::ALTInfo;
use crate::includer_client::IncluderClientTrait;
use crate::utils::{
    get_deployer_ata, get_gateway_root_config_internal, get_governance_config_pda,
    get_governance_event_authority_pda, get_incoming_message_pda, get_its_root_pda,
    get_minter_roles_pda, get_mpl_token_metadata_account, get_operator_proposal_pda,
    get_proposal_pda, get_token_manager_ata, get_token_manager_pda, get_token_mint_pda,
    get_validate_message_signing_pda,
};
use crate::{
    error::TransactionBuilderError, transaction_type::SolanaTransactionType,
    utils::get_gateway_event_authority_pda,
};
use anchor_lang::InstructionData;
use anchor_lang::ToAccountMetas;
use anchor_spl::{associated_token::spl_associated_token_account, token_2022::spl_token_2022};
use async_trait::async_trait;
use axelar_solana_gateway_v2::{executable::ExecutablePayload, state::incoming_message::Message};
use interchain_token_transfer_gmp::GMPPayload;
use mpl_token_metadata;
use relayer_core::utils::ThreadSafe;
use solana_sdk::address_lookup_table::instruction::{create_lookup_table, extend_lookup_table};
use solana_sdk::address_lookup_table::state::AddressLookupTable;
use solana_sdk::instruction::{AccountMeta, Instruction};
use solana_sdk::message::{v0, AddressLookupTableAccount, VersionedMessage};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signer::keypair::Keypair;
use solana_sdk::signer::Signer as _;
use solana_sdk::transaction::{Transaction, VersionedTransaction};
use std::sync::Arc;
use tracing::debug;

#[derive(Clone)]
pub struct TransactionBuilder<
    GE: GasCalculatorTrait + ThreadSafe,
    IC: IncluderClientTrait + ThreadSafe,
> {
    keypair: Arc<Keypair>,
    gas_calculator: GE,
    includer_client: Arc<IC>,
}

#[async_trait]
pub trait TransactionBuilderTrait<IC: IncluderClientTrait + ThreadSafe> {
    async fn build(
        &self,
        ixs: &[Instruction],
        gas_exceeded_count: u64, // how many times the gas exceeded the limit in previous attempts
        alt_info: Option<ALTInfo>,
    ) -> Result<SolanaTransactionType, TransactionBuilderError>;

    async fn build_execute_instruction(
        &self,
        message: &Message,
        payload: &[u8],
        destination_address: Pubkey,
        existing_alt_pubkey: Option<Pubkey>,
    ) -> Result<(Instruction, Option<ALTInfo>), TransactionBuilderError>;

    async fn build_lookup_table_instructions(
        &self,
        recent_slot: u64,
        execute_accounts: &[AccountMeta],
    ) -> Result<(Instruction, Instruction, Pubkey), TransactionBuilderError>;
}

impl<GE: GasCalculatorTrait + ThreadSafe, IC: IncluderClientTrait + ThreadSafe>
    TransactionBuilder<GE, IC>
{
    pub fn new(keypair: Arc<Keypair>, gas_calculator: GE, includer_client: Arc<IC>) -> Self {
        Self {
            keypair,
            gas_calculator,
            includer_client,
        }
    }
}

#[async_trait]
impl<GE: GasCalculatorTrait + ThreadSafe, IC: IncluderClientTrait + ThreadSafe>
    TransactionBuilderTrait<IC> for TransactionBuilder<GE, IC>
{
    async fn build(
        &self,
        ixs: &[Instruction],
        gas_exceeded_count: u64,
        alt_info: Option<ALTInfo>,
    ) -> Result<SolanaTransactionType, TransactionBuilderError> {
        let compute_unit_price_ix = self
            .gas_calculator
            .compute_unit_price(ixs)
            .await
            .map_err(|e| TransactionBuilderError::ClientError(e.to_string()))?;

        // Since simulation gets the latest blockhash we can directly use it for the tx construction
        let (compute_budget_ix, hash) = self
            .gas_calculator
            .compute_budget(ixs, gas_exceeded_count)
            .await
            .map_err(|e| TransactionBuilderError::ClientError(e.to_string()))?;

        let mut instructions = vec![compute_unit_price_ix, compute_budget_ix];
        instructions.extend_from_slice(ixs);

        match alt_info {
            Some(alt_info) => {
                let alt_pubkey = alt_info.alt_pubkey.ok_or_else(|| {
                    TransactionBuilderError::GenericError(
                        "ALTInfo provided without pubkey".to_string(),
                    )
                })?;

                let addresses = if let Some(addresses) = alt_info.alt_addresses {
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
                };

                let alt_ref = AddressLookupTableAccount {
                    key: alt_pubkey,
                    addresses,
                };
                let v0_msg = v0::Message::try_compile(
                    &self.keypair.pubkey(),
                    &instructions,
                    &[alt_ref],
                    hash,
                )
                .map_err(|e| TransactionBuilderError::GenericError(e.to_string()))?;
                let message = VersionedMessage::V0(v0_msg);
                let versioned_tx = VersionedTransaction::try_new(message, &[&self.keypair])
                    .map_err(|e| TransactionBuilderError::GenericError(e.to_string()))?;
                Ok(SolanaTransactionType::Versioned(versioned_tx))
            }
            None => Ok(SolanaTransactionType::Legacy(
                Transaction::new_signed_with_payer(
                    &instructions,
                    Some(&self.keypair.pubkey()),
                    &[&self.keypair],
                    hash,
                ),
            )),
        }
    }

    async fn build_execute_instruction(
        &self,
        message: &Message,
        payload: &[u8],
        destination_address: Pubkey,
        existing_alt_pubkey: Option<Pubkey>,
    ) -> Result<(Instruction, Option<ALTInfo>), TransactionBuilderError> {
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
                let (token_manager_ata, _) = get_token_manager_ata(&token_manager_pda, &token_mint);

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
                }
                .to_account_metas(None);

                let data = axelar_solana_its_v2::instruction::Execute {
                    message: message.clone(),
                    payload: payload.to_vec(),
                }
                .data();

                let alt_info = if let Some(existing_alt_pubkey) = existing_alt_pubkey {
                    // Use existing ALT pubkey, no need to create new ALT transaction
                    // Addresses will be fetched from chain in build() method
                    debug!(
                        "Using existing ALT pubkey from Redis: {}",
                        existing_alt_pubkey
                    );
                    Some(ALTInfo::new(None, None, Some(existing_alt_pubkey)))
                } else {
                    // Create new ALT
                    let recent_slot = self
                        .includer_client
                        .get_slot()
                        .await
                        .map_err(|e| TransactionBuilderError::ClientError(e.to_string()))?;

                    let alt_accounts: Vec<Pubkey> = accounts.iter().map(|acc| acc.pubkey).collect();

                    let (alt_ix_create, alt_ix_extend, alt_pubkey) = self
                        .build_lookup_table_instructions(recent_slot, &accounts)
                        .await
                        .map_err(|e| TransactionBuilderError::GenericError(e.to_string()))?;
                    Some(
                        ALTInfo::new(Some(alt_ix_create), Some(alt_ix_extend), Some(alt_pubkey))
                            .with_addresses(alt_accounts),
                    )
                };

                Ok((
                    Instruction {
                        program_id: axelar_solana_its_v2::ID,
                        accounts,
                        data,
                    },
                    alt_info,
                ))
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
                }
                .to_account_metas(None);

                let data = axelar_solana_governance_v2::instruction::ProcessGmp {
                    message: message.clone(),
                    payload: payload.to_vec(),
                }
                .data();

                Ok((
                    Instruction {
                        program_id: axelar_solana_governance_v2::ID,
                        accounts,
                        data,
                    },
                    None,
                ))
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

                Ok((
                    Instruction {
                        program_id: axelar_solana_gateway_v2::ID,
                        accounts,
                        data,
                    },
                    None,
                ))
            }
        }
    }

    async fn build_lookup_table_instructions(
        &self,
        recent_slot: u64,
        execute_accounts: &[AccountMeta],
    ) -> Result<(Instruction, Instruction, Pubkey), TransactionBuilderError> {
        let (ix_alt_create, alt_pubkey) =
            create_lookup_table(self.keypair.pubkey(), self.keypair.pubkey(), recent_slot);

        let alt_accounts: Vec<Pubkey> = execute_accounts.iter().map(|acc| acc.pubkey).collect();

        let ix_alt_extend = extend_lookup_table(
            alt_pubkey,
            self.keypair.pubkey(),
            Some(self.keypair.pubkey()),
            alt_accounts,
        );
        Ok((ix_alt_create, ix_alt_extend, alt_pubkey))
    }
}
