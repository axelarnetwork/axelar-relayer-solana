use crate::gas_calculator::GasCalculatorTrait;
use crate::includer::ALTInfo;
use crate::includer_client::IncluderClientTrait;
use crate::utils::{
    get_deployer_ata, get_destination_ata, get_gateway_root_config_internal,
    get_governance_config_pda, get_governance_event_authority_pda, get_incoming_message_pda,
    get_its_root_pda, get_minter_roles_pda, get_mpl_token_metadata_account,
    get_operator_proposal_pda, get_proposal_pda, get_token_manager_ata, get_token_manager_pda,
    get_token_mint_pda, get_validate_message_signing_pda,
};
use crate::{
    error::TransactionBuilderError, transaction_type::SolanaTransactionType,
    utils::get_gateway_event_authority_pda,
};
use anchor_lang::InstructionData;
use anchor_lang::ToAccountMetas;
use anchor_spl::{associated_token::spl_associated_token_account, token_2022::spl_token_2022};
use async_trait::async_trait;
use base64::Engine;
use interchain_token_transfer_gmp::GMPPayload;
use mpl_token_metadata;
use relayer_core::utils::ThreadSafe;
use solana_axelar_gateway::{executable::ExecutablePayload, state::incoming_message::Message};
use solana_axelar_its::instructions::{
    execute_deploy_interchain_token_extra_accounts, execute_interchain_transfer_extra_accounts,
    execute_link_token_extra_accounts,
};
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
pub struct TransactionBuilder<GE: GasCalculatorTrait, IC: IncluderClientTrait> {
    keypair: Arc<Keypair>,
    gas_calculator: GE,
    includer_client: Arc<IC>,
}

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait TransactionBuilderTrait<IC: IncluderClientTrait>: ThreadSafe {
    async fn build(
        &self,
        ixs: &[Instruction],
        alt_info: Option<ALTInfo>,
    ) -> Result<SolanaTransactionType, TransactionBuilderError>;

    async fn build_execute_instruction(
        &self,
        message: &Message,
        payload: &[u8],
        destination_address: Pubkey,
        existing_alt_pubkey: Option<Pubkey>,
    ) -> Result<(Instruction, Option<ALTInfo>), TransactionBuilderError>;

    async fn build_its_instruction(
        &self,
        message: &Message,
        payload: &[u8],
        incoming_message_pda: Pubkey,
        existing_alt_pubkey: Option<Pubkey>,
    ) -> Result<(Instruction, Option<ALTInfo>), TransactionBuilderError>;

    async fn build_governance_instruction(
        &self,
        message: &Message,
        payload: &[u8],
        incoming_message_pda: Pubkey,
    ) -> Result<(Instruction, Option<ALTInfo>), TransactionBuilderError>;

    async fn build_executable_instruction(
        &self,
        message: &Message,
        payload: &[u8],
        incoming_message_pda: Pubkey,
        destination_address: Pubkey,
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
            .compute_budget(ixs)
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
            x if x == solana_axelar_its::ID => {
                self.build_its_instruction(
                    message,
                    payload,
                    incoming_message_pda,
                    existing_alt_pubkey,
                )
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
        existing_alt_pubkey: Option<Pubkey>,
    ) -> Result<(Instruction, Option<ALTInfo>), TransactionBuilderError> {
        let gmp_decoded_payload = GMPPayload::decode(payload)
            .map_err(|e| TransactionBuilderError::GenericError(e.to_string()))?;

        let token_id = gmp_decoded_payload
            .token_id()
            .map_err(|e| TransactionBuilderError::GenericError(e.to_string()))?;

        let (signing_pda, _) =
            get_validate_message_signing_pda(&message.command_id(), &solana_axelar_gateway::ID);
        let (event_authority, _) = get_gateway_event_authority_pda();
        let (gateway_root_pda, _) = get_gateway_root_config_internal();

        let executable = solana_axelar_its::accounts::AxelarExecuteAccounts {
            incoming_message_pda,
            signing_pda,
            axelar_gateway_program: solana_axelar_gateway::ID,
            event_authority,
            gateway_root_pda,
        };

        let (its_root_pda, _) = get_its_root_pda();
        let (token_manager_pda, _) = get_token_manager_pda(&its_root_pda, &token_id);
        let (token_mint, _) = get_token_mint_pda(&its_root_pda, &token_id);
        let (token_manager_ata, _) = get_token_manager_ata(&token_manager_pda, &token_mint);

        let mut accounts = solana_axelar_its::accounts::Execute {
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
            program: solana_axelar_its::ID,
        }
        .to_account_metas(None);

        match gmp_decoded_payload {
            GMPPayload::InterchainTransfer(ref transfer) => {
                let destination_address =
                    Pubkey::try_from(transfer.destination_address.as_ref())
                        .map_err(|e| TransactionBuilderError::GenericError(e.to_string()))?;
                let (destination_ata, _) = get_destination_ata(&destination_address, &token_mint);
                accounts.extend(execute_interchain_transfer_extra_accounts(
                    destination_address,
                    destination_ata,
                ));
            }

            GMPPayload::DeployInterchainToken(ref deploy) => {
                let (deployer_ata, _) = get_deployer_ata(&self.keypair.pubkey(), &token_mint);
                let minter = if deploy.minter.is_empty() {
                    None
                } else {
                    Some(Pubkey::try_from(deploy.minter.as_ref()).map_err(|e| {
                        TransactionBuilderError::GenericError(format!(
                            "Invalid minter pubkey: {}",
                            e
                        ))
                    })?)
                };

                let minter_roles_pda =
                    minter.map(|minter| get_minter_roles_pda(&token_manager_pda, &minter).0);

                let (mpl_token_metadata_account, _) = get_mpl_token_metadata_account(&token_mint);

                accounts.extend(execute_deploy_interchain_token_extra_accounts(
                    deployer_ata,
                    self.keypair.pubkey(),
                    solana_program::sysvar::instructions::ID,
                    mpl_token_metadata::ID,
                    mpl_token_metadata_account,
                    minter,
                    minter_roles_pda,
                ));
            }
            GMPPayload::LinkToken(ref link) => {
                let (deployer_ata, _) = get_deployer_ata(&self.keypair.pubkey(), &token_mint);
                let minter = Pubkey::try_from(link.link_params.as_ref()).ok();

                let minter_roles_pda =
                    minter.map(|minter| get_minter_roles_pda(&token_manager_pda, &minter).0);

                accounts.extend(execute_link_token_extra_accounts(
                    deployer_ata,
                    minter,
                    minter_roles_pda,
                ))
            }
            _ => {}
        }

        let data = solana_axelar_its::instruction::Execute {
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
                program_id: solana_axelar_its::ID,
                accounts,
                data,
            },
            alt_info,
        ))
    }

    async fn build_governance_instruction(
        &self,
        message: &Message,
        payload: &[u8],
        incoming_message_pda: Pubkey,
    ) -> Result<(Instruction, Option<ALTInfo>), TransactionBuilderError> {
        let (signing_pda, _) =
            get_validate_message_signing_pda(&message.command_id(), &solana_axelar_gateway::ID);
        let (event_authority, _) = get_gateway_event_authority_pda();
        let (gateway_root_pda, _) = get_gateway_root_config_internal();
        let executable = solana_axelar_governance::accounts::AxelarExecuteAccounts {
            incoming_message_pda,
            signing_pda,
            axelar_gateway_program: solana_axelar_gateway::ID,
            event_authority,
            gateway_root_pda,
        };

        let (governance_config, _) = get_governance_config_pda();
        let (governance_event_authority, _) = get_governance_event_authority_pda();
        let (proposal_pda, _) = get_proposal_pda(&message.command_id());
        let (operator_proposal_pda, _) = get_operator_proposal_pda(&message.command_id());

        let accounts = solana_axelar_governance::accounts::ProcessGmp {
            executable,
            payer: self.keypair.pubkey(),
            governance_config,
            proposal_pda,
            operator_proposal_pda,
            governance_event_authority,
            axelar_governance_program: solana_axelar_governance::ID,
            system_program: solana_program::system_program::id(),
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
            None,
        ))
    }

    async fn build_executable_instruction(
        &self,
        message: &Message,
        payload: &[u8],
        incoming_message_pda: Pubkey,
        destination_address: Pubkey,
    ) -> Result<(Instruction, Option<ALTInfo>), TransactionBuilderError> {
        let b64_decoded = base64::prelude::BASE64_STANDARD
            .decode(payload)
            .map_err(|e| {
                TransactionBuilderError::GenericError(format!("Failed to decode payload: {}", e))
            })?;
        let decoded_payload = ExecutablePayload::decode(&b64_decoded)
            .map_err(|e| TransactionBuilderError::PayloadDecodeError(e.to_string()))?; // return custom error to send cannot_execute_message

        let user_provided_accounts = decoded_payload.account_meta();

        let (signing_pda, _) =
            get_validate_message_signing_pda(&message.command_id(), &destination_address);
        let (event_authority, _) = get_gateway_event_authority_pda();
        let (gateway_root_pda, _) = get_gateway_root_config_internal();

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
            None,
        ))
    }

    async fn build_lookup_table_instructions(
        &self,
        recent_slot: u64,
        execute_accounts: &[AccountMeta],
    ) -> Result<(Instruction, Instruction, Pubkey), TransactionBuilderError> {
        let (ix_alt_create, alt_pubkey) =
            create_lookup_table(self.keypair.pubkey(), self.keypair.pubkey(), recent_slot);

        let alt_accounts = execute_accounts.iter().map(|acc| acc.pubkey).collect();

        let ix_alt_extend = extend_lookup_table(
            alt_pubkey,
            self.keypair.pubkey(),
            Some(self.keypair.pubkey()),
            alt_accounts,
        );
        Ok((ix_alt_create, ix_alt_extend, alt_pubkey))
    }
}

#[cfg(test)]
mod tests {
    use crate::gas_calculator::MockGasCalculatorTrait;
    use crate::includer::ALTInfo;
    use crate::includer_client::MockIncluderClientTrait;
    use crate::transaction_builder::{TransactionBuilder, TransactionBuilderTrait};
    use crate::transaction_type::SolanaTransactionType;
    use anchor_lang::prelude::AccountMeta;
    use base64::prelude::BASE64_STANDARD;
    use base64::Engine;
    use interchain_token_transfer_gmp::alloy_primitives::{Bytes, FixedBytes, Uint};
    use interchain_token_transfer_gmp::GMPPayload;
    use solana_axelar_gateway::payload::EncodingScheme;
    use solana_axelar_gateway::{
        executable::ExecutablePayload, state::incoming_message::Message, CrossChainId,
    };
    use solana_axelar_governance;
    use solana_axelar_its;
    use solana_sdk::hash::Hash;
    use solana_sdk::instruction::Instruction;
    use solana_sdk::message::VersionedMessage;
    use solana_sdk::pubkey::Pubkey;
    use solana_sdk::signature::Keypair;
    use solana_sdk::signer::Signer;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_transaction_builder_build_with_alt_produces_versioned_tx() {
        let keypair = Arc::new(Keypair::new());
        let mut mock_gas = MockGasCalculatorTrait::new();
        let mock_client = Arc::new(MockIncluderClientTrait::new());

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

        let compute_unit_price_ix = Instruction::new_with_bytes(Pubkey::new_unique(), &[9], vec![]);
        let compute_budget_ix = Instruction::new_with_bytes(Pubkey::new_unique(), &[8], vec![]);
        let recent_blockhash = Hash::new_unique();

        let cup_ix_clone = compute_unit_price_ix.clone();
        mock_gas
            .expect_compute_unit_price()
            .times(1)
            .returning(move |_| {
                let ix = cup_ix_clone.clone();
                Ok(ix)
            });

        let cb_ix_clone = compute_budget_ix.clone();
        mock_gas
            .expect_compute_budget()
            .times(1)
            .returning(move |_| {
                let ix = cb_ix_clone.clone();
                let hash = recent_blockhash;
                Ok((ix, hash))
            });

        let alt_info = ALTInfo::new(None, None, Some(alt_pubkey)).with_addresses(alt_addresses);

        let builder = TransactionBuilder::new(Arc::clone(&keypair), mock_gas, mock_client);

        let result = builder
            .build(std::slice::from_ref(&user_ix), Some(alt_info))
            .await
            .expect("build with ALT should succeed");

        match result {
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
        let mock_client = Arc::new(MockIncluderClientTrait::new());

        let user_program = Pubkey::new_unique();
        let user_ix = Instruction::new_with_bytes(
            user_program,
            &[4, 5, 6],
            vec![AccountMeta::new(keypair.pubkey(), true)],
        );

        let compute_unit_price_ix = Instruction::new_with_bytes(Pubkey::new_unique(), &[7], vec![]);
        let compute_budget_ix = Instruction::new_with_bytes(Pubkey::new_unique(), &[8], vec![]);
        let recent_blockhash = Hash::new_unique();

        let cup_ix_clone = compute_unit_price_ix.clone();
        mock_gas
            .expect_compute_unit_price()
            .times(1)
            .returning(move |_| {
                let ix = cup_ix_clone.clone();
                Ok(ix)
            });

        let cb_ix_clone = compute_budget_ix.clone();
        mock_gas
            .expect_compute_budget()
            .times(1)
            .returning(move |_| {
                let ix = cb_ix_clone.clone();
                let hash = recent_blockhash;
                Ok((ix, hash))
            });

        let builder = TransactionBuilder::new(Arc::clone(&keypair), mock_gas, mock_client);

        let result = builder
            .build(std::slice::from_ref(&user_ix), None)
            .await
            .expect("build without ALT should succeed");

        match result {
            SolanaTransactionType::Legacy(tx) => {
                assert_eq!(tx.message.instructions.len(), 3);
                assert_eq!(tx.message.account_keys[0], keypair.pubkey());
                assert_eq!(tx.message.recent_blockhash, recent_blockhash);
                assert_eq!(tx.signatures.len(), 1);
            }
            _ => panic!("expected Legacy transaction when no ALTInfo is provided"),
        }
    }

    #[tokio::test]
    async fn test_build_execute_instruction_with_three_addresses() {
        let keypair = Arc::new(Keypair::new());
        let mock_gas = MockGasCalculatorTrait::new();
        let mut mock_client = MockIncluderClientTrait::new();

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

        mock_client
            .expect_get_slot()
            .times(1)
            .returning(|| Box::pin(async { Ok(1000u64) }));

        let builder =
            TransactionBuilder::new(Arc::clone(&keypair), mock_gas, Arc::new(mock_client));

        // ITS address should return Some(ALTInfo)
        let its_destination = solana_axelar_its::ID;

        let token_id = [1u8; 32];
        let destination_address_bytes = [2u8; 32];
        let gmp_payload =
            GMPPayload::InterchainTransfer(interchain_token_transfer_gmp::InterchainTransfer {
                token_id: FixedBytes::from(token_id),
                destination_address: Bytes::from(destination_address_bytes),
                amount: Uint::from(0), // Empty amount for simplicity
                data: Bytes::from(vec![]),
                selector: Uint::from(0),
                source_address: Bytes::from(vec![3u8; 20]),
            });
        let its_payload = gmp_payload.encode();

        let (its_instruction, its_alt_info) = builder
            .build_execute_instruction(&message, &its_payload, its_destination, None)
            .await
            .expect("ITS build_execute_instruction should succeed");

        assert_eq!(its_instruction.program_id, solana_axelar_its::ID);
        assert!(its_alt_info.is_some(), "ITS should return Some(ALTInfo)");
        let its_alt = its_alt_info.unwrap();
        assert!(
            its_alt.alt_pubkey.is_some(),
            "ALTInfo should have alt_pubkey"
        );
        assert!(
            its_alt.alt_ix_create.is_some(),
            "ALTInfo should have alt_ix_create"
        );
        assert!(
            its_alt.alt_ix_extend.is_some(),
            "ALTInfo should have alt_ix_extend"
        );
        assert!(
            its_alt.alt_addresses.is_some(),
            "ALTInfo should have alt_addresses"
        );

        // Governance address should return None
        let governance_destination = solana_axelar_governance::ID;
        let governance_payload = b"governance-payload-data";

        let (governance_instruction, governance_alt_info) = builder
            .build_execute_instruction(&message, governance_payload, governance_destination, None)
            .await
            .expect("Governance build_execute_instruction should succeed");

        assert_eq!(
            governance_instruction.program_id,
            solana_axelar_governance::ID
        );
        assert!(
            governance_alt_info.is_none(),
            "Governance should return None for ALTInfo"
        );

        // Arbitrary program address should return None
        let arbitrary_destination = Pubkey::new_unique();

        let executable_payload = ExecutablePayload::new::<AccountMeta>(
            &[1, 2, 3, 4, 5],
            &[AccountMeta::new(arbitrary_destination, false)],
            EncodingScheme::Borsh,
        );
        let executable_payload_bytes = executable_payload.encode().unwrap();
        let executable_payload_b64 = BASE64_STANDARD.encode(&executable_payload_bytes);

        let (arbitrary_instruction, arbitrary_alt_info) = builder
            .build_execute_instruction(
                &message,
                executable_payload_b64.as_bytes(),
                arbitrary_destination,
                None,
            )
            .await
            .expect("Arbitrary program build_execute_instruction should succeed");

        assert_eq!(arbitrary_instruction.program_id, arbitrary_destination);
        assert!(
            arbitrary_alt_info.is_none(),
            "Arbitrary program should return None for ALTInfo"
        );
    }
}
