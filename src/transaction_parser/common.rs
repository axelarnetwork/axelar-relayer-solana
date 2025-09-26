use std::str::FromStr;

use crate::error::TransactionParsingError;
use crate::parser::ParserConfig;
use solana_sdk::pubkey::Pubkey;
use solana_transaction_status::UiCompiledInstruction;
use tracing::{debug, warn};

pub fn check_discriminators_and_address(
    instruction: &UiCompiledInstruction,
    config: ParserConfig,
    accounts: &[String],
) -> Result<Vec<u8>, TransactionParsingError> {
    let account_keys: Vec<Pubkey> = accounts
        .iter()
        .map(|account| {
            Pubkey::from_str(account).map_err(|e| {
                warn!("invalid account address: {:?}", e);
                TransactionParsingError::InvalidAccountAddress(e.to_string())
            })
        })
        .collect::<Result<_, _>>()?;

    if !validate_account_address(
        &config.expected_contract_address,
        instruction,
        &account_keys,
    )? {
        return Err(TransactionParsingError::InvalidAccountAddress(
            "expected account did not match with actual emitter".to_string(),
        ));
    }

    let bytes = bs58::decode(&instruction.data).into_vec().map_err(|e| {
        warn!("invalid instruction data: {:?}", e);
        TransactionParsingError::InvalidAccountAddress(e.to_string())
    })?;
    if bytes.len() < 16 {
        return Err(TransactionParsingError::InvalidInstructionData(
            "instruction data is too short".to_string(),
        ));
    }

    if bytes.get(0..8) != Some(&config.event_cpi_discriminator) {
        debug!(
            "expected event cpi discriminator, got {:?}",
            bytes.get(0..8)
        );
        return Err(TransactionParsingError::InvalidInstructionData(
            "expected event cpi discriminator".to_string(),
        ));
    }
    if bytes.get(8..16) != Some(&config.event_type_discriminator) {
        debug!(
            "expected event type discriminator, got {:?}",
            bytes.get(8..16)
        );
        return Err(TransactionParsingError::InvalidInstructionData(
            "expected event type discriminator".to_string(),
        ));
    }
    Ok(bytes
        .get(16..)
        .ok_or_else(|| {
            TransactionParsingError::InvalidInstructionData(
                "instruction data is too short".to_string(),
            )
        })?
        .to_vec())
}

fn validate_account_address(
    required_account: &Pubkey,
    instruction: &UiCompiledInstruction,
    account_keys: &[Pubkey],
) -> Result<bool, TransactionParsingError> {
    let actual_account = account_keys
        .get(instruction.program_id_index as usize)
        .ok_or_else(|| {
            TransactionParsingError::InvalidInstructionData(
                "required account is out of bounds".to_string(),
            )
        })?;

    Ok(required_account == actual_account)
}
