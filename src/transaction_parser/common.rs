use crate::parser::ParserConfig;
use solana_transaction_status::UiCompiledInstruction;
use tracing::{debug, warn};

pub fn check_discriminators_and_address(
    instruction: &UiCompiledInstruction,
    config: ParserConfig,
) -> Option<Vec<u8>> {
    let bytes = match bs58::decode(&instruction.data).into_vec() {
        Ok(bytes) => bytes,
        Err(e) => {
            warn!("failed to decode bytes: {:?}", e);
            return None;
        }
    };
    if bytes.len() < 16 {
        return None;
    }

    if bytes.get(0..8) != Some(&config.event_cpi_discriminator) {
        debug!(
            "expected event cpi discriminator, got {:?}",
            bytes.get(0..8)
        );
        return None;
    }
    if bytes.get(8..16) != Some(&config.event_type_discriminator) {
        debug!(
            "expected event type discriminator, got {:?}",
            bytes.get(8..16)
        );
        return None;
    }
    Some(bytes.get(16..)?.to_vec())
}
