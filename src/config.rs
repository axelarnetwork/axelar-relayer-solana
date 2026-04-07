use anyhow::anyhow;
use relayer_core::config::Config;
use serde::Deserialize;
use solana_commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Keypair;
use std::str::FromStr;

#[derive(Debug, Clone, Deserialize, Default)]
pub struct SolanaConfig {
    #[serde(flatten)]
    pub common_config: Config,

    pub solana_poll_rpc: String,
    pub solana_stream_rpc: String,
    pub solana_keypair: String,

    #[serde(default = "default_cu_price_lower_limit")]
    pub cu_price_lower_limit: u64,
    #[serde(default = "default_cu_price_upper_limit")]
    pub cu_price_upper_limit: u64,

    pub its_global_alt: Option<String>,
}

fn default_cu_price_lower_limit() -> u64 {
    1_000
}

fn default_cu_price_upper_limit() -> u64 {
    10_000
}

impl SolanaConfig {
    pub fn solana_commitment(&self) -> CommitmentConfig {
        CommitmentConfig::finalized()
    }

    pub fn signing_keypair(&self) -> Keypair {
        Keypair::from_base58_string(&self.solana_keypair)
    }

    pub fn its_global_alt_pubkey(&self) -> Option<Pubkey> {
        self.its_global_alt.as_ref().and_then(|s| {
            Pubkey::from_str(s)
                .map_err(|e| {
                    tracing::error!("Invalid its_global_alt pubkey '{}': {}", s, e);
                    e
                })
                .ok()
        })
    }

    /// Derives the 9 static accounts that should be in the global ITS ALT.
    pub fn expected_its_alt_accounts() -> Result<Vec<Pubkey>, anyhow::Error> {
        let (gateway_root_pda, _) = solana_axelar_gateway::GatewayConfig::try_find_pda()
            .ok_or_else(|| anyhow!("Failed to derive gateway root PDA"))?;
        let (gateway_event_authority, _) =
            Pubkey::try_find_program_address(&[b"__event_authority"], &solana_axelar_gateway::ID)
                .ok_or_else(|| anyhow!("Failed to derive gateway event authority PDA"))?;
        let (its_root_pda, _) = solana_axelar_its::InterchainTokenService::try_find_pda()
            .ok_or_else(|| anyhow!("Failed to derive ITS root PDA"))?;
        let (its_event_authority, _) =
            Pubkey::try_find_program_address(&[b"__event_authority"], &solana_axelar_its::ID)
                .ok_or_else(|| anyhow!("Failed to derive ITS event authority PDA"))?;

        Ok(vec![
            gateway_root_pda,
            gateway_event_authority,
            solana_axelar_gateway::ID,
            its_root_pda,
            anchor_spl::associated_token::ID,
            solana_sdk_ids::system_program::ID,
            its_event_authority,
            anchor_spl::token::ID,
            anchor_spl::token_2022::spl_token_2022::ID,
        ])
    }
}
