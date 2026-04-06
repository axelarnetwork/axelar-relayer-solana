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
}
