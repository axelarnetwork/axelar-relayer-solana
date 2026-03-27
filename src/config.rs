use relayer_core::config::Config;
use serde::Deserialize;
use solana_commitment_config::CommitmentConfig;
use solana_sdk::signature::Keypair;

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
}
