use relayer_core::config::Config;
use serde::Deserialize;
use solana_sdk::{commitment_config::CommitmentConfig, signature::Keypair};

#[derive(Debug, Clone, Deserialize, Default)]
pub struct SolanaConfig {
    #[serde(flatten)]
    pub common_config: Config,

    pub refund_manager_addresses: String,
    pub includer_secrets: String,
    pub solana_poll_rpc: String,
    pub solana_stream_rpc: String,
    pub solana_faucet_url: String,
    pub solana_gas_service: String,
    pub solana_gateway: String,
    pub solana_its: String,
    pub solana_commitment: CommitmentConfig,
    pub solana_keypair: String,
}

impl SolanaConfig {
    pub fn signing_keypair(&self) -> Keypair {
        Keypair::from_base58_string(&self.solana_keypair)
    }
}
