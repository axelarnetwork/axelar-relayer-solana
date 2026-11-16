use relayer_core::config::Config;
use serde::Deserialize;
use solana_commitment_config::CommitmentConfig;
use solana_sdk::signature::Keypair;
use std::str::FromStr;
use tracing::error;

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

    #[serde(default = "default_min_wallet_balance_lamports")]
    pub min_wallet_balance_lamports: u64,

    pub solana_subscriber_commitment: Option<String>,
    pub solana_includer_commitment: Option<String>,
}

fn default_cu_price_lower_limit() -> u64 {
    1_000
}

fn default_cu_price_upper_limit() -> u64 {
    10_000
}

fn default_min_wallet_balance_lamports() -> u64 {
    500_000_000 // 0.5 SOL
}

fn parse_commitment_or(
    field: &str,
    value: Option<&String>,
    default: CommitmentConfig,
) -> CommitmentConfig {
    match value {
        Some(s) => CommitmentConfig::from_str(s).unwrap_or_else(|e| {
            error!(
                "Invalid {} '{}': {}; falling back to {:?}",
                field, s, e, default.commitment
            );
            default
        }),
        None => default,
    }
}

impl SolanaConfig {
    pub fn solana_subscriber_commitment(&self) -> CommitmentConfig {
        parse_commitment_or(
            "solana_subscriber_commitment",
            self.solana_subscriber_commitment.as_ref(),
            CommitmentConfig::finalized(),
        )
    }

    // The includer has a differnet commit level than the subscriber
    // This is because the includer posts transactions but does not report
    // events like the subscriber does, so there is no risk in the case of a reorg.
    // Still, in the history of Solana there has never been a confirmed block
    // that was reorg, so this is generally safe. But even in the extreme case that
    // one happens, the subscriber only reports finalized events.
    pub fn solana_includer_commitment(&self) -> CommitmentConfig {
        parse_commitment_or(
            "solana_includer_commitment",
            self.solana_includer_commitment.as_ref(),
            CommitmentConfig::confirmed(),
        )
    }

    pub fn signing_keypair(&self) -> Keypair {
        Keypair::from_base58_string(&self.solana_keypair)
    }
}
