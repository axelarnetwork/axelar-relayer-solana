use std::str::FromStr;

use crate::config::WalletConfig;
use solana_sdk::pubkey::Pubkey;

pub struct Wallet {
    pub public_key: Pubkey,
    pub secret_key: Pubkey,
}

impl Wallet {
    pub async fn new(config: WalletConfig) -> Result<Self, anyhow::Error> {
        Ok(Self {
            public_key: Pubkey::from_str(&config.public_key)?,
            secret_key: Pubkey::from_str(&config.secret_key)?,
        })
    }
}
