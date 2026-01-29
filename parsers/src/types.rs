use chrono::{offset::Utc, DateTime};
use serde::{Deserialize, Serialize};
use solana_sdk::signature::Signature;
use solana_transaction_status_client_types::UiInnerInstructions;
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct SolanaTransaction {
    pub signature: Signature,
    pub timestamp: Option<DateTime<Utc>>,
    pub logs: Vec<String>,
    pub slot: i64,
    pub ixs: Vec<UiInnerInstructions>,
    pub cost_units: u64,
    pub account_keys: Vec<String>,
}
