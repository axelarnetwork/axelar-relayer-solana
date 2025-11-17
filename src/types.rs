use anyhow::{anyhow, Result};
use chrono::{offset::Utc, DateTime};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use solana_sdk::signature::Signature;
use solana_transaction_status::{
    option_serializer::OptionSerializer, UiLoadedAddresses, UiMessage,
};
use solana_transaction_status_client_types::{
    EncodedConfirmedTransactionWithStatusMeta, EncodedTransaction, UiInnerInstructions,
};
use std::str::FromStr;
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

impl SolanaTransaction {
    pub fn from_rpc_response(
        rpc_response: RpcGetTransactionResponse,
    ) -> Result<Self, anyhow::Error> {
        let result = rpc_response
            .result
            .ok_or_else(|| anyhow!("No result found"))?;
        let meta = result
            .meta
            .as_ref()
            .ok_or_else(|| anyhow!("No meta found"))?;
        let logs = meta
            .log_messages
            .clone()
            .ok_or_else(|| anyhow!("No log messages found"))?;

        let slot = result.slot;
        let signature = result
            .transaction
            .signatures
            .first()
            .and_then(|s| Signature::from_str(s).ok())
            .ok_or_else(|| anyhow!("Missing or invalid signature"))?;

        let timestamp = result
            .block_time
            .map(|bt| DateTime::from_timestamp(bt, 0).unwrap_or_else(Utc::now));
        let ixs = meta.inner_instructions.clone();
        let cost_units = meta.cost_units.unwrap_or(0) + meta.fee; // base fee + gas paid for the tx

        let account_keys = result
            .transaction
            .message
            .account_keys
            .clone()
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<String>>();

        Ok(Self {
            signature,
            timestamp,
            logs,
            slot,
            ixs,
            cost_units,
            account_keys,
        })
    }

    pub fn from_encoded_confirmed_transaction_with_status_meta(
        signature: Signature,
        tx: EncodedConfirmedTransactionWithStatusMeta,
    ) -> Result<Self, anyhow::Error> {
        let meta = &tx
            .transaction
            .meta
            .ok_or_else(|| anyhow!("No meta found"))?;
        let mut account_keys = match &tx.transaction.transaction {
            EncodedTransaction::LegacyBinary(_) => {
                Err(anyhow!("Legacy binary transactions are not supported"))
            }
            EncodedTransaction::Binary(_, _) => {
                Err(anyhow!("Binary transactions are not supported"))
            }
            EncodedTransaction::Json(json_transaction) => match &json_transaction.message {
                UiMessage::Parsed(parsed_message) => Ok(parsed_message
                    .account_keys
                    .iter()
                    .map(|key| key.pubkey.to_string())
                    .collect::<Vec<String>>()),
                UiMessage::Raw(raw_message) => Ok(raw_message.account_keys.clone()),
            },
            EncodedTransaction::Accounts(accounts_transaction) => Ok(accounts_transaction
                .account_keys
                .iter()
                .map(|key| key.pubkey.to_string())
                .collect::<Vec<String>>()),
        }?;

        if let OptionSerializer::Some(loaded) = meta.loaded_addresses.as_ref() {
            account_keys.extend(loaded.writable.iter().cloned());
            account_keys.extend(loaded.readonly.iter().cloned());
        }

        Ok(Self {
            signature,
            timestamp: tx
                .block_time
                .map(|bt| DateTime::from_timestamp(bt, 0).unwrap_or_else(Utc::now)),
            logs: meta
                .log_messages
                .clone()
                .ok_or_else(|| anyhow!("No log messages found"))?,
            slot: tx.slot as i64,
            ixs: {
                meta.inner_instructions
                    .clone()
                    .ok_or_else(|| anyhow!("No inner instructions found"))?
            },
            cost_units: meta.compute_units_consumed.clone().unwrap_or(0) + meta.fee, // base fee + gas paid for the tx
            account_keys,
        })
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RpcGetTransactionResponse {
    pub jsonrpc: String,
    pub result: Option<RpcTransactionResult>,
    pub id: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RpcTransactionResult {
    #[serde(rename = "blockTime")]
    pub block_time: Option<i64>,
    pub meta: Option<RpcTransactionMeta>,
    pub slot: i64,
    pub transaction: RpcTransaction,
    pub version: Option<Value>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RpcTransactionMeta {
    #[serde(rename = "computeUnitsConsumed")]
    pub compute_units_consumed: Option<u64>,
    #[serde(rename = "costUnits")]
    pub cost_units: Option<u64>,
    pub err: Option<Value>,
    pub fee: u64,
    #[serde(rename = "innerInstructions")]
    pub inner_instructions: Vec<UiInnerInstructions>,
    #[serde(default, rename = "loadedAddresses")]
    pub loaded_addresses: Option<RpcLoadedAddresses>,
    #[serde(rename = "logMessages")]
    pub log_messages: Option<Vec<String>>,
    #[serde(default, rename = "postBalances")]
    pub post_balances: Vec<u64>,
    #[serde(default, rename = "postTokenBalances")]
    pub post_token_balances: Vec<Value>,
    #[serde(default, rename = "preBalances")]
    pub pre_balances: Vec<u64>,
    #[serde(default, rename = "preTokenBalances")]
    pub pre_token_balances: Vec<Value>,
    pub rewards: Option<Vec<Value>>,
    #[serde(default)]
    pub status: RpcStatus,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct RpcLoadedAddresses {
    #[serde(default)]
    pub readonly: Vec<String>,
    #[serde(default)]
    pub writable: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct RpcStatus {
    #[serde(default, rename = "Ok")]
    pub ok: Option<Value>,
    #[serde(default, rename = "Err")]
    pub err: Option<Value>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RpcTransaction {
    pub message: RpcMessage,
    pub signatures: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RpcMessage {
    #[serde(default, rename = "accountKeys")]
    pub account_keys: Vec<String>,
    #[serde(default)]
    pub header: RpcMessageHeader,
    #[serde(default)]
    pub instructions: Vec<RpcInstruction>,
    #[serde(default, rename = "recentBlockhash")]
    pub recent_blockhash: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct RpcMessageHeader {
    #[serde(default, rename = "numReadonlySignedAccounts")]
    pub num_readonly_signed_accounts: u8,
    #[serde(default, rename = "numReadonlyUnsignedAccounts")]
    pub num_readonly_unsigned_accounts: u8,
    #[serde(default, rename = "numRequiredSignatures")]
    pub num_required_signatures: u8,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RpcInstruction {
    pub accounts: Vec<u8>,
    pub data: String,
    #[serde(rename = "programIdIndex")]
    pub program_id_index: u8,
    #[serde(rename = "stackHeight")]
    pub stack_height: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::fixtures::{
        encoded_confirmed_tx_with_meta_fixtures, rpc_response_fixtures,
    };
    #[test]
    fn test_from_rpc_response() {
        let rpc_response = &rpc_response_fixtures()[0];
        let transaction = SolanaTransaction::from_rpc_response(rpc_response.clone()).unwrap();

        let expected_tx = SolanaTransaction {
            signature: Signature::from_str("2E1HEKZLXDthn9qU8rXnj5nmUoDnbSWP6KsmbVWZ1PsA7Q63gEKWmRqy374wuxvwVDLhjX9RJYHeyfFmRQRTuMyF").unwrap(),
            timestamp: Some(chrono::DateTime::parse_from_rfc3339("2025-08-28T16:04:27Z").unwrap().with_timezone(&chrono::Utc)),
            logs: vec!["Program DaejccUfXqoAFTiDTxDuMQfQ9oa6crjtR9cT52v1AvGK invoke [1]".to_string(), "Program log: Instruction: EmitReceived".to_string(), "Program data: QF09492rFLE=".to_string(), "Program log: This is a message for received".to_string(), "Program DaejccUfXqoAFTiDTxDuMQfQ9oa6crjtR9cT52v1AvGK consumed 624 of 200000 compute units".to_string(), "Program DaejccUfXqoAFTiDTxDuMQfQ9oa6crjtR9cT52v1AvGK success".to_string()],
            slot: 404139482,
            ixs: vec![],
            cost_units: 6654,
            account_keys: vec![
                "483jTxdFmFGRnzgx9nBoQM2Zao5mZxKvFgHzTb4Ytn1L".to_string(),
                "DaejccUfXqoAFTiDTxDuMQfQ9oa6crjtR9cT52v1AvGK".to_string()
            ]
        };
        assert_eq!(transaction, expected_tx);
    }

    #[test]
    fn test_from_encoded_confirmed_transaction_with_status_meta() {
        let tx_with_meta = encoded_confirmed_tx_with_meta_fixtures()
            .into_iter()
            .next()
            .unwrap();
        let transaction = SolanaTransaction::from_encoded_confirmed_transaction_with_status_meta(
            Signature::from_str("3Dj8s38U1GNRf1kxH3BB5iJbN2RwNXeADZXP4NHXjbxErjsRoBHbGriG2qJMbidi5sDw5Jorjfows37iNHLctbb2").unwrap(),
            tx_with_meta,
        )
        .unwrap();
        let expected_tx = SolanaTransaction {
            signature: Signature::from_str("3Dj8s38U1GNRf1kxH3BB5iJbN2RwNXeADZXP4NHXjbxErjsRoBHbGriG2qJMbidi5sDw5Jorjfows37iNHLctbb2").unwrap(),
            timestamp: Some(chrono::DateTime::parse_from_rfc3339("2025-09-11T17:23:37Z").unwrap().with_timezone(&chrono::Utc)),
            logs: vec![ "Program 7RdSDLUUy37Wqc6s9ebgo52AwhGiw4XbJWZJgidQ1fJc invoke [1]".to_string(),
            "Program log: Instruction: PayNativeForContractCall".to_string(),
            "Program 7RdSDLUUy37Wqc6s9ebgo52AwhGiw4XbJWZJgidQ1fJc invoke [2]".to_string(),
            "Program 7RdSDLUUy37Wqc6s9ebgo52AwhGiw4XbJWZJgidQ1fJc consumed 2093 of 192610 compute units".to_string(),
            "Program 7RdSDLUUy37Wqc6s9ebgo52AwhGiw4XbJWZJgidQ1fJc success".to_string(),
            "Program 7RdSDLUUy37Wqc6s9ebgo52AwhGiw4XbJWZJgidQ1fJc consumed 9725 of 200000 compute units".to_string(),
            "Program 7RdSDLUUy37Wqc6s9ebgo52AwhGiw4XbJWZJgidQ1fJc success".to_string()],
            slot: 30309,
            ixs: vec![serde_json::from_str::<UiInnerInstructions>(r#"{
                        "index": 0,
                        "instructions": [
                            {
                                "accounts": [
                                    3
                                ],
                                "data": "9K93pGwFHUmnecEp6h1ZtRK5LYv5MLSbJPQ1ZeViVDgJwV3DNyhs5fodWcDTrKdxwhsnwCWyM6DXeM4ibUHv46fRnc2RSdcSDJDDs683XMDwWJbmizwqGyjuhMmcpZDinAYtKLWJBpPT9Jo1hhzwsKt3MfAcJREjCYLgWzACeiM3yh9R2phwkQAvPF5GyZknkkNhe19ZC6b9LHZ235vxr64VkNQEXeT1vYyZ1SLQ7mGXkZ34Ravy7zYf1",
                                "programIdIndex": 4,
                                "stackHeight": 2
                            }
                        ]
                    }"#).unwrap()],
            cost_units: 14725,
            account_keys: vec!["483jTxdFmFGRnzgx9nBoQM2Zao5mZxKvFgHzTb4Ytn1L".to_string(), "11111111111111111111111111111111".to_string(), "4BMYqAenKkzMtzdHieH9w5FhKKMkZTPGgccbUGwrRqTk".to_string(), "5m85qicoxxbWNbfAVFZ1DuUDKBchmBtUmMc7T5SrCXEB".to_string(), "7RdSDLUUy37Wqc6s9ebgo52AwhGiw4XbJWZJgidQ1fJc".to_string()],
        };
        assert_eq!(transaction, expected_tx);
    }
}
