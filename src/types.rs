use std::str::FromStr as _;

use anyhow::{anyhow, Result};
use chrono::{offset::Utc, DateTime};
use serde::{Deserialize, Serialize};
use solana_sdk::signature::Signature;
use solana_transaction_status::{option_serializer::OptionSerializer, UiMessage};
use solana_transaction_status_client_types::{
    EncodedConfirmedTransactionWithStatusMeta, EncodedTransaction, UiInnerInstructions,
};
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

        Self::from_encoded_confirmed_transaction_with_status_meta(result)
    }

    pub fn from_encoded_confirmed_transaction_with_status_meta(
        tx: EncodedConfirmedTransactionWithStatusMeta,
    ) -> Result<Self, anyhow::Error> {
        let meta = &tx
            .transaction
            .meta
            .ok_or_else(|| anyhow!("No meta found"))?;
        let (signature, mut account_keys) = match &tx.transaction.transaction {
            EncodedTransaction::LegacyBinary(_) => {
                Err(anyhow!("Legacy binary transactions are not supported"))
            }
            EncodedTransaction::Binary(_, _) => {
                Err(anyhow!("Binary transactions are not supported"))
            }
            EncodedTransaction::Json(json_transaction) => match &json_transaction.message {
                UiMessage::Parsed(parsed_message) => Ok((
                    json_transaction
                        .signatures
                        .first()
                        .and_then(|s| Signature::from_str(s).ok())
                        .ok_or_else(|| anyhow!("Missing or invalid signature"))?,
                    parsed_message
                        .account_keys
                        .iter()
                        .map(|key| key.pubkey.to_string())
                        .collect::<Vec<String>>(),
                )),
                UiMessage::Raw(raw_message) => Ok((
                    json_transaction
                        .signatures
                        .first()
                        .and_then(|s| Signature::from_str(s).ok())
                        .ok_or_else(|| anyhow!("Missing or invalid signature"))?,
                    raw_message.account_keys.clone(),
                )),
            },
            EncodedTransaction::Accounts(accounts_transaction) => Ok((
                accounts_transaction
                    .signatures
                    .first()
                    .and_then(|s| Signature::from_str(s).ok())
                    .ok_or_else(|| anyhow!("Missing or invalid signature"))?,
                accounts_transaction
                    .account_keys
                    .iter()
                    .map(|key| key.pubkey.to_string())
                    .collect::<Vec<String>>(),
            )),
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
            cost_units: meta.fee,
            account_keys,
        })
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RpcGetTransactionResponse {
    pub jsonrpc: String,
    pub result: Option<EncodedConfirmedTransactionWithStatusMeta>,
    pub id: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_from_rpc_response_with_test_data() {
        let rpc_response_fixtures = crate::test_utils::fixtures::rpc_response_fixtures();
        let fixture = &rpc_response_fixtures[0];

        let transaction = SolanaTransaction::from_rpc_response(fixture.clone()).unwrap();

        // Verify signature extraction
        let expected_signature = Signature::from_str("2E1HEKZLXDthn9qU8rXnj5nmUoDnbSWP6KsmbVWZ1PsA7Q63gEKWmRqy374wuxvwVDLhjX9RJYHeyfFmRQRTuMyF").unwrap();
        assert_eq!(transaction.signature, expected_signature);

        // Verify timestamp conversion from blockTime
        let expected_timestamp = chrono::DateTime::from_timestamp(1756397067, 0).unwrap();
        assert_eq!(transaction.timestamp, Some(expected_timestamp));

        // Verify logs
        assert_eq!(transaction.logs.len(), 6);
        assert!(transaction.logs[0]
            .starts_with("Program DaejccUfXqoAFTiDTxDuMQfQ9oa6crjtR9cT52v1AvGK invoke [1]"));
        assert!(transaction.logs[5].contains("success"));

        // Verify slot
        assert_eq!(transaction.slot, 404139482);

        // Verify inner instructions (empty in this case)
        assert!(transaction.ixs.is_empty());

        // Verify cost units (fee)
        assert_eq!(transaction.cost_units, 5000);

        // Verify account keys
        assert_eq!(transaction.account_keys.len(), 2);
        assert_eq!(
            transaction.account_keys[0],
            "483jTxdFmFGRnzgx9nBoQM2Zao5mZxKvFgHzTb4Ytn1L"
        );
        assert_eq!(
            transaction.account_keys[1],
            "DaejccUfXqoAFTiDTxDuMQfQ9oa6crjtR9cT52v1AvGK"
        );
    }

    #[test]
    fn test_from_rpc_response_all_fixtures() {
        let rpc_response_fixtures = crate::test_utils::fixtures::rpc_response_fixtures();

        // Test all fixtures to ensure they all parse correctly
        for (i, fixture) in rpc_response_fixtures.iter().enumerate() {
            let transaction = SolanaTransaction::from_rpc_response(fixture.clone())
                .unwrap_or_else(|_| panic!("Failed to parse fixture {}", i));

            // Basic validation
            assert!(!transaction.signature.to_string().is_empty());
            assert!(transaction.slot > 0);
            assert!(transaction.cost_units > 0);
        }
    }

    #[test]
    fn test_from_encoded_confirmed_transaction_with_status_meta_with_test_data() {
        let fixtures = crate::test_utils::fixtures::encoded_confirmed_tx_with_meta_fixtures();
        let fixture = &fixtures[0];

        let transaction =
            SolanaTransaction::from_encoded_confirmed_transaction_with_status_meta(fixture.clone())
                .unwrap();

        // Verify signature extraction from the fixture
        assert!(!transaction.signature.to_string().is_empty());

        // Verify slot
        assert_eq!(transaction.slot, 30309);

        // Verify logs
        assert_eq!(transaction.logs.len(), 7);
        assert!(transaction.logs[0]
            .starts_with("Program 7RdSDLUUy37Wqc6s9ebgo52AwhGiw4XbJWZJgidQ1fJc invoke [1]"));

        // Verify account keys
        assert_eq!(transaction.account_keys.len(), 5);
        assert_eq!(
            transaction.account_keys[0],
            "483jTxdFmFGRnzgx9nBoQM2Zao5mZxKvFgHzTb4Ytn1L"
        );

        // Verify inner instructions
        assert_eq!(transaction.ixs.len(), 1);

        // Verify cost units
        assert_eq!(transaction.cost_units, 5000);
    }

    #[test]
    fn test_from_encoded_confirmed_transaction_all_fixtures() {
        let fixtures = crate::test_utils::fixtures::encoded_confirmed_tx_with_meta_fixtures();

        // Test all fixtures to ensure they all parse correctly
        for (i, fixture) in fixtures.iter().enumerate() {
            let transaction =
                SolanaTransaction::from_encoded_confirmed_transaction_with_status_meta(
                    fixture.clone(),
                )
                .unwrap_or_else(|_| panic!("Failed to parse fixture {}", i));

            // Basic validation
            assert!(!transaction.signature.to_string().is_empty());
            assert!(!transaction.logs.is_empty());
            assert!(!transaction.account_keys.is_empty());
            assert!(transaction.slot > 0);
            assert!(transaction.cost_units > 0);
        }
    }

    #[test]
    fn test_from_rpc_response_missing_result() {
        let rpc_response = RpcGetTransactionResponse {
            jsonrpc: "2.0".to_string(),
            result: None,
            id: 1,
        };

        let result = SolanaTransaction::from_rpc_response(rpc_response);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("No result found"));
    }

    #[test]
    fn test_both_methods_produce_same_result() {
        let rpc_response_fixtures = crate::test_utils::fixtures::rpc_response_fixtures();

        for fixture in rpc_response_fixtures {
            if let Some(result) = fixture.result.clone() {
                let tx1 = SolanaTransaction::from_rpc_response(fixture.clone()).unwrap();
                let tx2 =
                    SolanaTransaction::from_encoded_confirmed_transaction_with_status_meta(result)
                        .unwrap();

                assert_eq!(tx1, tx2);
            }
        }
    }
}
