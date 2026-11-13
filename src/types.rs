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

        if let Some(err) = &meta.err {
            return Err(anyhow!(
                "Skipping failed transaction (on-chain error: {err:?})"
            ));
        }

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

#[derive(Debug, Deserialize, Serialize)]
pub struct RpcGetTransactionResponse {
    pub jsonrpc: String,
    pub result: Option<EncodedConfirmedTransactionWithStatusMeta>,
    pub id: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::pubkey::Pubkey;
    use solana_sdk::transaction::TransactionVersion;
    use solana_transaction_parser::gmp_types::Event;
    use solana_transaction_parser::parser::{TransactionParser, TransactionParserTrait};
    use solana_transaction_parser::redis::MockCostCacheTrait;
    use std::str::FromStr;
    use std::sync::Arc;

    #[test]
    fn test_from_rpc_response_with_test_data() {
        let mut rpc_response_fixtures = crate::test_utils::fixtures::rpc_response_fixtures();
        let fixture = rpc_response_fixtures.remove(0);

        let transaction = SolanaTransaction::from_rpc_response(fixture).unwrap();

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
        for (i, fixture) in rpc_response_fixtures.into_iter().enumerate() {
            let transaction = SolanaTransaction::from_rpc_response(fixture)
                .unwrap_or_else(|_| panic!("Failed to parse fixture {}", i));

            // Basic validation
            assert!(!transaction.signature.to_string().is_empty());
            assert!(transaction.slot > 0);
            assert!(transaction.cost_units > 0);
        }
    }

    #[test]
    fn test_from_rpc_response_v1_transaction() {
        let fixture = crate::test_utils::fixtures::v1_rpc_response_fixture();
        let encoded = &fixture.result.as_ref().unwrap().transaction;
        assert_eq!(encoded.version, Some(TransactionVersion::Number(1)));

        let transaction = SolanaTransaction::from_rpc_response(fixture).unwrap();

        let expected_signature = Signature::from_str("4ZgiEc7dM6CUzeDKKvdPbT1WiotDn1VJyVD4UASshsb1gSZPyiDUND6b9Y5616Kc4vGYRwvVdJPniCrYbD6SLGFN").unwrap();
        assert_eq!(transaction.signature, expected_signature);
        assert_eq!(transaction.slot, 495666450);
        assert_eq!(transaction.logs.len(), 16);
        // v1 messages carry every account inline: nothing is loaded from lookup tables
        assert_eq!(transaction.account_keys.len(), 8);
        // CallContract and PayGas each emit one event via CPI
        assert_eq!(transaction.ixs.len(), 2);
        // 5000 base fee + 10000 absolute priority fee from the v1 transaction config
        assert_eq!(transaction.cost_units, 15000);
    }

    #[tokio::test]
    async fn test_v1_transaction_parses_into_call_and_gas_credit_events() {
        let fixture = crate::test_utils::fixtures::v1_rpc_response_fixture();
        let transaction = SolanaTransaction::from_rpc_response(fixture).unwrap();
        let signature = transaction.signature.to_string();

        // devnet-amplifier program ids, matching the fixture regardless of the build feature
        let parser = TransactionParser::new(
            "solana-18".to_string(),
            Pubkey::from_str("gasUBnVr9GZon2cp8X5gyFrUsQFhzrprjy734Ci6Bmn").unwrap(),
            Pubkey::from_str("gtwT4uGVTYSPnTGv6rSpMheyFyczUicxVWKqdtxNGw9").unwrap(),
            Pubkey::from_str("itsYxmqAxNKUL5zaj3fD1K1whuVhqpxKVoiLGie1reF").unwrap(),
            Arc::new(MockCostCacheTrait::new()),
        );
        let events = parser
            .parse_transaction(serde_json::to_string(&transaction).unwrap())
            .await
            .unwrap();

        assert_eq!(events.len(), 2);
        match &events[0] {
            Event::Call {
                message,
                destination_chain,
                payload,
                ..
            } => {
                assert_eq!(message.message_id, format!("{signature}-1.1"));
                assert_eq!(destination_chain, "avalanche-fuji");
                // base64 of the 2400-byte payload
                assert_eq!(payload.len(), 3200);
            }
            other => panic!("expected Call event, got {other:?}"),
        }
        match &events[1] {
            Event::GasCredit {
                message_id,
                payment,
                ..
            } => {
                assert_eq!(message_id, &format!("{signature}-1.1"));
                assert_eq!(payment.amount, "5000000");
            }
            other => panic!("expected GasCredit event, got {other:?}"),
        }
    }

    #[test]
    fn test_from_encoded_confirmed_transaction_with_status_meta_with_test_data() {
        let mut fixtures = crate::test_utils::fixtures::encoded_confirmed_tx_with_meta_fixtures();
        let fixture = fixtures.remove(0);

        let transaction =
            SolanaTransaction::from_encoded_confirmed_transaction_with_status_meta(fixture)
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
        for (i, fixture) in fixtures.into_iter().enumerate() {
            let transaction =
                SolanaTransaction::from_encoded_confirmed_transaction_with_status_meta(fixture)
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
    fn test_failed_transaction_is_rejected() {
        let mut fixtures = crate::test_utils::fixtures::encoded_confirmed_tx_with_meta_fixtures();
        let mut fixture = fixtures.remove(0);

        // Mark the transaction as failed on-chain; it must be dropped even though it still carries
        // valid logs/inner instructions.
        if let Some(meta) = fixture.transaction.meta.as_mut() {
            meta.err = Some(solana_sdk::transaction::TransactionError::AccountNotFound.into());
        }

        let result =
            SolanaTransaction::from_encoded_confirmed_transaction_with_status_meta(fixture);

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("failed transaction"));
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
        // Load fixtures twice - once for each method (avoids need for Clone)
        let rpc_fixtures = crate::test_utils::fixtures::rpc_response_fixtures();
        let encoded_fixtures = crate::test_utils::fixtures::rpc_response_fixtures();

        for (rpc_fixture, encoded_fixture) in rpc_fixtures.into_iter().zip(encoded_fixtures) {
            // Use encoded_fixture to get the inner result, rpc_fixture for the full response
            if let Some(result) = encoded_fixture.result {
                let tx1 = SolanaTransaction::from_rpc_response(rpc_fixture).unwrap();
                let tx2 =
                    SolanaTransaction::from_encoded_confirmed_transaction_with_status_meta(result)
                        .unwrap();

                assert_eq!(tx1, tx2);
            }
        }
    }
}
