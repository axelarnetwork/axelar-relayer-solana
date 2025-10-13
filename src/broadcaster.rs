/*!

Broadcaster implementation for Solana. Listens to GATEWAY_TX (essentially APPROVE messages) and REFUND.

# Note

Relayer code assumes there is one message per transaction. This might not be a safe assumption,
and broadcaster should potentially be returning a vector of BroadcastResults.

*/

use super::poll_client::{SolanaRpcClient, SolanaRpcClientTrait};
use crate::gas_estimator::GasEstimatorTrait;
use crate::includer_client::IncluderClientTrait;
use crate::utils::get_signature_verification_pda;
use crate::wallet::Wallet;
use anchor_lang::prelude::AccountMeta;
use anchor_lang::prelude::Context;
use anchor_lang::InstructionData;
use anchor_lang::ToAccountMetas;
use async_trait::async_trait;
use axelar_solana_encoding::types::execute_data::ExecuteData;
use axelar_solana_gateway_v2::accounts::InitializePayloadVerificationSession;
use axelar_solana_gateway_v2::instruction::InitializePayloadVerificationSession as InitializePayloadVerificationSessionIx;
use base64::engine::general_purpose;
use base64::Engine;
use borsh::BorshDeserialize;
use relayer_core::error::BroadcasterError::RPCCallFailed;
use relayer_core::gmp_api::gmp_types::{ExecuteTaskFields, RefundTaskFields};
use relayer_core::utils::ThreadSafe;
use relayer_core::{
    error::BroadcasterError,
    includer::{BroadcastResult, Broadcaster},
};
use solana_sdk::instruction::Instruction;
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

#[derive(Clone)]
pub struct SolanaBroadcaster<GE> {
    wallet: Arc<Wallet>,
    client: Arc<dyn IncluderClientTrait>,
    gateway_address: Pubkey,
    gas_service_address: Pubkey,
    chain_name: String,
    gas_estimator: GE,
}

impl<GE> SolanaBroadcaster<GE>
where
    GE: GasEstimatorTrait + ThreadSafe,
{
    pub fn new(
        wallet: Arc<Wallet>,
        client: Arc<dyn IncluderClientTrait>,
        gateway_address: Pubkey,
        gas_service_address: Pubkey,
        chain_name: String,
        gas_estimator: GE,
    ) -> anyhow::Result<Self, BroadcasterError> {
        Ok(SolanaBroadcaster {
            wallet,
            client,
            gateway_address,
            gas_service_address,
            chain_name,
            gas_estimator,
        })
    }
}

#[derive(Clone)]
pub struct SolanaTransaction;

#[async_trait]
impl<GE> Broadcaster for SolanaBroadcaster<GE>
where
    GE: GasEstimatorTrait + ThreadSafe,
{
    type Transaction = SolanaTransaction;

    #[tracing::instrument(skip(self), fields(message_id))]
    async fn broadcast_prover_message(
        &self,
        tx_blob: String,
    ) -> Result<BroadcastResult<Self::Transaction>, BroadcasterError> {
        let execute_data_bytes = tx_blob.into_bytes().as_slice();
        let execute_data = ExecuteData::try_from_slice(execute_data_bytes).map_err(|_err| {
            BroadcasterError::GenericError("cannot decode execute data".to_string())
        })?;

        let (verification_session_tracker_pda, bump) =
            get_signature_verification_pda(&execute_data.payload_merkle_root);

        let ix_data = axelar_solana_gateway_v2::instruction::InitializePayloadVerificationSession {
            merkle_root: execute_data.payload_merkle_root,
        }
        .data();

        let ix = Instruction {
            program_id: axelar_solana_gateway_v2::ID,
            accounts: vec![
                AccountMeta::new(self.wallet.public_key, true),
                AccountMeta::new_readonly(self.gateway_address, false),
                AccountMeta::new(verification_session_tracker_pda, false),
                AccountMeta::new_readonly(solana_program::system_program::id(), false),
            ],
            data: ix_data,
        };

        let res = self
            .send_to_chain(self.wallet.public_key, vec![ix], None)
            .await;
        let (tx_hash, status) = match res {
            Ok(response) => (response.message_hash, Ok(())),
            Err(err) => (String::new(), Err(err)),
        };

        Ok(BroadcastResult {
            transaction: SolanaTransaction,
            tx_hash,
            message_id: Some(message.message_id.clone()),
            source_chain: Some(message.source_chain.clone()),
            status,
        })
    }

    #[tracing::instrument(skip(self))]
    async fn broadcast_refund(&self, _data: String) -> Result<String, BroadcasterError> {
        Ok(String::new())
    }

    #[tracing::instrument(skip(self), fields(message_id))]
    async fn broadcast_execute_message(
        &self,
        message: ExecuteTaskFields,
    ) -> Result<BroadcastResult<Self::Transaction>, BroadcasterError> {
        let destination_address: Pubkey =
            message.message.destination_address.parse().map_err(|e| {
                BroadcasterError::GenericError(format!("TonAddressParseError: {e:?}"))
            })?;

        let decoded_bytes = general_purpose::STANDARD
            .decode(message.payload.clone())
            .map_err(|e| {
                BroadcasterError::GenericError(format!("Failed decoding payload: {e:?}"))
            })?;

        let payload_len = decoded_bytes.len();

        let hex_payload = hex::encode(decoded_bytes.clone());

        let message_id = message.message.message_id;
        let source_chain = message.message.source_chain;

        tracing::Span::current().record("message_id", &message_id);

        let available_gas = u64::from_str(&message.available_gas_balance.amount).unwrap_or(0);
        let required_gas = self.gas_estimator.execute_estimate(payload_len).await;

        info!(
            "Considering execute message: message_id={}, source_chain={}, available_gas={}, required_gas={}, payload_len={}",
            message_id, source_chain, available_gas, required_gas, payload_len
        );
        if available_gas < required_gas {
            return Ok(BroadcastResult {
                transaction: SolanaTransaction,
                tx_hash: String::new(),
                message_id: Some(message_id),
                source_chain: Some(source_chain),
                status: Err(BroadcasterError::InsufficientGas(
                    "Cannot proceed to execute".to_string(),
                )),
            });
        }

        let result = async {
            let relayer_execute_msg = RelayerExecuteMessage::new(
                message_id.clone(),
                source_chain.clone(),
                message.message.source_address,
                self.chain_name.clone(),
                destination_address,
                hex_payload,
                self.wallet.public_key.clone(),
            );

            let boc = relayer_execute_msg
                .to_cell()
                .map_err(|e| BroadcasterError::GenericError(e.to_string()))?
                .to_boc_hex(true)
                .map_err(|e| {
                    BroadcasterError::GenericError(format!(
                        "Failed to serialize relayer execute message: {e:?}"
                    ))
                })?;

            let execute_message_value: BigUint =
                BigUint::from(self.gas_estimator.execute_send(payload_len).await);

            let actions: Vec<OutAction> = vec![out_action(
                &boc,
                execute_message_value.clone(),
                self.gateway_address.clone(),
            )
            .map_err(|e| BroadcasterError::GenericError(e.to_string()))?];

            let res = self.send_to_chain(wallet, actions.clone(), None).await;
            let (tx_hash, status) = match res {
                Ok(response) => (response.message_hash, Ok(())),
                Err(err) => (String::new(), Err(err)),
            };

            Ok(BroadcastResult {
                transaction: SolanaTransaction,
                tx_hash,
                message_id: Some(message_id.clone()),
                source_chain: Some(source_chain.clone()),
                status,
            })
        }
        .await;

        self.wallet.release(wallet).await;

        result
    }

    #[tracing::instrument(skip(self), fields(message_id))]
    async fn broadcast_refund_message(
        &self,
        refund_task: RefundTaskFields,
    ) -> Result<String, BroadcasterError> {
        if refund_task.remaining_gas_balance.token_id.is_some() {
            return Err(BroadcasterError::GenericError(
                "Refund task with token_id is not supported".to_string(),
            ));
        }

        let cleaned_hash = refund_task
            .message
            .message_id
            .strip_prefix("0x")
            .unwrap_or(&refund_task.message.message_id);

        tracing::Span::current().record("message_id", &refund_task.message.message_id);

        let tx_hash = TonHash::from_hex(cleaned_hash)
            .map_err(|e| BroadcasterError::GenericError(e.to_string()))?;

        let address = Pubkey::from_str(&refund_task.refund_recipient_address)
            .map_err(|err| BroadcasterError::GenericError(err.to_string()))?;

        let original_amount = BigUint::from_str(&refund_task.remaining_gas_balance.amount)
            .map_err(|err| BroadcasterError::GenericError(err.to_string()))?;
        let gas_estimate = self.gas_estimator.native_gas_refund_estimate().await;

        info!(
            "Considering refund message: message_id={}, address={}, original_amount={}, gas_estimate={}",
            refund_task.message.message_id, address, refund_task.remaining_gas_balance.amount, gas_estimate
        );

        if original_amount < BigUint::from(gas_estimate) {
            info!(
                "Not enough balance to cover gas for refund: message_id={}",
                refund_task.message.message_id
            );
            return Err(BroadcasterError::InsufficientGas(
                "Not enough balance to cover gas for refund".to_string(),
            ));
        }

        let amount = original_amount - BigUint::from(gas_estimate);

        let native_refund = NativeRefundMessage::new(tx_hash, address, amount);

        let boc = native_refund
            .to_cell()
            .map_err(|e| BroadcasterError::GenericError(e.to_string()))?
            .to_boc_hex(true)
            .map_err(|e| {
                BroadcasterError::GenericError(format!(
                    "Failed to serialize relayer execute message: {e:?}"
                ))
            })?;

        let wallet = self.wallet.acquire().await.map_err(|e| {
            error!("Error acquiring wallet: {e:?}");
            BroadcasterError::GenericError(format!("Wallet acquire failed: {e:?}"))
        })?;

        let result = async {
            let msg_value: BigUint = BigUint::from(REFUND_DUST);

            let actions: Vec<OutAction> =
                vec![
                    out_action(&boc, msg_value.clone(), self.gas_service_address.clone())
                        .map_err(|e| BroadcasterError::GenericError(e.to_string()))?,
                ];

            let res = self.send_to_chain(wallet, actions.clone(), None).await;
            let (tx_hash, _status) = match res {
                Ok(response) => (response.message_hash, Ok(())),
                Err(err) => (String::new(), Err(err)),
            };

            Ok(tx_hash)
        }
        .await;

        self.wallet.release(wallet).await;

        result
    }
}

#[cfg(test)]
mod tests {
    use crate::broadcaster::{SolanaBroadcaster, SolanaTransaction};
    use crate::gas_estimator::MockGasEstimator;
    use crate::poll_client::{MockRestClient, V3MessageResponse};
    use crate::wallet::Wallet;
    use base64::prelude::BASE64_STANDARD;
    use base64::Engine;
    use relayer_core::error::BroadcasterError;
    use relayer_core::gmp_api::gmp_types::{
        Amount, ExecuteTaskFields, GatewayV2Message, RefundTaskFields,
    };
    use relayer_core::includer::{BroadcastResult, Broadcaster};
    use std::sync::Arc;

    struct MockQueryIdWrapper;

    #[async_trait::async_trait]
    impl HighLoadQueryIdWrapper for MockQueryIdWrapper {
        async fn next(
            &self,
            _address: &str,
            _timeout: u64,
            _force_shift_increase: bool,
        ) -> Result<HighLoadQueryId, HighLoadQueryIdWrapperError> {
            Ok(HighLoadQueryId::from_shift_and_bitnumber(0u32, 0u32)
                .await
                .unwrap())
        }
    }

    #[tokio::test]
    async fn test_broadcast_prover_message() {
        let mut client = MockRestClient::new();

        client.expect_post_v3_message().returning(move |_| {
            Ok(V3MessageResponse {
                message_hash: "abc".to_string(),
                message_hash_norm: "ABC".to_string(),
            })
        });

        let wallet = load_wallets().await;
        let query_id_wrapper = MockQueryIdWrapper;
        let gateway_address = TonAddress::from_str(
            "0:0000000000000000000000000000000000000000000000000000000000000000",
        )
        .unwrap();
        let gas_service_address = TonAddress::from_str(
            "0:0000000000000000000000000000000000000000000000000000000000000fff",
        )
        .unwrap();

        let mut gas_estimator = MockGasEstimator::new();
        gas_estimator.expect_approve_send().returning(|_| 42u64);
        gas_estimator
            .expect_highload_wallet_send()
            .returning(|_| 1024u64);

        let broadcaster = TONBroadcaster {
            wallet: Arc::new(wallet),
            query_id_wrapper: Arc::new(query_id_wrapper),
            client: Arc::new(client),
            gateway_address,
            gas_service_address,
            chain_name: "ton2".to_string(),
            gas_estimator,
        };
        let approve_message = hex::encode(BASE64_STANDARD.decode("te6cckECDAEAAYsAAggAAAAoAQIBYYAAAAAAAAAAAAAAAAAAAACAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAADf5gkADAQHABADi0LAAUYmshNOh1nWEdwB3eJHd51H6EH1kg3v2M30y32eQAAAAAAAAAAAAAAAAAAAAAQ+j+g0KWjWTaPqB9qQHuWZQn7IPz7x3xzwbprT1a85sjh0UlPlFU84LDdRcD4GZ6n6GJlEKKTlRW5QtlzKGrAsBAtAFBECeAcQjykQMXsK+7MnQoVK1T8jnpBbJMbcInq8iFgWvFwYHCAkAiDB4MTdmZDdkYTNkODE5Y2ZiYzQ2ZmYyOGYzZDgwOTgwNzcwZWMxYjgwZmQ3ZDFiMjI5Y2VjMzI1MTkzOWI5YjIzZi0xABxhdmFsYW5jaGUtZnVqaQBUMHhkNzA2N0FlM0MzNTllODM3ODkwYjI4QjdCRDBkMjA4NENmRGY0OWI1AgAKCwBAuHpKD2RLehhu5xoUVGNPcMIqYqyhprpna1F1wh1/2TAACHRvbjJLddsV").unwrap());

        let res = broadcaster
            .broadcast_prover_message(approve_message.to_string())
            .await;
        assert!(res.is_ok());

        let good = BroadcastResult {
            transaction: SolanaTransaction,
            tx_hash: "abc".to_string(),
            message_id: Some(
                "0x17fd7da3d819cfbc46ff28f3d80980770ec1b80fd7d1b229cec3251939b9b23f-1".to_string(),
            ),
            source_chain: Some("avalanche-fuji".to_string()),
            status: Ok(()),
        };

        let unwrapped = res.unwrap();

        assert_eq!(unwrapped.tx_hash, good.tx_hash);
        assert_eq!(unwrapped.message_id, good.message_id);
        assert_eq!(unwrapped.source_chain, good.source_chain);
    }

    #[tokio::test]
    async fn test_broadcast_prover_message_invalid_input() {
        let mut client = MockRestClient::new();
        client.expect_post_v3_message().returning(|_| {
            Ok(V3MessageResponse {
                message_hash: "abc".to_string(),
                message_hash_norm: "ABC".to_string(),
            })
        });
        let wallet = load_wallets().await;
        let query_id_wrapper = MockQueryIdWrapper;
        let gateway_address = TonAddress::from_str(
            "0:0000000000000000000000000000000000000000000000000000000000000000",
        )
        .unwrap();
        let gas_service_address = TonAddress::from_str(
            "0:0000000000000000000000000000000000000000000000000000000000000fff",
        )
        .unwrap();

        let gas_estimator = MockGasEstimator::new();

        let broadcaster = SolanaBroadcaster {
            wallet: Arc::new(wallet),
            query_id_wrapper: Arc::new(query_id_wrapper),
            client: Arc::new(client),
            gateway_address,
            gas_service_address,
            chain_name: "ton2".to_string(),
            gas_estimator,
        };

        // Invalid base64 string for BOC (non-decodable)
        let invalid_approve_message = "!!!invalid_base64_data###";

        let res = broadcaster
            .broadcast_prover_message(invalid_approve_message.to_string())
            .await;

        assert!(res.is_err());

        match res {
            Err(BroadcasterError::GenericError(e)) => {
                assert!(
                    e.contains("BocParsingError") || e.contains("BoC deserialization error"),
                    "Expected BoC deserialization error, got: {e}",
                );
            }
            _other => panic!("Expected GenericError with BoC parsing issue"),
        }
    }

    #[tokio::test]
    async fn test_broadcast_execute_message() {
        let mut client = MockRestClient::new();
        client.expect_post_v3_message().returning(|_| {
            Ok(V3MessageResponse {
                message_hash: "abc".to_string(),
                message_hash_norm: "ABC".to_string(),
            })
        });

        let wallet = load_wallets().await;
        let query_id_wrapper = MockQueryIdWrapper;
        let gateway_address = TonAddress::from_str(
            "0:0000000000000000000000000000000000000000000000000000000000000000",
        )
        .unwrap();
        let gas_service_address = TonAddress::from_str(
            "0:0000000000000000000000000000000000000000000000000000000000000fff",
        )
        .unwrap();

        let mut gas_estimator = MockGasEstimator::new();
        gas_estimator.expect_execute_estimate().returning(|_| 42u64);
        gas_estimator.expect_execute_send().returning(|_| 42u64);
        gas_estimator
            .expect_highload_wallet_send()
            .returning(|_| 1024u64);

        let broadcaster = SolanaBroadcaster {
            wallet: Arc::new(wallet),
            query_id_wrapper: Arc::new(query_id_wrapper),
            client: Arc::new(client),
            gateway_address,
            gas_service_address,
            chain_name: "ton2".to_string(),
            gas_estimator,
        };

        let execute_task = ExecuteTaskFields {
            message: GatewayV2Message {
                message_id: "0xf38d2a646e4b60e37bc16d54bb9163739372594dc96bab954a85b4a170f49e58-1".to_string(),
                source_chain: "avalanche-fuji".to_string(),
                destination_address: "0:b87a4a0f644b7a186ee71a1454634f70c22a62aca1a6ba676b5175c21d7fd930".to_string(),
                source_address: "ton2".to_string(),
                payload_hash: "aea6524367000fb4a0aa20b1d4f63daad1ed9e9df70=".to_string()
            },
            payload: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAE0hlbGxvIGZyb20gcmVsYXllciEAAAAAAAAAAAAAAAAA".to_string(),
            available_gas_balance: Amount { token_id: None, amount: "84".to_string() },
        };

        let res = broadcaster.broadcast_execute_message(execute_task).await;
        assert!(res.is_ok());

        let good = BroadcastResult {
            transaction: SolanaTransaction,
            tx_hash: "abc".to_string(),
            message_id: Some(
                "0xf38d2a646e4b60e37bc16d54bb9163739372594dc96bab954a85b4a170f49e58-1".to_string(),
            ),
            source_chain: Some("avalanche-fuji".to_string()),
            status: Ok(()),
        };

        let unwrapped = res.unwrap();

        assert!(unwrapped.status.is_ok());
        assert_eq!(unwrapped.tx_hash, good.tx_hash);
        assert_eq!(unwrapped.message_id, good.message_id);
        assert_eq!(unwrapped.source_chain, good.source_chain);
    }

    #[tokio::test]
    async fn test_broadcast_execute_message_not_enough_gas() {
        let mut client = MockRestClient::new();
        client.expect_post_v3_message().returning(|_| {
            Ok(V3MessageResponse {
                message_hash: "abc".to_string(),
                message_hash_norm: "ABC".to_string(),
            })
        });

        let wallet = load_wallets().await;
        let query_id_wrapper = MockQueryIdWrapper;
        let gateway_address = TonAddress::from_str(
            "0:0000000000000000000000000000000000000000000000000000000000000000",
        )
        .unwrap();
        let gas_service_address = TonAddress::from_str(
            "0:0000000000000000000000000000000000000000000000000000000000000fff",
        )
        .unwrap();

        let mut gas_estimator = MockGasEstimator::new();
        gas_estimator.expect_execute_estimate().returning(|_| 42u64);
        gas_estimator
            .expect_highload_wallet_send()
            .returning(|_| 1024u64);

        let broadcaster = SolanaBroadcaster {
            wallet: Arc::new(wallet),
            query_id_wrapper: Arc::new(query_id_wrapper),
            client: Arc::new(client),
            gateway_address,
            gas_service_address,
            chain_name: "ton2".to_string(),
            gas_estimator,
        };

        let execute_task = ExecuteTaskFields {
            message: GatewayV2Message {
                message_id: "0xf38d2a646e4b60e37bc16d54bb9163739372594dc96bab954a85b4a170f49e58-1".to_string(),
                source_chain: "avalanche-fuji".to_string(),
                destination_address: "0:b87a4a0f644b7a186ee71a1454634f70c22a62aca1a6ba676b5175c21d7fd930".to_string(),
                source_address: "ton2".to_string(),
                payload_hash: "aea6524367000fb4a0aa20b1d4f63daad1ed9e9df70=".to_string()
            },
            payload: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAE0hlbGxvIGZyb20gcmVsYXllciEAAAAAAAAAAAAAAAAA".to_string(),
            available_gas_balance: Amount { token_id: None, amount: "11".to_string() },
        };

        let res = broadcaster.broadcast_execute_message(execute_task).await;
        assert!(res.is_ok());

        let unwrapped = res.unwrap();

        assert!(unwrapped.status.is_err());
        assert_eq!(
            unwrapped.status.err().unwrap().to_string(),
            "Insufficient gas: Cannot proceed to execute"
        );
    }

    #[tokio::test]
    async fn test_broadcast_refund_message() {
        let mut client = MockRestClient::new();
        client
            .expect_post_v3_message()
            .withf(|boc| {
                let cell = Cell::from_boc_b64(boc);
                cell.is_ok()
            })
            .returning(|_| {
                Ok(V3MessageResponse {
                    message_hash: "abc".to_string(),
                    message_hash_norm: "ABC".to_string(),
                })
            });

        let wallet = load_wallets().await;
        let query_id_wrapper = MockQueryIdWrapper;
        let gateway_address = TonAddress::from_str(
            "0:0000000000000000000000000000000000000000000000000000000000000000",
        )
        .unwrap();
        let gas_service_address = TonAddress::from_str(
            "0:0000000000000000000000000000000000000000000000000000000000000fff",
        )
        .unwrap();

        let mut gas_estimator = MockGasEstimator::new();
        gas_estimator
            .expect_native_gas_refund_estimate()
            .returning(|| 42u64);
        gas_estimator
            .expect_highload_wallet_send()
            .returning(|_| 1024u64);

        let broadcaster = TONBroadcaster {
            wallet: Arc::new(wallet),
            query_id_wrapper: Arc::new(query_id_wrapper),
            client: Arc::new(client),
            gateway_address,
            gas_service_address,
            chain_name: "ton2".to_string(),
            gas_estimator,
        };

        let refund_task = refund_task();

        let res = broadcaster.broadcast_refund_message(refund_task).await;
        assert!(res.is_ok());

        let unwrapped = res.unwrap();

        assert_eq!(unwrapped, "abc");
    }

    #[tokio::test]
    async fn test_broadcast_refund_message_refund_too_big() {
        let client = mock_rest_client();

        let wallet = load_wallets().await;
        let query_id_wrapper = MockQueryIdWrapper;
        let gateway_address = TonAddress::from_str(
            "0:0000000000000000000000000000000000000000000000000000000000000000",
        )
        .unwrap();
        let gas_service_address = TonAddress::from_str(
            "0:0000000000000000000000000000000000000000000000000000000000000fff",
        )
        .unwrap();

        let mut gas_estimator = MockGasEstimator::new();
        gas_estimator
            .expect_native_gas_refund_estimate()
            .returning(|| 1000u64);
        gas_estimator
            .expect_highload_wallet_send()
            .returning(|_| 1000u64);

        let broadcaster = TONBroadcaster {
            wallet: Arc::new(wallet),
            query_id_wrapper: Arc::new(query_id_wrapper),
            client: Arc::new(client),
            gateway_address,
            gas_service_address,
            chain_name: "ton2".to_string(),
            gas_estimator,
        };

        let refund_task = refund_task();

        let res = broadcaster.broadcast_refund_message(refund_task).await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_send_to_chain_retry_on_throwif_36() {
        use crate::out_action::out_action;
        use base64::prelude::BASE64_STANDARD;
        use base64::Engine;
        use hex;
        use num_bigint::BigUint;
        use relayer_core::error::ClientError;
        use std::sync::atomic::{AtomicU32, Ordering};
        use std::sync::{Arc, Mutex};

        struct TestQueryIdWrapper {
            // It allows us to keep track easily without making &self mutable.
            // The execution is sequential though.
            force_shift_increase_count: Arc<AtomicU32>,
        }

        #[async_trait::async_trait]
        impl HighLoadQueryIdWrapper for TestQueryIdWrapper {
            async fn next(
                &self,
                _address: &str,
                _timeout: u64,
                force_shift_increase: bool,
            ) -> Result<HighLoadQueryId, HighLoadQueryIdWrapperError> {
                if force_shift_increase {
                    self.force_shift_increase_count
                        .fetch_add(1, Ordering::SeqCst);
                }
                Ok(HighLoadQueryId::from_shift_and_bitnumber(0u32, 0u32)
                    .await
                    .unwrap())
            }
        }

        let call_count = Arc::new(Mutex::new(0));
        let call_count_clone = Arc::clone(&call_count);

        let mut client = MockRestClient::new();
        client.expect_post_v3_message().returning(move |_| {
            let mut count = call_count_clone.lock().unwrap();
            *count += 1;
            if *count <= 5 {
                // First call fails with THROWIF 36
                Err(ClientError::BadResponse(
                    "THROWIF 36 error occurred".to_string(),
                ))
            } else {
                // Subsequent calls succeed
                Ok(V3MessageResponse {
                    message_hash: "abc".to_string(),
                    message_hash_norm: "ABC".to_string(),
                })
            }
        });

        let wallet = load_wallets().await;

        let force_shift_increase_count = Arc::new(AtomicU32::new(0));

        let query_id_wrapper = TestQueryIdWrapper {
            force_shift_increase_count: Arc::clone(&force_shift_increase_count),
        };

        let gateway_address = TonAddress::from_str(
            "0:0000000000000000000000000000000000000000000000000000000000000000",
        )
        .unwrap();

        let gas_service_address = TonAddress::from_str(
            "0:0000000000000000000000000000000000000000000000000000000000000fff",
        )
        .unwrap();

        let mut gas_estimator = MockGasEstimator::new();
        gas_estimator
            .expect_highload_wallet_send()
            .returning(|_| 1024u64);

        let broadcaster = TONBroadcaster {
            wallet: Arc::new(wallet),
            query_id_wrapper: Arc::new(query_id_wrapper),
            client: Arc::new(client),
            gateway_address,
            gas_service_address,
            chain_name: "ton2".to_string(),
            gas_estimator,
        };

        let wallet = broadcaster.wallet.acquire().await.unwrap();
        let approve_message = hex::encode(BASE64_STANDARD.decode("te6cckECDAEAAYsAAggAAAAoAQIBYYAAAAAAAAAAAAAAAAAAAACAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAADf5gkADAQHABADi0LAAUYmshNOh1nWEdwB3eJHd51H6EH1kg3v2M30y32eQAAAAAAAAAAAAAAAAAAAAAQ+j+g0KWjWTaPqB9qQHuWZQn7IPz7x3xzwbprT1a85sjh0UlPlFU84LDdRcD4GZ6n6GJlEKKTlRW5QtlzKGrAsBAtAFBECeAcQjykQMXsK+7MnQoVK1T8jnpBbJMbcInq8iFgWvFwYHCAkAiDB4MTdmZDdkYTNkODE5Y2ZiYzQ2ZmYyOGYzZDgwOTgwNzcwZWMxYjgwZmQ3ZDFiMjI5Y2VjMzI1MTkzOWI5YjIzZi0xABxhdmFsYW5jaGUtZnVqaQBUMHhkNzA2N0FlM0MzNTllODM3ODkwYjI4QjdCRDBkMjA4NENmRGY0OWI1AgAKCwBAuHpKD2RLehhu5xoUVGNPcMIqYqyhprpna1F1wh1/2TAACHRvbjJLddsV").unwrap());
        let actions = vec![out_action(
            &approve_message,
            BigUint::from(100u32),
            broadcaster.gateway_address.clone(),
        )
        .unwrap()];
        let result = broadcaster.send_to_chain(wallet, actions, None).await;
        broadcaster.wallet.release(wallet).await;
        assert!(result.is_ok());
        assert_eq!(*call_count.lock().unwrap(), 6);
        assert_eq!(force_shift_increase_count.load(Ordering::SeqCst), 5);
    }

    fn mock_rest_client() -> MockRestClient {
        let mut client = MockRestClient::new();
        client
            .expect_post_v3_message()
            .withf(|boc| {
                let cell = Cell::from_boc_b64(boc);
                cell.is_ok()
            })
            .returning(|_| {
                Ok(V3MessageResponse {
                    message_hash: "abc".to_string(),
                    message_hash_norm: "ABC".to_string(),
                })
            });
        client
    }

    fn refund_task() -> RefundTaskFields {
        RefundTaskFields {
            message: GatewayV2Message {
                message_id: "0xf38d2a646e4b60e37bc16d54bb9163739372594dc96bab954a85b4a170f49e58"
                    .to_string(),
                source_chain: "avalanche-fuji".to_string(),
                destination_address:
                    "0:b87a4a0f644b7a186ee71a1454634f70c22a62aca1a6ba676b5175c21d7fd930".to_string(),
                source_address: "ton2".to_string(),
                payload_hash: "aea6524367000fb4a0aa20b1d4f63daad1ed9e9df70=".to_string(),
            },
            refund_recipient_address:
                "0:e1e633eb701b118b44297716cee7069ee847b56db88c497efea681ed14b2d2c7".to_string(),
            remaining_gas_balance: Amount {
                token_id: None,
                amount: "42".to_string(),
            },
        }
    }
}
