#[cfg(any(test, feature = "test-utils"))]
pub mod fixtures {
    use std::fs;

    use solana_transaction_status::EncodedConfirmedTransactionWithStatusMeta;

    use crate::types::{RpcGetTransactionResponse, SolanaTransaction};

    pub fn transaction_fixtures() -> Vec<SolanaTransaction> {
        let file_path = "tests/testdata/transactions/solana_transaction.json";
        let body = fs::read_to_string(file_path).expect("Failed to read JSON test file");
        serde_json::from_str::<Vec<SolanaTransaction>>(&body)
            .expect("Failed to deserialize SolanaTransaction array")
    }

    pub fn rpc_response_fixtures() -> Vec<RpcGetTransactionResponse> {
        let file_path = "tests/testdata/rpc_batch_repsonse.json";
        let body = fs::read_to_string(file_path).expect("Failed to read JSON test file");
        serde_json::from_str::<Vec<RpcGetTransactionResponse>>(&body)
            .expect("Failed to deserialize RpcGetTransactionResponse array")
    }

    pub fn encoded_confirmed_tx_with_meta_fixtures(
    ) -> Vec<EncodedConfirmedTransactionWithStatusMeta> {
        let file_path = "tests/testdata/transactions/encoded_confirmed_tx_with_meta.json";
        let body = fs::read_to_string(file_path).expect("Failed to read JSON test file");
        serde_json::from_str::<Vec<EncodedConfirmedTransactionWithStatusMeta>>(&body)
            .expect("Failed to deserialize EncodedConfirmedTransactionWithStatusMeta array")
    }
}
