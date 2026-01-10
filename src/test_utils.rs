#[cfg(test)]
pub mod fixtures {
    use crate::types::{RpcGetTransactionResponse, SolanaTransaction};
    use solana_transaction_status::EncodedConfirmedTransactionWithStatusMeta;

    pub fn transaction_fixtures() -> Vec<SolanaTransaction> {
        let body = include_str!("../tests/testdata/transactions/solana_transaction.json");
        serde_json::from_str::<Vec<SolanaTransaction>>(body)
            .expect("Failed to deserialize SolanaTransaction array")
    }

    pub fn rpc_response_fixtures() -> Vec<RpcGetTransactionResponse> {
        let body = include_str!("../tests/testdata/rpc_batch_response.json");
        serde_json::from_str::<Vec<RpcGetTransactionResponse>>(body)
            .expect("Failed to deserialize RpcGetTransactionResponse array")
    }

    pub fn encoded_confirmed_tx_with_meta_fixtures(
    ) -> Vec<EncodedConfirmedTransactionWithStatusMeta> {
        let body =
            include_str!("../tests/testdata/transactions/encoded_confirmed_tx_with_meta.json");
        serde_json::from_str::<Vec<EncodedConfirmedTransactionWithStatusMeta>>(body)
            .expect("Failed to deserialize EncodedConfirmedTransactionWithStatusMeta array")
    }
}
