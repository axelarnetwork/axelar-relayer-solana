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

    /// A real devnet v1 transaction (gateway `call_contract` with a 2400-byte payload plus
    /// gas-service `pay_gas`, 3032 bytes serialized) as returned by `getTransaction`.
    pub fn v1_rpc_response_fixture() -> RpcGetTransactionResponse {
        let body = include_str!("../tests/testdata/transactions/v1_transaction.json");
        serde_json::from_str::<RpcGetTransactionResponse>(body)
            .expect("Failed to deserialize v1 RpcGetTransactionResponse")
    }

    pub fn encoded_confirmed_tx_with_meta_fixtures(
    ) -> Vec<EncodedConfirmedTransactionWithStatusMeta> {
        let body =
            include_str!("../tests/testdata/transactions/encoded_confirmed_tx_with_meta.json");
        serde_json::from_str::<Vec<EncodedConfirmedTransactionWithStatusMeta>>(body)
            .expect("Failed to deserialize EncodedConfirmedTransactionWithStatusMeta array")
    }
}
