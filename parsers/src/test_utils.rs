#[cfg(test)]
pub mod fixtures {
    use crate::types::SolanaTransaction;

    pub fn transaction_fixtures() -> Vec<SolanaTransaction> {
        let body = include_str!("../../tests/testdata/transactions/solana_transaction.json");
        serde_json::from_str::<Vec<SolanaTransaction>>(body)
            .expect("Failed to deserialize SolanaTransaction array")
    }
}
