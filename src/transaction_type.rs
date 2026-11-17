use solana_sdk::signature::Signature;
use solana_sdk::transaction::{Transaction, VersionedTransaction};

#[derive(Clone)]
pub enum SolanaTransactionType {
    Legacy(Transaction),
    Versioned(VersionedTransaction),
}

impl SolanaTransactionType {
    pub fn get_num_required_signatures(&self) -> u64 {
        match self {
            SolanaTransactionType::Legacy(tx) => tx.message.header.num_required_signatures as u64,
            SolanaTransactionType::Versioned(tx) => {
                tx.message.header().num_required_signatures as u64
            }
        }
    }

    pub fn get_num_instructions(&self) -> u64 {
        match self {
            SolanaTransactionType::Legacy(tx) => tx.message.instructions.len() as u64,
            SolanaTransactionType::Versioned(tx) => tx.message.instructions().len() as u64,
        }
    }

    pub fn get_signature(&self) -> Option<&Signature> {
        match self {
            SolanaTransactionType::Legacy(tx) => tx.signatures.first(),
            SolanaTransactionType::Versioned(tx) => tx.signatures.first(),
        }
    }
}
