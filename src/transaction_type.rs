use solana_sdk::compute_budget::ComputeBudgetInstruction;
use solana_sdk::signature::Signature;
use solana_sdk::transaction::{Transaction, VersionedTransaction};

#[derive(Clone)]
pub enum SolanaTransactionType {
    Legacy(Transaction),
    Versioned(VersionedTransaction),
}

impl SolanaTransactionType {
    pub fn get_potential_micro_priority_price(&self) -> u64 {
        match self {
            SolanaTransactionType::Legacy(tx) => {
                let mut micro_price: u64 = 0;
                for ix in &tx.message.instructions {
                    if ix.program_id(&tx.message.account_keys) == &solana_sdk::compute_budget::id()
                    {
                        if let Ok(ComputeBudgetInstruction::SetComputeUnitPrice(p)) =
                            bincode::deserialize(&ix.data)
                        {
                            micro_price = p;
                        }
                    }
                }
                micro_price
            }
            SolanaTransactionType::Versioned(tx) => {
                let mut micro_price: u64 = 0;
                for ix in tx.message.instructions() {
                    if let Some(program_id) = tx
                        .message
                        .static_account_keys()
                        .get(ix.program_id_index as usize)
                    {
                        if program_id == &solana_sdk::compute_budget::id() {
                            if let Ok(ComputeBudgetInstruction::SetComputeUnitPrice(p)) =
                                bincode::deserialize(&ix.data)
                            {
                                micro_price = p;
                            }
                        }
                    }
                }
                micro_price
            }
        }
    }

    pub fn get_num_required_signatures(&self) -> u64 {
        match self {
            SolanaTransactionType::Legacy(tx) => tx.message.header.num_required_signatures as u64,
            SolanaTransactionType::Versioned(tx) => {
                tx.message.header().num_required_signatures as u64
            }
        }
    }

    pub fn get_signature(&self) -> Option<&Signature> {
        match self {
            SolanaTransactionType::Legacy(tx) => tx.signatures.first(),
            SolanaTransactionType::Versioned(tx) => tx.signatures.first(),
        }
    }
}
