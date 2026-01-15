use solana_sdk::signature::Signature;
use solana_sdk::transaction::{Transaction, VersionedTransaction};

#[derive(Clone)]
pub enum SolanaTransactionType {
    Legacy(Transaction),
    Versioned(VersionedTransaction),
}

impl SolanaTransactionType {
    pub fn get_potential_micro_priority_price(&self) -> u64 {
        // Extract the compute unit price from a ComputeBudgetInstruction's serialized data.
        // Unable to use ComputeBudgetInstruction::try_from_slice() because of multiple borsh versions in the dependency tree.
        fn extract_price_from_ix_data(data: &[u8]) -> Option<u64> {
            let (discriminant, rest) = data.split_first()?;
            if *discriminant != 3 || rest.len() != 8 {
                return None;
            }

            let mut price_bytes = [0u8; 8];
            price_bytes.copy_from_slice(rest);
            Some(u64::from_le_bytes(price_bytes))
        }

        match self {
            SolanaTransactionType::Legacy(tx) => {
                let mut micro_price: u64 = 0;
                for ix in &tx.message.instructions {
                    if ix.program_id(&tx.message.account_keys)
                        == &solana_compute_budget_interface::id()
                    {
                        if let Some(p) = extract_price_from_ix_data(&ix.data) {
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
                        if program_id == &solana_compute_budget_interface::id() {
                            if let Some(p) = extract_price_from_ix_data(&ix.data) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use solana_compute_budget_interface::ComputeBudgetInstruction;
    use solana_sdk::hash::Hash;
    use solana_sdk::instruction::Instruction;
    use solana_sdk::signer::{keypair::Keypair, Signer};

    #[test]
    fn test_get_potential_micro_priority_price_with_compute_budget() {
        let keypair = Keypair::new();
        let price = 1_000_000u64;

        // Create a transaction with compute budget instruction
        let tx = Transaction::new_signed_with_payer(
            &[ComputeBudgetInstruction::set_compute_unit_price(price)],
            Some(&keypair.pubkey()),
            &[&keypair],
            Hash::default(),
        );

        let tx_type = SolanaTransactionType::Legacy(tx);
        assert_eq!(tx_type.get_potential_micro_priority_price(), price);
    }

    #[test]
    fn test_get_potential_micro_priority_price_without_compute_budget() {
        let keypair = Keypair::new();

        // Create a transaction without compute budget instruction
        let tx = Transaction::new_signed_with_payer(
            &[Instruction::new_with_bytes(
                solana_sdk_ids::system_program::ID,
                &[],
                vec![],
            )],
            Some(&keypair.pubkey()),
            &[&keypair],
            Hash::default(),
        );

        let tx_type = SolanaTransactionType::Legacy(tx);
        assert_eq!(tx_type.get_potential_micro_priority_price(), 0);
    }

    #[test]
    fn test_get_potential_micro_priority_price_zero_price() {
        let keypair = Keypair::new();
        let price = 0u64;

        let tx = Transaction::new_signed_with_payer(
            &[ComputeBudgetInstruction::set_compute_unit_price(price)],
            Some(&keypair.pubkey()),
            &[&keypair],
            Hash::default(),
        );

        let tx_type = SolanaTransactionType::Legacy(tx);
        assert_eq!(tx_type.get_potential_micro_priority_price(), price);
    }

    #[test]
    fn test_get_potential_micro_priority_price_very_high_price() {
        let keypair = Keypair::new();
        let price = 2_000_000_000_000u64; // 2 trillion micro-lamports

        let tx = Transaction::new_signed_with_payer(
            &[ComputeBudgetInstruction::set_compute_unit_price(price)],
            Some(&keypair.pubkey()),
            &[&keypair],
            Hash::default(),
        );

        let tx_type = SolanaTransactionType::Legacy(tx);
        assert_eq!(tx_type.get_potential_micro_priority_price(), price);
    }

    #[test]
    fn test_get_potential_micro_priority_price_max_u64() {
        let keypair = Keypair::new();
        let price = u64::MAX;

        let tx = Transaction::new_signed_with_payer(
            &[ComputeBudgetInstruction::set_compute_unit_price(price)],
            Some(&keypair.pubkey()),
            &[&keypair],
            Hash::default(),
        );

        let tx_type = SolanaTransactionType::Legacy(tx);
        assert_eq!(tx_type.get_potential_micro_priority_price(), price);
    }

    #[test]
    fn test_get_potential_micro_priority_price_multiple_instructions() {
        let keypair = Keypair::new();
        let price1 = 1_000_000u64;
        let price2 = 5_000_000u64;

        // Create a transaction with multiple compute budget instructions
        // The function should return the last one
        let tx = Transaction::new_signed_with_payer(
            &[
                ComputeBudgetInstruction::set_compute_unit_price(price1),
                Instruction::new_with_bytes(solana_sdk_ids::system_program::ID, &[], vec![]),
                ComputeBudgetInstruction::set_compute_unit_price(price2),
            ],
            Some(&keypair.pubkey()),
            &[&keypair],
            Hash::default(),
        );

        let tx_type = SolanaTransactionType::Legacy(tx);
        // Should return the last compute unit price instruction
        assert_eq!(tx_type.get_potential_micro_priority_price(), price2);
    }

    #[test]
    fn test_get_potential_micro_priority_price_with_other_compute_budget_instructions() {
        let keypair = Keypair::new();
        let price = 2_500_000u64;

        // Create a transaction with both compute limit and price instructions
        let tx = Transaction::new_signed_with_payer(
            &[
                ComputeBudgetInstruction::set_compute_unit_limit(400_000),
                ComputeBudgetInstruction::set_compute_unit_price(price),
            ],
            Some(&keypair.pubkey()),
            &[&keypair],
            Hash::default(),
        );

        let tx_type = SolanaTransactionType::Legacy(tx);
        assert_eq!(tx_type.get_potential_micro_priority_price(), price);
    }

    #[test]
    fn test_get_potential_micro_priority_price_empty_transaction() {
        let keypair = Keypair::new();

        // Create a transaction with no instructions
        let tx = Transaction::new_signed_with_payer(
            &[],
            Some(&keypair.pubkey()),
            &[&keypair],
            Hash::default(),
        );

        let tx_type = SolanaTransactionType::Legacy(tx);
        assert_eq!(tx_type.get_potential_micro_priority_price(), 0);
    }

    #[test]
    fn test_get_potential_micro_priority_price_various_prices() {
        let keypair = Keypair::new();

        // Test various realistic price values
        let test_prices = vec![
            1u64,
            100u64,
            1_000u64,
            10_000u64,
            100_000u64,
            1_000_000u64,
            10_000_000u64,
            100_000_000u64,
            1_000_000_000u64,
            10_000_000_000u64,
        ];

        for price in test_prices {
            let tx = Transaction::new_signed_with_payer(
                &[ComputeBudgetInstruction::set_compute_unit_price(price)],
                Some(&keypair.pubkey()),
                &[&keypair],
                Hash::default(),
            );

            let tx_type = SolanaTransactionType::Legacy(tx);
            assert_eq!(
                tx_type.get_potential_micro_priority_price(),
                price,
                "Failed for price: {}",
                price
            );
        }
    }
}
