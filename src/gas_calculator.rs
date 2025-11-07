// Estimates the gas required to make a transaction on Solana
// https://solana.com/developers/guides/advanced/exchange
// Read about prioritization fees in the corresponding section in the guide

use crate::error::GasCalculatorError;
use crate::includer_client::IncluderClientTrait;
use crate::transaction_type::SolanaTransactionType;
use async_trait::async_trait;
use relayer_core::utils::ThreadSafe;
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use solana_sdk::hash::Hash;
use solana_sdk::instruction::Instruction;
use solana_sdk::signer::keypair::Keypair;
use solana_sdk::signer::Signer;
use solana_sdk::transaction::Transaction;
use std::sync::Arc;

#[derive(Clone)]
pub struct GasCalculator<IC: IncluderClientTrait> {
    includer_client: IC,
    solana_keypair: Arc<Keypair>,
}

impl<IC: IncluderClientTrait> GasCalculator<IC> {
    pub fn new(includer_client: IC, solana_keypair: Arc<Keypair>) -> Self {
        Self {
            includer_client,
            solana_keypair,
        }
    }
}

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait GasCalculatorTrait: ThreadSafe {
    async fn compute_budget(
        &self,
        ixs: &[Instruction],
    ) -> Result<(Instruction, Hash), GasCalculatorError>;
    async fn compute_unit_price(
        &self,
        ixs: &[Instruction],
    ) -> Result<Instruction, GasCalculatorError>;
}

#[async_trait]
impl<IC: IncluderClientTrait> GasCalculatorTrait for GasCalculator<IC> {
    async fn compute_budget(
        &self,
        ixs: &[Instruction],
    ) -> Result<(Instruction, Hash), GasCalculatorError> {
        const PERCENT_POINTS_TO_TOP_UP: u64 = 10;

        let hash = self
            .includer_client
            .get_latest_blockhash()
            .await
            .map_err(|e| GasCalculatorError::Generic(e.to_string()))?;
        let tx_to_simulate = Transaction::new_signed_with_payer(
            ixs,
            Some(&self.solana_keypair.pubkey()),
            &[&self.solana_keypair],
            hash,
        );
        let computed_units = self
            .includer_client
            .get_units_consumed_from_simulation(SolanaTransactionType::Legacy(tx_to_simulate))
            .await
            .map_err(|e| GasCalculatorError::Generic(e.to_string()))?;

        let safety_margin = computed_units
            .saturating_mul(PERCENT_POINTS_TO_TOP_UP)
            .saturating_div(100);
        let compute_budget = computed_units.saturating_add(safety_margin);

        let ix =
            ComputeBudgetInstruction::set_compute_unit_limit(compute_budget.try_into().map_err(
                |e: std::num::TryFromIntError| GasCalculatorError::Generic(e.to_string()),
            )?);
        Ok((ix, hash))
    }

    async fn compute_unit_price(
        &self,
        ixs: &[Instruction],
    ) -> Result<Instruction, GasCalculatorError> {
        const MAX_ACCOUNTS: usize = 128;
        const N_SLOTS_TO_CHECK: usize = 10;

        let all_touched_accounts = ixs
            .iter()
            .flat_map(|x| x.accounts.as_slice())
            .take(MAX_ACCOUNTS)
            .map(|x| x.pubkey)
            .collect::<Vec<_>>();
        let fees = self
            .includer_client
            .get_recent_prioritization_fees(&all_touched_accounts)
            .await
            .map_err(|e| GasCalculatorError::Generic(e.to_string()))?;
        let (sum, count) = fees
            .into_iter()
            .rev()
            .take(N_SLOTS_TO_CHECK)
            .map(|x| x.prioritization_fee)
            // Simple rolling average of the last `N_SLOTS_TO_CHECK` items.
            .fold((0_u64, 0_u64), |(sum, count), fee| {
                (sum.saturating_add(fee), count.saturating_add(1))
            });
        let average = if count > 0 {
            sum.saturating_div(count)
        } else {
            0
        };

        Ok(ComputeBudgetInstruction::set_compute_unit_price(average))
    }
}
