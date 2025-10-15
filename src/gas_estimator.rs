// Estimates the gas required to make a transaction on Solana
// https://solana.com/developers/guides/advanced/exchange
// Read about prioritization fees in the corresponding section in the guide

use crate::error::GasEstimationError;
use crate::includer_client::IncluderClientTrait;
use async_trait::async_trait;
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use solana_sdk::hash::Hash;
use solana_sdk::instruction::Instruction;
use solana_sdk::signer::keypair::Keypair;
use solana_sdk::signer::Signer;
use solana_sdk::transaction::Transaction;
use std::sync::Arc;

pub const MAX_COMPUTE_UNIT_LIMIT: u32 = 1_399_850;

#[derive(Clone)]
pub struct GasEstimator<IC: IncluderClientTrait> {
    includer_client: IC,
    solana_keypair: Arc<Keypair>,
}

impl<IC: IncluderClientTrait> GasEstimator<IC> {
    pub fn new(includer_client: IC, solana_keypair: Arc<Keypair>) -> Self {
        Self {
            includer_client,
            solana_keypair,
        }
    }
}

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait GasEstimatorTrait {
    async fn compute_budget(
        &self,
        ixs: &[Instruction],
    ) -> Result<(Instruction, Hash), GasEstimationError>;
    async fn compute_unit_price(
        &self,
        ixs: &[Instruction],
    ) -> Result<Instruction, GasEstimationError>;
}

#[async_trait]
impl<IC: IncluderClientTrait> GasEstimatorTrait for GasEstimator<IC> {
    async fn compute_budget(
        &self,
        ixs: &[Instruction],
    ) -> Result<(Instruction, Hash), GasEstimationError> {
        const PERCENT_POINTS_TO_TOP_UP: u64 = 10;

        let hash = self
            .includer_client
            .inner()
            .get_latest_blockhash()
            .await
            .map_err(|e| GasEstimationError::Generic(e.to_string()))?;
        let tx_to_simulate = Transaction::new_signed_with_payer(
            ixs,
            Some(&self.solana_keypair.pubkey()),
            &[&self.solana_keypair],
            hash,
        );
        let simulation_result = self
            .includer_client
            .inner()
            .simulate_transaction(&tx_to_simulate)
            .await
            .map_err(|e| GasEstimationError::Generic(e.to_string()))?;
        if let Some(err) = simulation_result.value.err {
            return Err(GasEstimationError::Generic(format!(
                "Simulation error: {:?}",
                err
            )));
        }
        let computed_units = simulation_result.value.units_consumed.unwrap_or(0);
        let top_up = computed_units
            .checked_div(PERCENT_POINTS_TO_TOP_UP)
            .unwrap_or(0);
        let compute_budget = computed_units.saturating_add(top_up);
        let compute_budget = compute_budget.min(u64::from(MAX_COMPUTE_UNIT_LIMIT)); // Safe conversion since we move from an u32 to u64
        let ix =
            ComputeBudgetInstruction::set_compute_unit_limit(compute_budget.try_into().map_err(
                |e: std::num::TryFromIntError| GasEstimationError::Generic(e.to_string()),
            )?);
        Ok((ix, hash))
    }

    async fn compute_unit_price(
        &self,
        ixs: &[Instruction],
    ) -> Result<Instruction, GasEstimationError> {
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
            .inner()
            .get_recent_prioritization_fees(&all_touched_accounts)
            .await
            .map_err(|e| GasEstimationError::Generic(e.to_string()))?;
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
            sum.checked_div(count).unwrap_or(0)
        } else {
            0
        };
        Ok(ComputeBudgetInstruction::set_compute_unit_price(average))
    }
}
