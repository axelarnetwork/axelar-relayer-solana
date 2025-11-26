// Estimates the gas required to make a transaction on Solana
// https://solana.com/developers/guides/advanced/exchange
// Read about prioritization fees in the corresponding section in the guide
// Updated to support configurable percentiles

use crate::error::GasCalculatorError;
use crate::fees_client::FeesClientTrait;
use crate::includer_client::IncluderClientTrait;
use crate::transaction_type::SolanaTransactionType;
use async_trait::async_trait;
use relayer_core::utils::ThreadSafe;
use solana_sdk::instruction::Instruction;
use tracing::debug;

#[derive(Clone)]
pub struct GasCalculator<IC: IncluderClientTrait, FC: FeesClientTrait> {
    includer_client: IC,
    fees_client: FC,
}

impl<IC: IncluderClientTrait, FC: FeesClientTrait> GasCalculator<IC, FC> {
    pub fn new(includer_client: IC, fees_client: FC) -> Self {
        Self {
            includer_client,
            fees_client,
        }
    }
}

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait GasCalculatorTrait: ThreadSafe {
    async fn compute_budget(&self, tx: SolanaTransactionType) -> Result<u64, GasCalculatorError>;
    async fn compute_unit_price(
        &self,
        ixs: &[Instruction],
        percentile: f64,
    ) -> Result<u64, GasCalculatorError>;
}

#[async_trait]
impl<IC: IncluderClientTrait, FC: FeesClientTrait> GasCalculatorTrait for GasCalculator<IC, FC> {
    async fn compute_budget(&self, tx: SolanaTransactionType) -> Result<u64, GasCalculatorError> {
        const PERCENT_POINTS_TO_TOP_UP: u64 = 20;

        let computed_units = self
            .includer_client
            .get_units_consumed_from_simulation(tx)
            .await
            .map_err(|e| GasCalculatorError::Generic(e.to_string()))?;

        let safety_margin = computed_units
            .saturating_mul(PERCENT_POINTS_TO_TOP_UP)
            .saturating_div(100);

        Ok(computed_units.saturating_add(safety_margin))
    }

    async fn compute_unit_price(
        &self,
        ixs: &[Instruction],
        percentile: f64,
    ) -> Result<u64, GasCalculatorError> {
        const MAX_ACCOUNTS: usize = 128;

        let all_touched_accounts = ixs
            .iter()
            .flat_map(|x| x.accounts.as_slice())
            .take(MAX_ACCOUNTS)
            .map(|x| x.pubkey)
            .collect::<Vec<_>>();

        let fees = self
            .fees_client
            .get_recent_prioritization_fees(&all_touched_accounts, percentile)
            .await;

        debug!("Got prioritization fees: {}", fees);

        Ok(fees)
    }
}
