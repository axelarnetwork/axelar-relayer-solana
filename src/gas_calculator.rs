// Estimates the gas required to make a transaction on Solana
// https://solana.com/developers/guides/advanced/exchange
// Read about prioritization fees in the corresponding section in the guide
// Updated to support configurable percentiles

use crate::error::GasCalculatorError;
use crate::includer_client::IncluderClientTrait;
use crate::transaction_type::SolanaTransactionType;
use async_trait::async_trait;
use relayer_core::utils::ThreadSafe;

#[derive(Clone)]
pub struct GasCalculator<IC: IncluderClientTrait> {
    includer_client: IC,
}

impl<IC: IncluderClientTrait> GasCalculator<IC> {
    pub fn new(includer_client: IC) -> Self {
        Self { includer_client }
    }
}

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait GasCalculatorTrait: ThreadSafe {
    async fn compute_budget(&self, tx: SolanaTransactionType) -> Result<u64, GasCalculatorError>;
}

#[async_trait]
impl<IC: IncluderClientTrait> GasCalculatorTrait for GasCalculator<IC> {
    async fn compute_budget(&self, tx: SolanaTransactionType) -> Result<u64, GasCalculatorError> {
        const PERCENT_POINTS_TO_TOP_UP: u64 = 25;

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
}
