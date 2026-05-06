// Estimates the gas required to make a transaction on Solana
// https://solana.com/developers/guides/advanced/exchange
// Read about prioritization fees in the corresponding section in the guide
//
// Two paths exist depending on `InstructionKind`:
//   - hardcoded: protocol-known instructions (init session, verify signature) whose CU
//     consumption is deterministic on mainnet. We skip simulation entirely and return a
//     constant — see CU_HARDCODED_* below.
//   - simulated: instructions whose CU varies with payload/accounts. We simulate, take
//     `units_consumed`, add a 25% margin. On simulation error, we either fall back to a
//     conservative constant (ApproveMessage, AltCreateExtend) or propagate the error
//     (Execute, Other) — never silently return 0 like the previous implementation.

use crate::error::GasCalculatorError;
use crate::includer_client::IncluderClientTrait;
use crate::transaction_type::SolanaTransactionType;
use async_trait::async_trait;
use relayer_core::utils::ThreadSafe;
use tracing::{debug, error};

/// Margin added on top of simulated CU to absorb between-sim and on-chain state drift.
/// Mainnet stats show stable-state sim-vs-actual deltas of 0–+3 CU, but ApproveMessage
/// has ±4.2% payload-driven variance, and integration tests against a local validator
/// can produce additional drift beyond mainnet samples. 25% gives comfortable headroom
/// without inflating priority-fee cost meaningfully (since milliLamports × CU is small).
const PERCENT_POINTS_TO_TOP_UP: u64 = 25;

/// Hardcoded CU values for protocol-known instructions (mainnet-measured + ~10% buffer).
/// These do not vary by payload, so simulation is wasted work and adds a failure mode
/// (sim-during-state-race).
const CU_HARDCODED_INIT_PAYLOAD_VERIFICATION: u64 = 13_000;
const CU_HARDCODED_VERIFY_SIGNATURE: u64 = 220_000;

/// Conservative fallbacks for simulated instructions when the simulation itself errors.
/// Sized comfortably above mainnet-observed peaks so a one-off sim failure doesn't block
/// inclusion.
const CU_FALLBACK_APPROVE_MESSAGE: u64 = 80_000;
const CU_FALLBACK_ALT_CREATE_EXTEND: u64 = 20_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstructionKind {
    /// Gateway InitializePayloadVerificationSession — hardcoded.
    InitPayloadVerification,
    /// Gateway VerifySignature — hardcoded.
    VerifySignature,
    /// Gateway ApproveMessage — simulated; falls back to a constant on sim error.
    ApproveMessage,
    /// GMP / ITS / governance execute on the destination program — simulated, no fallback.
    /// Payload size and destination program vary too widely for a meaningful fallback.
    Execute,
    /// AddressLookupTable create+extend — simulated, falls back to a constant.
    AltCreateExtend,
    /// Anything else (rotate signers, refunds, gas service fee claims, etc.) — simulated,
    /// no fallback.
    Other,
}

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
    async fn compute_budget(
        &self,
        tx: SolanaTransactionType,
        kind: InstructionKind,
    ) -> Result<u64, GasCalculatorError>;
}

#[async_trait]
impl<IC: IncluderClientTrait> GasCalculatorTrait for GasCalculator<IC> {
    async fn compute_budget(
        &self,
        tx: SolanaTransactionType,
        kind: InstructionKind,
    ) -> Result<u64, GasCalculatorError> {
        match kind {
            InstructionKind::InitPayloadVerification => {
                debug!(
                    kind = ?kind,
                    cu = CU_HARDCODED_INIT_PAYLOAD_VERIFICATION,
                    "compute_budget: using hardcoded CU"
                );
                Ok(CU_HARDCODED_INIT_PAYLOAD_VERIFICATION)
            }
            InstructionKind::VerifySignature => {
                debug!(
                    kind = ?kind,
                    cu = CU_HARDCODED_VERIFY_SIGNATURE,
                    "compute_budget: using hardcoded CU"
                );
                Ok(CU_HARDCODED_VERIFY_SIGNATURE)
            }
            InstructionKind::ApproveMessage => {
                self.simulate_with_fallback(tx, kind, Some(CU_FALLBACK_APPROVE_MESSAGE))
                    .await
            }
            InstructionKind::AltCreateExtend => {
                self.simulate_with_fallback(tx, kind, Some(CU_FALLBACK_ALT_CREATE_EXTEND))
                    .await
            }
            InstructionKind::Execute | InstructionKind::Other => {
                self.simulate_with_fallback(tx, kind, None).await
            }
        }
    }
}

impl<IC: IncluderClientTrait> GasCalculator<IC> {
    /// Simulate the transaction; on success return `units_consumed × (1 + margin)`.
    /// On simulation error, log it and either return `fallback` (if provided) or surface
    /// the error so the includer can retry against fresh state. The fallback only fires
    /// on simulation failure — it is not a floor on a successful simulation.
    async fn simulate_with_fallback(
        &self,
        tx: SolanaTransactionType,
        kind: InstructionKind,
        fallback: Option<u64>,
    ) -> Result<u64, GasCalculatorError> {
        match self
            .includer_client
            .get_units_consumed_from_simulation(tx)
            .await
        {
            Ok(units) => {
                let safety_margin = units
                    .saturating_mul(PERCENT_POINTS_TO_TOP_UP)
                    .saturating_div(100);
                let final_cu = units.saturating_add(safety_margin);
                println!(
                    "[CU_DEBUG] kind={:?} simulated_units={} margin_pct={} final_cu={}",
                    kind, units, PERCENT_POINTS_TO_TOP_UP, final_cu
                );
                debug!(
                    kind = ?kind,
                    simulated_units = units,
                    margin_pct = PERCENT_POINTS_TO_TOP_UP,
                    final_cu,
                    "compute_budget: simulated"
                );
                Ok(final_cu)
            }
            Err(e) => match fallback {
                Some(cu) => {
                    error!(
                        kind = ?kind,
                        fallback_cu = cu,
                        error = %e,
                        "compute_budget: simulation failed, using fallback CU"
                    );
                    Ok(cu)
                }
                None => {
                    error!(
                        kind = ?kind,
                        error = %e,
                        "compute_budget: simulation failed and no fallback configured; propagating error"
                    );
                    Err(GasCalculatorError::Generic(e.to_string()))
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::IncluderClientError;
    use crate::includer_client::MockIncluderClientTrait;
    use solana_sdk::hash::Hash;
    use solana_sdk::signer::keypair::Keypair;
    use solana_sdk::signer::Signer as _;
    use solana_sdk::transaction::Transaction;

    fn dummy_tx() -> SolanaTransactionType {
        let kp = Keypair::new();
        let tx = Transaction::new_signed_with_payer(
            &[solana_sdk::instruction::Instruction::new_with_bytes(
                solana_sdk_ids::system_program::ID,
                &[],
                vec![],
            )],
            Some(&kp.pubkey()),
            &[&kp],
            Hash::default(),
        );
        SolanaTransactionType::Legacy(tx)
    }

    #[tokio::test]
    async fn init_payload_verification_is_hardcoded_and_skips_simulation() {
        let mut client = MockIncluderClientTrait::new();
        // Simulation must NOT be called for hardcoded kinds.
        client.expect_get_units_consumed_from_simulation().times(0);

        let calc = GasCalculator::new(client);
        let cu = calc
            .compute_budget(dummy_tx(), InstructionKind::InitPayloadVerification)
            .await
            .unwrap();
        assert_eq!(cu, CU_HARDCODED_INIT_PAYLOAD_VERIFICATION);
    }

    #[tokio::test]
    async fn verify_signature_is_hardcoded_and_skips_simulation() {
        let mut client = MockIncluderClientTrait::new();
        client.expect_get_units_consumed_from_simulation().times(0);

        let calc = GasCalculator::new(client);
        let cu = calc
            .compute_budget(dummy_tx(), InstructionKind::VerifySignature)
            .await
            .unwrap();
        assert_eq!(cu, CU_HARDCODED_VERIFY_SIGNATURE);
    }

    #[tokio::test]
    async fn approve_message_simulates_and_applies_margin() {
        let mut client = MockIncluderClientTrait::new();
        client
            .expect_get_units_consumed_from_simulation()
            .times(1)
            .returning(|_| Box::pin(async { Ok(50_000) }));

        let calc = GasCalculator::new(client);
        let cu = calc
            .compute_budget(dummy_tx(), InstructionKind::ApproveMessage)
            .await
            .unwrap();
        // 50_000 + 25% = 62_500
        assert_eq!(cu, 62_500);
    }

    #[tokio::test]
    async fn approve_message_falls_back_on_simulation_error() {
        let mut client = MockIncluderClientTrait::new();
        client
            .expect_get_units_consumed_from_simulation()
            .times(1)
            .returning(|_| {
                Box::pin(async { Err(IncluderClientError::GenericError("sim boom".into())) })
            });

        let calc = GasCalculator::new(client);
        let cu = calc
            .compute_budget(dummy_tx(), InstructionKind::ApproveMessage)
            .await
            .unwrap();
        assert_eq!(cu, CU_FALLBACK_APPROVE_MESSAGE);
    }

    #[tokio::test]
    async fn alt_create_extend_simulates_and_applies_margin() {
        let mut client = MockIncluderClientTrait::new();
        client
            .expect_get_units_consumed_from_simulation()
            .times(1)
            .returning(|_| Box::pin(async { Ok(10_000) }));

        let calc = GasCalculator::new(client);
        let cu = calc
            .compute_budget(dummy_tx(), InstructionKind::AltCreateExtend)
            .await
            .unwrap();
        // 10_000 + 25% = 12_500
        assert_eq!(cu, 12_500);
    }

    #[tokio::test]
    async fn alt_create_extend_falls_back_on_simulation_error() {
        let mut client = MockIncluderClientTrait::new();
        client
            .expect_get_units_consumed_from_simulation()
            .times(1)
            .returning(|_| {
                Box::pin(async { Err(IncluderClientError::GenericError("sim boom".into())) })
            });

        let calc = GasCalculator::new(client);
        let cu = calc
            .compute_budget(dummy_tx(), InstructionKind::AltCreateExtend)
            .await
            .unwrap();
        assert_eq!(cu, CU_FALLBACK_ALT_CREATE_EXTEND);
    }

    #[tokio::test]
    async fn execute_simulates_and_applies_margin() {
        let mut client = MockIncluderClientTrait::new();
        client
            .expect_get_units_consumed_from_simulation()
            .times(1)
            .returning(|_| Box::pin(async { Ok(49_910) }));

        let calc = GasCalculator::new(client);
        let cu = calc
            .compute_budget(dummy_tx(), InstructionKind::Execute)
            .await
            .unwrap();
        // 49_910 + floor(49_910 * 25 / 100) = 49_910 + 12_477 = 62_387
        assert_eq!(cu, 62_387);
    }

    #[tokio::test]
    async fn execute_propagates_simulation_error_with_no_fallback() {
        let mut client = MockIncluderClientTrait::new();
        client
            .expect_get_units_consumed_from_simulation()
            .times(1)
            .returning(|_| {
                Box::pin(async { Err(IncluderClientError::GenericError("sim boom".into())) })
            });

        let calc = GasCalculator::new(client);
        let result = calc
            .compute_budget(dummy_tx(), InstructionKind::Execute)
            .await;
        assert!(
            result.is_err(),
            "Execute must error out, never silently fall back"
        );
    }

    #[tokio::test]
    async fn other_kind_simulates_and_propagates_error() {
        // success path
        let mut client = MockIncluderClientTrait::new();
        client
            .expect_get_units_consumed_from_simulation()
            .times(1)
            .returning(|_| Box::pin(async { Ok(20_000) }));
        let calc = GasCalculator::new(client);
        // 20_000 + 25% = 25_000
        assert_eq!(
            calc.compute_budget(dummy_tx(), InstructionKind::Other)
                .await
                .unwrap(),
            25_000
        );

        // error path — no fallback
        let mut client = MockIncluderClientTrait::new();
        client
            .expect_get_units_consumed_from_simulation()
            .times(1)
            .returning(|_| {
                Box::pin(async { Err(IncluderClientError::GenericError("sim boom".into())) })
            });
        let calc = GasCalculator::new(client);
        assert!(calc
            .compute_budget(dummy_tx(), InstructionKind::Other)
            .await
            .is_err());
    }

    /// Regression for the silent CU=0 bug: simulation returning Ok(0) used to be
    /// passed through and produced SetComputeUnitLimit(0) txs. With the simulation-error
    /// fix in includer_client, Ok(0) can still occur for genuinely-empty txs but the
    /// 10% margin will keep it at 0; the upstream simulation-error guard prevents the
    /// pathological "err=Some + units_consumed=Some(0)" case from reaching here at all.
    #[tokio::test]
    async fn zero_units_pass_through_only_when_simulation_was_genuinely_clean() {
        let mut client = MockIncluderClientTrait::new();
        client
            .expect_get_units_consumed_from_simulation()
            .times(1)
            .returning(|_| Box::pin(async { Ok(0) }));

        let calc = GasCalculator::new(client);
        let cu = calc
            .compute_budget(dummy_tx(), InstructionKind::Execute)
            .await
            .unwrap();
        assert_eq!(cu, 0);
    }
}
