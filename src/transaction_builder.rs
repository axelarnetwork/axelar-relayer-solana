use crate::gas_estimator::GasEstimatorTrait;
use crate::includer_client::IncluderClientTrait;
use async_trait::async_trait;
use relayer_core::error::ClientError;
use relayer_core::utils::ThreadSafe;
use solana_sdk::instruction::Instruction;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signer::keypair::Keypair;
use solana_sdk::signer::Signer;
use solana_sdk::transaction::Transaction;
use std::sync::Arc;

#[derive(Clone)]
pub struct TransactionBuilder<
    GE: GasEstimatorTrait + ThreadSafe,
    IC: IncluderClientTrait + ThreadSafe,
> {
    keypair: Arc<Keypair>,
    client: IC,
    gateway_address: Pubkey,
    gas_service_address: Pubkey,
    chain_name: String,
    gas_estimator: GE,
}

#[async_trait]
pub trait TransactionBuilderTrait {
    async fn build(&self, ix: Instruction) -> Result<Transaction, ClientError>;
}

impl<GE: GasEstimatorTrait + ThreadSafe, IC: IncluderClientTrait + ThreadSafe>
    TransactionBuilder<GE, IC>
{
    pub fn new(
        keypair: Arc<Keypair>,
        client: IC,
        gateway_address: Pubkey,
        gas_service_address: Pubkey,
        chain_name: String,
        gas_estimator: GE,
    ) -> Self {
        Self {
            keypair,
            client,
            gateway_address,
            gas_service_address,
            chain_name,
            gas_estimator,
        }
    }
}

#[async_trait]
impl<GE: GasEstimatorTrait + ThreadSafe, IC: IncluderClientTrait + ThreadSafe>
    TransactionBuilderTrait for TransactionBuilder<GE, IC>
{
    async fn build(&self, ix: Instruction) -> Result<Transaction, ClientError> {
        let compute_unit_price_ix = self
            .gas_estimator
            .compute_unit_price(&[ix.clone()])
            .await
            .map_err(|e| ClientError::BadRequest(e.to_string()))?;

        // Since simulation gets the latest blockhash we can directly use it for the tx construction
        let (compute_budget_ix, hash) = self
            .gas_estimator
            .compute_budget(&[ix.clone()])
            .await
            .map_err(|e| ClientError::BadRequest(e.to_string()))?;

        // Add the two budget instructions first, the the instruction we want to execute on Solana
        let instructions = vec![compute_unit_price_ix, compute_budget_ix, ix];

        Ok(Transaction::new_signed_with_payer(
            &instructions,
            Some(&self.keypair.pubkey()),
            &[&self.keypair],
            hash,
        ))
    }
}
