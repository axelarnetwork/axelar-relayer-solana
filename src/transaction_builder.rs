pub struct TransactionBuilder {
    wallet: Wallet,
    client: IncluderClient,
    gateway_address: Pubkey,
    gas_service_address: Pubkey,
    chain_name: String,
}

pub trait TransactionBuilderTrait {
    fn build(&self, ix: Instruction) -> Transaction;
}
