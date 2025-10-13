// after a tx happened, we need to calculate the gas used to issue a refund tx

use solana_sdk::pubkey::Pubkey;

#[derive(Clone)]
pub struct GasCalculator {
    _our_addresses: Vec<Pubkey>,
}

impl GasCalculator {
    pub fn new(our_addresses: Vec<Pubkey>) -> Self {
        Self {
            _our_addresses: our_addresses,
        }
    }
}
