use anchor_lang::{AnchorDeserialize, AnchorSerialize};
use solana_axelar_gateway::{MerklizedMessage, SigningVerifierSetInfo};
pub type Signature = [u8; 65];

#[derive(Debug, Eq, PartialEq, Clone, AnchorSerialize, AnchorDeserialize)]
pub struct ExecuteData {
    pub signing_verifier_set_merkle_root: [u8; 32],
    pub signing_verifier_set_leaves: Vec<SigningVerifierSetInfo>,
    pub payload_merkle_root: [u8; 32],
    pub payload_items: MerkleisedPayload,
}

#[derive(Debug, Eq, PartialEq, Clone, AnchorSerialize, AnchorDeserialize)]
pub enum MerkleisedPayload {
    VerifierSetRotation {
        new_verifier_set_merkle_root: [u8; 32],
    },
    NewMessages {
        messages: Vec<MerklizedMessage>,
    },
}
