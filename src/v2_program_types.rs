use anchor_lang::{AnchorDeserialize, AnchorSerialize};
use axelar_solana_gateway_v2::{MessageLeaf, SigningVerifierSetInfo};
pub type Signature = [u8; 65];

#[derive(Debug, Eq, PartialEq, Clone, AnchorSerialize, AnchorDeserialize)]
#[allow(clippy::pub_underscore_fields)]
pub struct ExecuteData {
    /// The Merkle root of the signing verifier set.
    pub signing_verifier_set_merkle_root: [u8; 32],

    /// A list of information about each verifier in the signing set, including
    /// their signatures and Merkle proofs.
    pub signing_verifier_set_leaves: Vec<SigningVerifierSetInfo>,

    /// The Merkle root of the payload data.
    pub payload_merkle_root: [u8; 32],

    /// The payload items, which can either be new messages or a verifier set
    /// rotation, each accompanied by their respective Merkle proofs.
    pub payload_items: MerkleisedPayload,

    /// Padding for memory alignment compatibility.
    pub _padding: [u8; 7],
}

#[derive(Debug, Eq, PartialEq, Clone, AnchorSerialize, AnchorDeserialize)]
#[allow(clippy::pub_underscore_fields)]
pub enum MerkleisedPayload {
    /// Indicates a rotation of the verifier set, providing the new Merkle root
    /// of the verifier set.
    VerifierSetRotation {
        /// The Merkle root of the new verifier set after rotation.
        new_verifier_set_merkle_root: [u8; 32],
        /// Padding for memory alignment compatibility.
        _padding: [u8; 7],
    },

    /// Contains a list of new messages, each with its corresponding Merkle
    /// proof.
    NewMessages {
        /// A vector of `MerkleisedMessage` instances, each representing a
        /// message and its proof.
        messages: Vec<MerkleisedMessage>,
        /// Padding for memory alignment compatibility.
        _padding: [u8; 7],
    },
}

#[derive(Debug, Eq, PartialEq, Clone, AnchorSerialize, AnchorDeserialize)]
#[allow(clippy::pub_underscore_fields)]
pub struct MerkleisedMessage {
    /// The leaf node representing the message in the Merkle tree.
    pub leaf: MessageLeaf,

    /// The Merkle proof demonstrating the message's inclusion in the payload's
    /// Merkle tree.
    pub proof: Vec<u8>,

    /// Padding for memory alignment compatibility.
    pub _padding: [u8; 7],
}
