//! Pre-flight cost estimation for execute transactions.
//!
//! The consensus fee (base signature + priority) is deterministic from the compute budget,
//! but the fee payer (the relayer) also funds rent for any accounts the destination program
//! creates. `simulateTransaction` only gives us the compute units; it does not tell us the
//! rent up front in a way we can rely on across RPC providers. Rent on Solana is, however,
//! a deterministic function of account size — `(128 + data_len) * 6960` lamports — so for the
//! ITS GMP entrypoints (whose account layouts are fixed per program version) we can add the
//! rent as static constants.
//!
//! The constants below were measured on-chain (test validator; identical on devnet/mainnet
//! since the rent parameters and the Metaplex create fee are protocol-wide) and are locked in
//! by the `test_approve_and_execute_its_message` integration test, which asserts each created
//! account's balance against them — so a program upgrade that changes a layout fails CI.

use borsh::BorshDeserialize;
use solana_axelar_its::encoding::{HubMessage, Message};
use solana_sdk::pubkey::Pubkey;

use crate::utils::is_valid_pubkey;

/// Base fee per signature (Solana protocol constant).
pub const LAMPORTS_PER_SIGNATURE: u64 = 5_000;

/// Rent for the `TokenManager` PDA (139 bytes).
pub const TOKEN_MANAGER_RENT: u64 = 1_858_320;
/// Rent for a native interchain-token mint — a plain Token-2022 mint, 82 bytes.
pub const INTERCHAIN_MINT_RENT: u64 = 1_461_600;
/// Rent for a Token-2022 associated token account (170 bytes = 165 base + the 5-byte
/// `ImmutableOwner` extension the associated-token program always adds). Native interchain
/// tokens are Token-2022, so their token-manager and destination ATAs use this.
pub const TOKEN_2022_ATA_RENT: u64 = 2_074_080;
/// Rent for a classic SPL associated token account (165 bytes). Used for linked tokens whose
/// mint is owned by the classic SPL Token program.
pub const SPL_ATA_RENT: u64 = 2_039_280;
/// Cost of the Metaplex `mpl_token_metadata` account created on a deploy: 5,115,600 rent
/// (607 bytes) + the 10,000,000 lamport Metaplex create fee held in the account.
pub const METADATA_RENT_AND_FEE: u64 = 15_115_600;
/// Rent for a `UserRoles` PDA (10 bytes) — the minter role on a deploy, the operator role on a
/// link.
pub const USER_ROLES_RENT: u64 = 960_480;

/// Rent for an associated token account, selected by the owning token program.
fn ata_rent(is_token_2022: bool) -> u64 {
    if is_token_2022 {
        TOKEN_2022_ATA_RENT
    } else {
        SPL_ATA_RENT
    }
}

/// Which ITS GMP entrypoint an execute transaction drives. Determines which accounts the fee
/// payer funds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecuteEntrypoint {
    /// Creates token-manager PDA, mint, token-manager ATA, Metaplex metadata, and — if a
    /// minter is set — a minter roles PDA.
    DeployInterchainToken { has_minter: bool },
    /// Creates token-manager PDA, token-manager ATA, and — if an operator is provided — an
    /// operator roles PDA. `ata_is_token_2022` selects the ATA size for the linked token's
    /// program (a linked classic-SPL token has a smaller ATA).
    LinkToken {
        has_operator: bool,
        ata_is_token_2022: bool,
    },
    /// Creates the destination ATA only when it does not already exist (a repeat transfer to
    /// the same recipient reuses it and pays no rent). `ata_is_token_2022` selects the ATA size.
    InterchainTransfer {
        creates_destination_ata: bool,
        ata_is_token_2022: bool,
    },
    /// Non-ITS execute (governance, arbitrary executable) — no relayer-funded accounts.
    Other,
}

/// Rent (lamports) the fee payer funds for the accounts this entrypoint creates.
pub fn execute_rent_lamports(entrypoint: ExecuteEntrypoint) -> u64 {
    match entrypoint {
        ExecuteEntrypoint::DeployInterchainToken { has_minter } => {
            TOKEN_MANAGER_RENT
                + INTERCHAIN_MINT_RENT
                + TOKEN_2022_ATA_RENT
                + METADATA_RENT_AND_FEE
                + if has_minter { USER_ROLES_RENT } else { 0 }
        }
        ExecuteEntrypoint::LinkToken {
            has_operator,
            ata_is_token_2022,
        } => {
            TOKEN_MANAGER_RENT
                + ata_rent(ata_is_token_2022)
                + if has_operator { USER_ROLES_RENT } else { 0 }
        }
        ExecuteEntrypoint::InterchainTransfer {
            creates_destination_ata,
            ata_is_token_2022,
        } => {
            if creates_destination_ata {
                ata_rent(ata_is_token_2022)
            } else {
                0
            }
        }
        ExecuteEntrypoint::Other => 0,
    }
}

/// Consensus fee: base signature fee + priority fee (`ceil(micro_lamports_per_cu * units / 1e6)`).
pub fn consensus_fee_lamports(
    num_signatures: u64,
    compute_units: u64,
    micro_lamports_per_cu: u64,
) -> u64 {
    let total_micro = u128::from(micro_lamports_per_cu).saturating_mul(u128::from(compute_units));
    let priority = total_micro.div_ceil(1_000_000);
    let priority = u64::try_from(priority).unwrap_or(u64::MAX);
    LAMPORTS_PER_SIGNATURE
        .saturating_mul(num_signatures)
        .saturating_add(priority)
}

/// Full pre-flight estimate the relayer compares against the user's prepaid gas:
/// consensus fee + rent for the accounts the entrypoint creates.
pub fn estimate_execute_cost_lamports(
    num_signatures: u64,
    compute_units: u64,
    micro_lamports_per_cu: u64,
    entrypoint: ExecuteEntrypoint,
) -> u64 {
    consensus_fee_lamports(num_signatures, compute_units, micro_lamports_per_cu)
        .saturating_add(execute_rent_lamports(entrypoint))
}

/// Classifies an execute by decoding its GMP payload. `destination_ata_exists` (consulted only
/// for interchain transfers) folds in an on-chain check so a transfer to an existing ATA isn't
/// charged rent it won't pay; `ata_is_token_2022` (consulted for transfers and links) selects
/// the ATA size for the token's program. Returns [`ExecuteEntrypoint::Other`] for non-ITS
/// destinations or payloads that do not decode (the consensus fee still applies, no rent added).
pub fn classify_execute(
    destination_address: &Pubkey,
    payload: &[u8],
    destination_ata_exists: bool,
    ata_is_token_2022: bool,
) -> ExecuteEntrypoint {
    if *destination_address != solana_axelar_its::ID {
        return ExecuteEntrypoint::Other;
    }

    let mut reader = payload;
    let message = match HubMessage::deserialize(&mut reader) {
        Ok(HubMessage::ReceiveFromHub { message, .. }) => message,
        _ => return ExecuteEntrypoint::Other,
    };

    match message {
        Message::DeployInterchainToken(deploy) => ExecuteEntrypoint::DeployInterchainToken {
            has_minter: is_valid_pubkey(deploy.minter.as_deref()),
        },
        Message::LinkToken(link) => ExecuteEntrypoint::LinkToken {
            // The operator role is created from `params` only when it is a valid pubkey — the
            // same check the transaction builder uses.
            has_operator: is_valid_pubkey(link.params.as_deref()),
            ata_is_token_2022,
        },
        Message::InterchainTransfer(_) => ExecuteEntrypoint::InterchainTransfer {
            creates_destination_ata: !destination_ata_exists,
            ata_is_token_2022,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_axelar_its::encoding::{DeployInterchainToken, InterchainTransfer, LinkToken};

    // Borsh-encode a ReceiveFromHub payload exactly as the relayer receives it on the wire.
    fn receive_from_hub(message: Message) -> Vec<u8> {
        borsh::to_vec(&HubMessage::ReceiveFromHub {
            source_chain: "axelar".to_string(),
            message,
        })
        .unwrap()
    }

    fn deploy_payload(minter: Option<Vec<u8>>) -> Vec<u8> {
        receive_from_hub(Message::DeployInterchainToken(DeployInterchainToken {
            token_id: [1u8; 32],
            name: "Test Token".to_string(),
            symbol: "TEST".to_string(),
            decimals: 9,
            minter,
        }))
    }

    fn link_payload(params: Option<Vec<u8>>) -> Vec<u8> {
        receive_from_hub(Message::LinkToken(LinkToken {
            token_id: [2u8; 32],
            token_manager_type: 4,
            source_token_address: vec![1, 2, 3],
            destination_token_address: Pubkey::new_unique().to_bytes().to_vec(),
            params,
        }))
    }

    fn transfer_payload() -> Vec<u8> {
        receive_from_hub(Message::InterchainTransfer(InterchainTransfer {
            token_id: [3u8; 32],
            source_address: b"ethereum_addr".to_vec(),
            destination_address: Pubkey::new_unique().to_bytes().to_vec(),
            amount: 1_000_000,
            data: None,
        }))
    }

    #[test]
    fn rent_per_entrypoint_matches_measured_constants() {
        // Measured on-chain (test validator; identical on devnet/mainnet) and locked by the
        // `test_approve_and_execute_its_message` integration test.
        assert_eq!(
            execute_rent_lamports(ExecuteEntrypoint::DeployInterchainToken { has_minter: false }),
            20_509_600,
            "deploy = token_manager + mint + ata + metadata"
        );
        assert_eq!(
            execute_rent_lamports(ExecuteEntrypoint::DeployInterchainToken { has_minter: true }),
            21_470_080,
            "deploy + minter roles"
        );
        assert_eq!(
            execute_rent_lamports(ExecuteEntrypoint::LinkToken {
                has_operator: false,
                ata_is_token_2022: true,
            }),
            3_932_400,
            "link (token-2022) = token_manager + token-2022 ata"
        );
        assert_eq!(
            execute_rent_lamports(ExecuteEntrypoint::LinkToken {
                has_operator: false,
                ata_is_token_2022: false,
            }),
            3_897_600,
            "link (classic SPL) = token_manager + spl ata"
        );
        assert_eq!(
            execute_rent_lamports(ExecuteEntrypoint::LinkToken {
                has_operator: true,
                ata_is_token_2022: true,
            }),
            4_892_880,
            "link + operator roles"
        );
        assert_eq!(
            execute_rent_lamports(ExecuteEntrypoint::InterchainTransfer {
                creates_destination_ata: true,
                ata_is_token_2022: true,
            }),
            2_074_080,
            "transfer creating a new token-2022 destination ata"
        );
        assert_eq!(
            execute_rent_lamports(ExecuteEntrypoint::InterchainTransfer {
                creates_destination_ata: true,
                ata_is_token_2022: false,
            }),
            2_039_280,
            "transfer creating a new classic-SPL destination ata"
        );
        assert_eq!(
            execute_rent_lamports(ExecuteEntrypoint::InterchainTransfer {
                creates_destination_ata: false,
                ata_is_token_2022: true,
            }),
            0,
            "transfer reusing an existing destination ata pays no rent"
        );
        assert_eq!(execute_rent_lamports(ExecuteEntrypoint::Other), 0);
    }

    #[test]
    fn classify_decodes_real_payloads() {
        let its = solana_axelar_its::ID;
        let valid_minter = Pubkey::new_unique().to_bytes().to_vec();
        let ata_missing = false; // destination-ata-exists flag
        let t22 = true; // ata_is_token_2022

        assert_eq!(
            classify_execute(&its, &deploy_payload(None), ata_missing, t22),
            ExecuteEntrypoint::DeployInterchainToken { has_minter: false }
        );
        assert_eq!(
            classify_execute(
                &its,
                &deploy_payload(Some(valid_minter.clone())),
                ata_missing,
                t22
            ),
            ExecuteEntrypoint::DeployInterchainToken { has_minter: true }
        );
        // A non-pubkey minter (wrong length) is not counted — matches the builder.
        assert_eq!(
            classify_execute(&its, &deploy_payload(Some(vec![1, 2, 3])), ata_missing, t22),
            ExecuteEntrypoint::DeployInterchainToken { has_minter: false }
        );
        assert_eq!(
            classify_execute(&its, &link_payload(None), ata_missing, t22),
            ExecuteEntrypoint::LinkToken {
                has_operator: false,
                ata_is_token_2022: true,
            }
        );
        // A classic-SPL linked token carries the SPL ata flag through.
        assert_eq!(
            classify_execute(&its, &link_payload(Some(valid_minter)), ata_missing, false),
            ExecuteEntrypoint::LinkToken {
                has_operator: true,
                ata_is_token_2022: false,
            }
        );
        // Transfer: the ata-exists flag decides whether rent is added; ata program decides size.
        assert_eq!(
            classify_execute(&its, &transfer_payload(), false, true),
            ExecuteEntrypoint::InterchainTransfer {
                creates_destination_ata: true,
                ata_is_token_2022: true,
            }
        );
        assert_eq!(
            classify_execute(&its, &transfer_payload(), false, false),
            ExecuteEntrypoint::InterchainTransfer {
                creates_destination_ata: true,
                ata_is_token_2022: false,
            }
        );
        assert_eq!(
            classify_execute(&its, &transfer_payload(), true, true),
            ExecuteEntrypoint::InterchainTransfer {
                creates_destination_ata: false,
                ata_is_token_2022: true,
            }
        );
    }

    #[test]
    fn classify_returns_other_for_non_its_or_undecodable() {
        // Non-ITS destination → Other even with a valid ITS payload.
        assert_eq!(
            classify_execute(&Pubkey::new_unique(), &transfer_payload(), false, true),
            ExecuteEntrypoint::Other
        );
        // Undecodable payload on the ITS program → Other (consensus-only estimate).
        assert_eq!(
            classify_execute(&solana_axelar_its::ID, b"not a hub message", false, true),
            ExecuteEntrypoint::Other
        );
    }

    #[test]
    fn consensus_fee_uses_base_plus_ceil_priority() {
        // cu price 1000 micro-lamports/CU: priority = ceil(1000 * units / 1e6) = ceil(units/1000).
        assert_eq!(consensus_fee_lamports(1, 0, 1000), 5_000);
        assert_eq!(consensus_fee_lamports(1, 200_000, 1000), 5_200);
        assert_eq!(consensus_fee_lamports(2, 200_000, 1000), 10_200);
        // Rounds up.
        assert_eq!(consensus_fee_lamports(1, 1_001, 1000), 5_002);
    }

    /// Reproduces a real on-chain sequence — deploy, then two transfers of that token to the same
    /// recipient — and checks the relayer's pre-calculation matches the measured costs to the
    /// lamport. CU price is assumed 1000 micro-lamports/CU and the compute units are chosen so the
    /// consensus fee equals each tx's real `meta.fee`; the destination-ATA-exists flag is the only
    /// thing that differs between the two transfers.
    ///
    /// On-chain ground truth (1 signature each, all 2026-06-03 mainnet):
    ///   - deploy             5iqomi95… → 20,514,832 (meta.fee 5,232)
    ///   - transfer #1        4ndviy4s… →  2,079,239 (meta.fee 5,159, new Token-2022 ATA)
    ///   - transfer #2        2MbuqbGx… →      5,134 (meta.fee 5,134, ATA reused)
    ///   - transfer (SPL)     5tkwsAUC… →  2,044,492 (meta.fee 5,212, new classic-SPL ATA)
    #[test]
    fn precalculates_deploy_then_two_transfers() {
        let its = solana_axelar_its::ID;

        // Deploy (no minter). units 232,000 → consensus 5,000 + 232 = 5,232.
        let deploy = classify_execute(&its, &deploy_payload(None), false, true);
        assert_eq!(
            estimate_execute_cost_lamports(1, 232_000, 1000, deploy),
            20_514_832,
        );

        // Transfer #1 — Token-2022 recipient ATA does not exist yet, so it is created.
        // units 159,000 → consensus 5,159; + 2,074,080 Token-2022 ATA rent.
        let transfer_new = classify_execute(&its, &transfer_payload(), false, true);
        assert_eq!(
            estimate_execute_cost_lamports(1, 159_000, 1000, transfer_new),
            2_079_239,
        );

        // Transfer #2 — same recipient, ATA already exists → no rent, consensus only.
        // units 134,000 → consensus 5,134.
        let transfer_reuse = classify_execute(&its, &transfer_payload(), true, true);
        assert_eq!(
            estimate_execute_cost_lamports(1, 134_000, 1000, transfer_reuse),
            5_134,
        );

        // Transfer of a linked classic-SPL token, new ATA (165 bytes).
        // units 212,000 → consensus 5,212; + 2,039,280 SPL ATA rent.
        let transfer_spl = classify_execute(&its, &transfer_payload(), false, false);
        assert_eq!(
            estimate_execute_cost_lamports(1, 212_000, 1000, transfer_spl),
            2_044_492,
        );
    }

    #[test]
    fn non_its_execute_is_consensus_only() {
        let other = classify_execute(&Pubkey::new_unique(), b"opaque", false, true);
        assert_eq!(
            estimate_execute_cost_lamports(1, 100_000, 1000, other),
            5_100
        );
    }
}
