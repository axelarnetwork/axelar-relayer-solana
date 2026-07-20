//! Pre-flight cost estimation for execute transactions.
//!
//! The consensus fee (base signature + priority) is deterministic from the compute budget,
//! but the fee payer (the relayer) also funds rent for any accounts the destination program
//! creates. `simulateTransaction` only gives us the compute units; it does not tell us the
//! rent up front in a way we can rely on across RPC providers. Rent on Solana is, however,
//! a deterministic function of account size, so for the ITS GMP entrypoints we derive the rent
//! from the on-chain account layouts: the Anchor `INIT_SPACE` of the PDAs we create, the
//! SPL / Token-2022 account sizes, and — for a Token-2022 ATA — the actual size the
//! associated-token program allocates given the mint's extensions.
//!
//! The derived values are confirmed against real on-chain balances by the
//! `test_approve_and_execute_its_message` integration test, so a program upgrade that changes a
//! layout fails CI. Only the Metaplex metadata account stays a measured literal — its size and
//! create fee are owned by an external program and not exposed as constants.

use anchor_lang::{Discriminator, Space};
use anchor_spl::token::spl_token;
use anchor_spl::token_2022::spl_token_2022::{
    extension::{BaseStateWithExtensions, ExtensionType, StateWithExtensions},
    state::{Account as Token2022Account, Mint as Token2022Mint},
};
use solana_axelar_its::encoding::Message;
use solana_axelar_its::state::{TokenManager, UserRoles};
use solana_sdk::program_error::ProgramError;
use solana_sdk::program_pack::Pack;
use solana_sdk::rent::Rent;

use crate::utils::is_valid_pubkey;

/// Base fee per signature (Solana protocol constant).
pub const LAMPORTS_PER_SIGNATURE: u64 = 5_000;

/// Metaplex metadata account layout size (bytes). `mpl-token-metadata` exposes only field
/// maxes, not a packed-length constant, and the account is allocated by the external Metaplex
/// program — so this stays a measured literal, locked by the integration test.
const METADATA_ACCOUNT_LEN: usize = 607;
/// Flat fee the Metaplex program charges on metadata creation and deposits into the account on
/// top of rent. Not exposed as a constant by `mpl-token-metadata`.
const METAPLEX_CREATE_FEE: u64 = 10_000_000;

/// Rent-exempt minimum for an account of `data_len` bytes (Solana's standard rent params).
pub fn rent_exempt_lamports(data_len: usize) -> u64 {
    Rent::default().minimum_balance(data_len)
}

/// Rent for the `TokenManager` PDA, sized from its on-chain Anchor layout.
pub fn token_manager_rent() -> u64 {
    rent_exempt_lamports(TokenManager::DISCRIMINATOR.len() + TokenManager::INIT_SPACE)
}

/// Rent for a `UserRoles` PDA — the minter role on a deploy, the operator role on a link.
pub fn user_roles_rent() -> u64 {
    rent_exempt_lamports(UserRoles::DISCRIMINATOR.len() + UserRoles::INIT_SPACE)
}

/// Rent for a native interchain-token mint — a plain (extension-free) Token-2022 mint.
pub fn interchain_mint_rent() -> u64 {
    rent_exempt_lamports(Token2022Mint::LEN)
}

/// Rent for a classic SPL associated token account.
pub fn spl_ata_rent() -> u64 {
    rent_exempt_lamports(spl_token::state::Account::LEN)
}

/// Rent + Metaplex create fee for the metadata account a deploy creates.
pub fn metadata_rent_and_fee() -> u64 {
    rent_exempt_lamports(METADATA_ACCOUNT_LEN) + METAPLEX_CREATE_FEE
}

/// Account extensions the associated-token program initializes on a Token-2022 ATA: the
/// `ImmutableOwner` it always adds, plus the account-side extensions the mint's extensions
/// require (e.g. `TransferFeeConfig` → `TransferFeeAmount`).
fn token_2022_account_extensions(mint_extensions: &[ExtensionType]) -> Vec<ExtensionType> {
    let mut extensions = ExtensionType::get_required_init_account_extensions(mint_extensions);
    if !extensions.contains(&ExtensionType::ImmutableOwner) {
        extensions.push(ExtensionType::ImmutableOwner);
    }
    extensions
}

/// Data length of the ATA the associated-token program creates for a Token-2022 `mint`. The
/// size — and thus the rent — depends on the mint's extensions, so a `TransferFeeConfig` (or
/// `NonTransferable`, `TransferHook`, …) mint yields a larger ATA than a plain one.
pub fn token_2022_ata_len(mint_data: &[u8]) -> Result<usize, ProgramError> {
    let mint = StateWithExtensions::<Token2022Mint>::unpack(mint_data)?;
    let extensions = token_2022_account_extensions(&mint.get_extension_types()?);
    ExtensionType::try_calculate_account_len::<Token2022Account>(&extensions)
}

/// Rent for the extension-free Token-2022 ATA the deploy flow creates for the freshly-minted
/// native interchain token (base account + `ImmutableOwner`).
pub fn native_interchain_ata_rent() -> u64 {
    let len = ExtensionType::try_calculate_account_len::<Token2022Account>(&[
        ExtensionType::ImmutableOwner,
    ])
    .expect("a fixed extension set has a known length");
    rent_exempt_lamports(len)
}

/// Rent for an address lookup table holding `num_addresses` entries (56-byte meta + 32 bytes per
/// address). This rent is reclaimed when the table is closed, so it must be excluded from a
/// message's reported cost. Only the create/extend consensus fee is a real cost.
pub fn alt_rent_lamports(num_addresses: usize) -> u64 {
    const LOOKUP_TABLE_META_SIZE: usize = 56;
    rent_exempt_lamports(LOOKUP_TABLE_META_SIZE + 32 * num_addresses)
}

/// Which ITS GMP entrypoint an execute transaction drives. Determines which accounts the fee
/// payer funds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecuteEntrypoint {
    /// Creates token-manager PDA, mint, token-manager ATA, Metaplex metadata, and — if a
    /// minter is set — a minter roles PDA.
    DeployInterchainToken { has_minter: bool },
    /// Creates token-manager PDA, the token-manager ATA (only when it doesn't already exist —
    /// the ATA is permissionlessly creatable, and the on-chain instruction is `init_if_needed`),
    /// and — if an operator is provided — an operator roles PDA. `ata_len` is the on-chain byte
    /// length of the linked token's ATA (classic SPL, or Token-2022 sized for the mint's
    /// extensions).
    LinkToken {
        has_operator: bool,
        creates_ata: bool,
        ata_len: usize,
    },
    /// Creates the destination ATA only when it does not already exist (a repeat transfer to
    /// the same recipient reuses it and pays no rent). `ata_len` is the destination ATA's
    /// on-chain byte length.
    InterchainTransfer {
        creates_destination_ata: bool,
        ata_len: usize,
    },
    /// Non-ITS execute (governance, arbitrary executable) — no relayer-funded accounts.
    Other,
}

/// Rent (lamports) the fee payer funds for the accounts this entrypoint creates.
pub fn execute_rent_lamports(entrypoint: ExecuteEntrypoint) -> u64 {
    match entrypoint {
        ExecuteEntrypoint::DeployInterchainToken { has_minter } => {
            token_manager_rent()
                + interchain_mint_rent()
                + native_interchain_ata_rent()
                + metadata_rent_and_fee()
                + if has_minter { user_roles_rent() } else { 0 }
        }
        ExecuteEntrypoint::LinkToken {
            has_operator,
            creates_ata,
            ata_len,
        } => {
            token_manager_rent()
                + if creates_ata {
                    rent_exempt_lamports(ata_len)
                } else {
                    0
                }
                + if has_operator { user_roles_rent() } else { 0 }
        }
        ExecuteEntrypoint::InterchainTransfer {
            creates_destination_ata,
            ata_len,
        } => {
            if creates_destination_ata {
                rent_exempt_lamports(ata_len)
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

/// Classifies an already-decoded ITS execute message into the entrypoint whose rent the fee payer funds.
pub fn classify_execute(message: &Message, ata_exists: bool, ata_len: usize) -> ExecuteEntrypoint {
    match message {
        Message::DeployInterchainToken(deploy) => ExecuteEntrypoint::DeployInterchainToken {
            has_minter: is_valid_pubkey(deploy.minter.as_deref()),
        },
        Message::LinkToken(link) => ExecuteEntrypoint::LinkToken {
            // The operator role is created from `params` only when it is a valid pubkey — the
            // same check the transaction builder uses.
            has_operator: is_valid_pubkey(link.params.as_deref()),
            creates_ata: !ata_exists,
            ata_len,
        },
        Message::InterchainTransfer(_) => ExecuteEntrypoint::InterchainTransfer {
            creates_destination_ata: !ata_exists,
            ata_len,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_axelar_its::encoding::{DeployInterchainToken, InterchainTransfer, LinkToken};
    use solana_sdk::pubkey::Pubkey;

    fn deploy_msg(minter: Option<Vec<u8>>) -> Message {
        Message::DeployInterchainToken(DeployInterchainToken {
            token_id: [1u8; 32],
            name: "Test Token".to_string(),
            symbol: "TEST".to_string(),
            decimals: 9,
            minter,
        })
    }

    fn link_msg(params: Option<Vec<u8>>) -> Message {
        Message::LinkToken(LinkToken {
            token_id: [2u8; 32],
            token_manager_type: 4,
            source_token_address: vec![1, 2, 3],
            destination_token_address: Pubkey::new_unique().to_bytes().to_vec(),
            params,
        })
    }

    fn transfer_msg() -> Message {
        Message::InterchainTransfer(InterchainTransfer {
            token_id: [3u8; 32],
            source_address: b"ethereum_addr".to_vec(),
            destination_address: Pubkey::new_unique().to_bytes().to_vec(),
            amount: 1_000_000,
            data: None,
        })
    }

    /// On-chain byte length of a plain Token-2022 ATA (base account + `ImmutableOwner`) and a
    /// classic SPL ATA — the two ATA sizes the cost model uses when no mint extensions apply.
    const T22_ATA: usize = 170;
    const SPL_ATA: usize = 165;

    /// A bare (extension-free) Token-2022 mint, as the relayer would read it from the chain.
    fn bare_token_2022_mint() -> Vec<u8> {
        let mut data = vec![0u8; Token2022Mint::LEN];
        Token2022Mint::pack(
            Token2022Mint {
                is_initialized: true,
                ..Default::default()
            },
            &mut data,
        )
        .unwrap();
        data
    }

    /// A Token-2022 mint carrying the given extensions (only `TransferFeeConfig` is supported by
    /// this helper), packed exactly as the chain stores it.
    fn token_2022_mint_with(extensions: &[ExtensionType]) -> Vec<u8> {
        use anchor_spl::token_2022::spl_token_2022::extension::{
            transfer_fee::TransferFeeConfig, BaseStateWithExtensionsMut, StateWithExtensionsMut,
        };

        let len = ExtensionType::try_calculate_account_len::<Token2022Mint>(extensions).unwrap();
        let mut data = vec![0u8; len];
        let mut state =
            StateWithExtensionsMut::<Token2022Mint>::unpack_uninitialized(&mut data).unwrap();
        for extension in extensions {
            match extension {
                ExtensionType::TransferFeeConfig => {
                    state.init_extension::<TransferFeeConfig>(true).unwrap();
                }
                other => panic!("test helper does not support {other:?}"),
            }
        }
        state.base = Token2022Mint {
            is_initialized: true,
            ..Default::default()
        };
        state.pack_base();
        state.init_account_type().unwrap();
        data
    }

    #[test]
    fn token_2022_ata_len_grows_with_mint_extensions() {
        // A plain mint → the ATA carries only ImmutableOwner → the baseline 170 bytes.
        assert_eq!(
            token_2022_ata_len(&bare_token_2022_mint()).unwrap(),
            T22_ATA
        );

        // A TransferFeeConfig mint forces a TransferFeeAmount extension on every token account,
        // so its ATA is larger than the old fixed 170-byte assumption. Charging the baseline
        // would under-fund the rent the relayer pays on every transfer of such a token.
        let fee_ata =
            token_2022_ata_len(&token_2022_mint_with(&[ExtensionType::TransferFeeConfig])).unwrap();
        assert_eq!(fee_ata, 182);
        assert!(rent_exempt_lamports(fee_ata) > native_interchain_ata_rent());
    }

    #[test]
    fn alt_rent_scales_with_address_count() {
        // ALT account = 56-byte meta + 32 bytes per address.
        assert_eq!(alt_rent_lamports(0), rent_exempt_lamports(56));
        assert_eq!(alt_rent_lamports(1), rent_exempt_lamports(88));
        assert_eq!(alt_rent_lamports(20), rent_exempt_lamports(696));
        assert_eq!(alt_rent_lamports(1), 1_503_360); // rent of a 216-byte account
    }

    #[test]
    fn rent_per_entrypoint_matches_measured_values() {
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
                creates_ata: true,
                ata_len: T22_ATA,
            }),
            3_932_400,
            "link (token-2022) = token_manager + token-2022 ata"
        );
        assert_eq!(
            execute_rent_lamports(ExecuteEntrypoint::LinkToken {
                has_operator: false,
                creates_ata: true,
                ata_len: SPL_ATA,
            }),
            3_897_600,
            "link (classic SPL) = token_manager + spl ata"
        );
        assert_eq!(
            execute_rent_lamports(ExecuteEntrypoint::LinkToken {
                has_operator: true,
                creates_ata: true,
                ata_len: T22_ATA,
            }),
            4_892_880,
            "link + operator roles"
        );
        assert_eq!(
            execute_rent_lamports(ExecuteEntrypoint::LinkToken {
                has_operator: false,
                creates_ata: false,
                ata_len: T22_ATA,
            }),
            1_858_320,
            "link reusing a pre-existing token-manager ata pays only the token_manager rent"
        );
        assert_eq!(
            execute_rent_lamports(ExecuteEntrypoint::InterchainTransfer {
                creates_destination_ata: true,
                ata_len: T22_ATA,
            }),
            2_074_080,
            "transfer creating a new token-2022 destination ata"
        );
        assert_eq!(
            execute_rent_lamports(ExecuteEntrypoint::InterchainTransfer {
                creates_destination_ata: true,
                ata_len: SPL_ATA,
            }),
            2_039_280,
            "transfer creating a new classic-SPL destination ata"
        );
        assert_eq!(
            execute_rent_lamports(ExecuteEntrypoint::InterchainTransfer {
                creates_destination_ata: false,
                ata_len: T22_ATA,
            }),
            0,
            "transfer reusing an existing destination ata pays no rent"
        );
        assert_eq!(execute_rent_lamports(ExecuteEntrypoint::Other), 0);
    }

    #[test]
    fn classify_decodes_real_payloads() {
        let valid_minter = Pubkey::new_unique().to_bytes().to_vec();
        let ata_exists = false;

        assert_eq!(
            classify_execute(&deploy_msg(None), ata_exists, T22_ATA),
            ExecuteEntrypoint::DeployInterchainToken { has_minter: false }
        );
        assert_eq!(
            classify_execute(&deploy_msg(Some(valid_minter.clone())), ata_exists, T22_ATA),
            ExecuteEntrypoint::DeployInterchainToken { has_minter: true }
        );
        // A non-pubkey minter (wrong length) is not counted — matches the builder.
        assert_eq!(
            classify_execute(&deploy_msg(Some(vec![1, 2, 3])), ata_exists, T22_ATA),
            ExecuteEntrypoint::DeployInterchainToken { has_minter: false }
        );
        assert_eq!(
            classify_execute(&link_msg(None), ata_exists, T22_ATA),
            ExecuteEntrypoint::LinkToken {
                has_operator: false,
                creates_ata: true,
                ata_len: T22_ATA,
            }
        );
        // A classic-SPL linked token carries its ATA length through.
        assert_eq!(
            classify_execute(&link_msg(Some(valid_minter)), ata_exists, SPL_ATA),
            ExecuteEntrypoint::LinkToken {
                has_operator: true,
                creates_ata: true,
                ata_len: SPL_ATA,
            }
        );
        // A link whose token-manager ATA already exists pays no ATA rent.
        assert_eq!(
            classify_execute(&link_msg(None), true, T22_ATA),
            ExecuteEntrypoint::LinkToken {
                has_operator: false,
                creates_ata: false,
                ata_len: T22_ATA,
            }
        );
        // Transfer: the ata-exists flag decides whether rent is added; ata_len sizes it.
        assert_eq!(
            classify_execute(&transfer_msg(), false, T22_ATA),
            ExecuteEntrypoint::InterchainTransfer {
                creates_destination_ata: true,
                ata_len: T22_ATA,
            }
        );
        assert_eq!(
            classify_execute(&transfer_msg(), false, SPL_ATA),
            ExecuteEntrypoint::InterchainTransfer {
                creates_destination_ata: true,
                ata_len: SPL_ATA,
            }
        );
        assert_eq!(
            classify_execute(&transfer_msg(), true, T22_ATA),
            ExecuteEntrypoint::InterchainTransfer {
                creates_destination_ata: false,
                ata_len: T22_ATA,
            }
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
        // Deploy (no minter). units 232,000 → consensus 5,000 + 232 = 5,232.
        let deploy = classify_execute(&deploy_msg(None), false, T22_ATA);
        assert_eq!(
            estimate_execute_cost_lamports(1, 232_000, 1000, deploy),
            20_514_832,
        );

        // Transfer #1 — Token-2022 recipient ATA does not exist yet, so it is created.
        // units 159,000 → consensus 5,159; + 2,074,080 Token-2022 ATA rent.
        let transfer_new = classify_execute(&transfer_msg(), false, T22_ATA);
        assert_eq!(
            estimate_execute_cost_lamports(1, 159_000, 1000, transfer_new),
            2_079_239,
        );

        // Transfer #2 — same recipient, ATA already exists → no rent, consensus only.
        // units 134,000 → consensus 5,134.
        let transfer_reuse = classify_execute(&transfer_msg(), true, T22_ATA);
        assert_eq!(
            estimate_execute_cost_lamports(1, 134_000, 1000, transfer_reuse),
            5_134,
        );

        // Transfer of a linked classic-SPL token, new ATA (165 bytes).
        // units 212,000 → consensus 5,212; + 2,039,280 SPL ATA rent.
        let transfer_spl = classify_execute(&transfer_msg(), false, SPL_ATA);
        assert_eq!(
            estimate_execute_cost_lamports(1, 212_000, 1000, transfer_spl),
            2_044_492,
        );
    }

    #[test]
    fn non_its_execute_is_consensus_only() {
        // A non-ITS execute funds no relayer accounts, so its estimate is consensus-only.
        assert_eq!(
            estimate_execute_cost_lamports(1, 100_000, 1000, ExecuteEntrypoint::Other),
            5_100
        );
    }
}
