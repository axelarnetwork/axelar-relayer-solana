pub mod config;
pub mod error;
pub mod gas_calculator;
pub mod ingestor;
pub mod models;
pub mod poll_client;
pub mod stream_client;
pub mod utils;
pub use models::refunds;
pub use models::solana_subscriber_cursor;
pub use models::solana_transaction;
pub mod fees_client;
pub mod includer;
pub mod includer_client;
pub mod redis;
pub mod refund_manager;
pub mod subscriber_listener;
pub mod subscriber_poller;
pub mod test_utils;
pub mod transaction_builder;
pub mod transaction_type;
pub mod types;

/// Re-export mock types when the `test-mocks` feature is enabled.
/// Use this in integration tests by adding `solana = { path = "..", features = ["test-mocks"] }`
///
/// Only external services and input data should be mocked:
/// - GMP API (external service)
/// - Redis connection (for gas cost caching - can be mocked)
/// - Refunds model (if not using test DB)
/// - Update events model (for ingestor tests)
///
/// Real components should be used for true integration tests:
/// - IncluderClient, TransactionBuilder, GasCalculator, FeesClient
#[cfg(feature = "test-mocks")]
pub mod mocks {
    // For ingestor tests - mock the update events model
    pub use crate::models::solana_transaction::MockUpdateEvents;

    // For includer tests - mock external services only
    pub use crate::models::refunds::MockRefundsModel;
    pub use crate::redis::MockRedisConnectionTrait;

    // Clone implementations for mocks (required by SolanaIncluder)
    impl Clone for MockRedisConnectionTrait {
        fn clone(&self) -> Self {
            Self::new()
        }
    }

    impl Clone for MockRefundsModel {
        fn clone(&self) -> Self {
            Self::new()
        }
    }
}
