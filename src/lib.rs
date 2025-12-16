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
#[cfg(feature = "test-mocks")]
pub mod mocks {
    pub use crate::models::solana_transaction::MockUpdateEvents;

    pub use crate::models::refunds::MockRefundsModel;
    pub use crate::redis::MockRedisConnectionTrait;

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
