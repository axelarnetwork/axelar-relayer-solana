//! Integration tests for the Subscriber (Poller and Listener).
//!
//! These tests demonstrate how to:
//! 1. Create subscriber instances with mocks
//! 2. Run background tasks in tests
//! 3. Use cancellation tokens for graceful shutdown
//!
//! To run these tests:
//! ```bash
//! cargo test --test subscriber_integration
//! ```
//! Note: The `test-mocks` feature is enabled by default, so rust-analyzer
//! will recognize the mock types without any IDE configuration.
//!

use std::sync::Arc;
use std::time::Duration;

// Import mocks from the crate (available via default `test-mocks` feature)
use solana::mocks::{MockSolanaRpcClientTrait, MockSolanaTransactionModel, MockSubscriberCursor};
use solana::subscriber_poller::SolanaPoller;

use relayer_core::queue::MockQueueTrait;
use relayer_core::queue::QueueTrait;

use solana_sdk::pubkey::Pubkey;
use tokio_util::sync::CancellationToken;

/// Example test that spawns the poller in the background and cancels it after some time.
///
/// Yes, you CAN spawn tasks inside `#[tokio::test]`!
/// The test runtime supports spawning background tasks.
#[tokio::test]
async fn test_subscriber_poller_starts_and_stops() {
    // Set up mocks
    let mut mock_cursor = MockSubscriberCursor::new();
    let mock_tx_model = MockSolanaTransactionModel::new();
    let mut mock_queue = MockQueueTrait::new();
    let mut mock_client = MockSolanaRpcClientTrait::new();

    // Configure mock expectations
    // The cursor will be asked for the latest signature
    mock_cursor
        .expect_get_latest_signature()
        .returning(|_, _| Box::pin(async { Ok(None) }));

    mock_cursor
        .expect_store_latest_signature()
        .returning(|_, _, _| Box::pin(async { Ok(()) }));

    // The client will be asked for signatures - return empty to simulate no new transactions
    mock_client
        .expect_get_transactions_for_account()
        .returning(|_, _, _| Box::pin(async { Ok(vec![]) }));

    // Queue publish should never be called since there are no transactions
    mock_queue.expect_publish().times(0);

    // Create the poller
    let queue: Arc<dyn QueueTrait> = Arc::new(mock_queue);
    let poller = SolanaPoller::new(
        mock_client,
        "test_poller".to_string(),
        Arc::new(mock_tx_model),
        Arc::new(mock_cursor),
        queue,
    )
    .await
    .expect("Failed to create poller");

    // Create a cancellation token for graceful shutdown
    let cancellation_token = CancellationToken::new();
    let token_clone = cancellation_token.clone();

    // Spawn the poller in the background
    let poller_handle = tokio::spawn(async move {
        poller
            .run(
                Pubkey::new_unique(), // gas_service_account
                Pubkey::new_unique(), // gateway_account
                Pubkey::new_unique(), // its_account
                token_clone,
            )
            .await;
    });

    // Let it run for a short time
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Signal shutdown
    cancellation_token.cancel();

    // Wait for the poller to finish (with timeout to avoid hanging)
    let result = tokio::time::timeout(Duration::from_secs(5), poller_handle).await;

    assert!(result.is_ok(), "Poller should have stopped within timeout");
    assert!(
        result.unwrap().is_ok(),
        "Poller task should complete without panic"
    );
}

/// Example test using tokio::select! to race between multiple futures.
/// This is useful when you want to assert something while the subscriber is running.
#[tokio::test]
async fn test_subscriber_with_select() {
    let mut mock_cursor = MockSubscriberCursor::new();
    let mock_tx_model = MockSolanaTransactionModel::new();
    let mut mock_queue = MockQueueTrait::new();
    let mut mock_client = MockSolanaRpcClientTrait::new();

    // Use an atomic counter to track how many times the poller polls
    let poll_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let poll_count_clone = Arc::clone(&poll_count);

    mock_cursor
        .expect_get_latest_signature()
        .returning(|_, _| Box::pin(async { Ok(None) }));

    mock_cursor
        .expect_store_latest_signature()
        .returning(|_, _, _| Box::pin(async { Ok(()) }));

    // Track poll attempts
    mock_client
        .expect_get_transactions_for_account()
        .returning(move |_, _, _| {
            poll_count_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Box::pin(async { Ok(vec![]) })
        });

    mock_queue.expect_publish().times(0);

    let queue: Arc<dyn QueueTrait> = Arc::new(mock_queue);
    let poller = SolanaPoller::new(
        mock_client,
        "test_poller".to_string(),
        Arc::new(mock_tx_model),
        Arc::new(mock_cursor),
        queue,
    )
    .await
    .unwrap();

    let cancellation_token = CancellationToken::new();
    let token_clone = cancellation_token.clone();

    let poller_handle = tokio::spawn(async move {
        poller
            .run(
                Pubkey::new_unique(),
                Pubkey::new_unique(),
                Pubkey::new_unique(),
                token_clone,
            )
            .await;
    });

    // Use select! to race between:
    // 1. Waiting for the poller to poll at least once
    // 2. A timeout
    tokio::select! {
        _ = async {
            // Wait until we've polled at least once
            while poll_count.load(std::sync::atomic::Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        } => {
            // Success! The poller ran at least once
        }
        _ = tokio::time::sleep(Duration::from_secs(5)) => {
            panic!("Timeout waiting for poller to poll");
        }
    }

    // Now we can assert things
    assert!(
        poll_count.load(std::sync::atomic::Ordering::SeqCst) >= 1,
        "Poller should have polled at least once"
    );

    // Clean up
    cancellation_token.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(2), poller_handle).await;
}

/// Example of testing with channels for more complex coordination.
/// Channels allow the mock to signal when certain events happen.
#[tokio::test]
async fn test_subscriber_with_channels() {
    use tokio::sync::mpsc;

    let mut mock_cursor = MockSubscriberCursor::new();
    let mock_tx_model = MockSolanaTransactionModel::new();
    let mut mock_queue = MockQueueTrait::new();
    let mut mock_client = MockSolanaRpcClientTrait::new();

    // Create a channel to signal when the client is called
    let (tx, mut rx) = mpsc::channel::<()>(10);

    mock_cursor
        .expect_get_latest_signature()
        .returning(|_, _| Box::pin(async { Ok(None) }));

    mock_cursor
        .expect_store_latest_signature()
        .returning(|_, _, _| Box::pin(async { Ok(()) }));

    mock_client
        .expect_get_transactions_for_account()
        .returning(move |_, _, _| {
            let tx = tx.clone();
            Box::pin(async move {
                // Signal that we were called
                let _ = tx.send(()).await;
                Ok(vec![])
            })
        });

    mock_queue.expect_publish().times(0);

    let queue: Arc<dyn QueueTrait> = Arc::new(mock_queue);
    let poller = SolanaPoller::new(
        mock_client,
        "test_poller".to_string(),
        Arc::new(mock_tx_model),
        Arc::new(mock_cursor),
        queue,
    )
    .await
    .unwrap();

    let cancellation_token = CancellationToken::new();
    let token_clone = cancellation_token.clone();

    let poller_handle = tokio::spawn(async move {
        poller
            .run(
                Pubkey::new_unique(),
                Pubkey::new_unique(),
                Pubkey::new_unique(),
                token_clone,
            )
            .await;
    });

    // Wait for the signal from the mock
    let received = tokio::time::timeout(Duration::from_secs(5), rx.recv()).await;
    assert!(received.is_ok(), "Should receive signal from mock");
    assert!(received.unwrap().is_some(), "Channel should not be closed");

    // Clean up
    cancellation_token.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(2), poller_handle).await;
}
