use std::{str::FromStr, sync::Arc, time::Duration};

use crate::types::SolanaTransaction;
use crate::{
    models::{
        solana_subscriber_cursor::{AccountPollerEnum, SubscriberCursor},
        solana_transaction::SolanaTransactionModel,
    },
    poll_client::SolanaRpcClientTrait,
    utils::upsert_and_publish,
};
use anyhow::anyhow;
use async_trait::async_trait;
use relayer_core::{error::SubscriberError, queue::Queue};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use tokio::select;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

#[async_trait]
pub trait TransactionPoller {
    type Transaction;
    type Account;

    async fn poll_account(
        &self,
        account: Self::Account,
        account_type: AccountPollerEnum,
    ) -> Result<Vec<Self::Transaction>, anyhow::Error>;

    async fn poll_tx(&self, tx_hash: String) -> Result<Self::Transaction, anyhow::Error>;
}

pub struct SolanaPoller<RPC: SolanaRpcClientTrait, SC: SubscriberCursor, SM: SolanaTransactionModel>
{
    client: RPC,
    cursor_model: Arc<SC>,
    context: String,
    transaction_model: Arc<SM>,
    queue: Arc<Queue>,
}

impl<RPC: SolanaRpcClientTrait, SC: SubscriberCursor, SM: SolanaTransactionModel>
    SolanaPoller<RPC, SC, SM>
{
    pub async fn new(
        client: RPC,
        context: String,
        transaction_model: Arc<SM>,
        cursor_model: Arc<SC>,
        queue: Arc<Queue>,
    ) -> Result<Self, SubscriberError> {
        Ok(SolanaPoller {
            client,
            context,
            cursor_model,
            transaction_model,
            queue,
        })
    }

    pub async fn run(
        &self,
        gas_service_account: Pubkey,
        gateway_account: Pubkey,
        its_account: Pubkey,
        cancellation_token: CancellationToken,
    ) {
        select! {
            _ = self.work(gas_service_account, AccountPollerEnum::GasService, cancellation_token.clone()) => {
                warn!("Gas service subscriber stream ended");
            },
            _ = self.work(gateway_account, AccountPollerEnum::Gateway, cancellation_token.clone()) => {
                warn!("Gateway subscriber stream ended");
            },
            _ = self.work(its_account, AccountPollerEnum::ITS, cancellation_token.clone()) => {
                warn!("ITS subscriber stream ended");
            }
        };
    }

    // Poller runs in the background and polls every X seconds for all transactions
    // that happened for the specified account since its last poll. It only writes
    // to the queue if the transaction has not been processed already.
    // It acts as a backup to the listener for when it's initiated or restarted to not
    // miss any transactions.

    // TODO: Create a general stream work function and separate it from poll work
    async fn work(
        &self,
        account: Pubkey,
        account_type: AccountPollerEnum,
        cancellation_token: CancellationToken,
    ) {
        loop {
            if cancellation_token.is_cancelled() {
                warn!("Cancellation requested; no longer polling account");
                break;
            }
            let transactions = match self.poll_account(account, account_type.clone()).await {
                Ok(transactions) => transactions,
                Err(e) => {
                    error!("Error polling account: {:?}", e);
                    continue;
                }
            };

            for tx in transactions.clone() {
                match upsert_and_publish(
                    &self.transaction_model,
                    &self.queue,
                    &tx,
                    "poller".to_string(),
                )
                .await
                {
                    Ok(inserted) => {
                        if inserted {
                            info!("Upserted and published transaction by poller: {:?}", tx);
                        }
                    }
                    Err(e) => {
                        error!("Error upserting and publishing transaction: {:?}", e);
                        continue;
                    }
                }
            }
            let maybe_transaction_with_max_slot = transactions.iter().max_by_key(|tx| tx.slot);
            if let Some(transaction) = maybe_transaction_with_max_slot {
                if let Err(err) = self
                    .store_last_transaction_checked(transaction.signature, account_type.clone())
                    .await
                {
                    error!("{:?}", err);
                    continue;
                }
            }
            tokio::time::sleep(Duration::from_secs(10)).await;
        }
    }

    pub async fn store_last_transaction_checked(
        &self,
        signature: Signature,
        account_type: AccountPollerEnum,
    ) -> Result<(), anyhow::Error> {
        self.cursor_model
            .store_latest_signature(self.context.clone(), signature.to_string(), account_type)
            .await
            .map_err(|e| anyhow!("Error storing latest signature: {:?}", e))
    }
}

#[async_trait]
impl<RPC: SolanaRpcClientTrait, SC: SubscriberCursor, SM: SolanaTransactionModel> TransactionPoller
    for SolanaPoller<RPC, SC, SM>
{
    type Transaction = SolanaTransaction;
    type Account = Pubkey;

    async fn poll_account(
        &self,
        account_id: Pubkey,
        account_type: AccountPollerEnum,
    ) -> Result<Vec<Self::Transaction>, anyhow::Error> {
        let last_signature_checked = self
            .cursor_model
            .get_latest_signature(self.context.clone(), account_type)
            .await?
            .map(|signature| Signature::from_str(&signature))
            .transpose()?;

        let transactions = self
            .client
            .get_transactions_for_account(&account_id, None, last_signature_checked)
            .await?;

        Ok(transactions)
    }

    async fn poll_tx(&self, signature: String) -> Result<Self::Transaction, anyhow::Error> {
        let transaction = self
            .client
            .get_transaction_by_signature(Signature::from_str(&signature)?)
            .await?;

        Ok(transaction)
    }
}

#[cfg(test)]
mod tests {

    use std::str::FromStr;

    use crate::models::solana_subscriber_cursor::MockSubscriberCursor;
    use crate::models::solana_transaction::MockSolanaTransactionModel;
    use crate::poll_client::MockSolanaRpcClientTrait;

    use super::*;

    #[tokio::test]
    async fn test_poll_account() {
        let mut mock_cursor_model = MockSubscriberCursor::new();
        let mock_transaction_model = MockSolanaTransactionModel::new();
        let queue = Queue::new("amqp://guest:guest@localhost:5672", "test", 1).await;
        let mut mock_client = MockSolanaRpcClientTrait::new();

        mock_client
            .expect_get_transactions_for_account()
            .withf(|account_id, before, last_signature_checked| {
                *account_id
                    == Pubkey::from_str("DaejccUfXqoAFTiDTxDuMQfQ9oa6crjtR9cT52v1AvGK").unwrap()
                    && before.is_none()
                    && *last_signature_checked == Some(Signature::default())
            })
            .returning(|_, _, _| {
                Box::pin(async {
                    Ok(vec![SolanaTransaction {
                        signature: Signature::default(),
                        slot: 1,
                        logs: vec![],
                        ixs: vec![],
                        account_keys: vec![],
                        cost_units: 0,
                        timestamp: None,
                    }])
                })
            });

        mock_cursor_model
            .expect_get_latest_signature()
            .withf(|context, account_type| {
                context == "test" && *account_type == AccountPollerEnum::GasService
            })
            .returning(|_, _| Box::pin(async { Ok(Some(Signature::default().to_string())) }));

        let subscriber_poller = SolanaPoller::new(
            mock_client,
            "test".to_string(),
            Arc::new(mock_transaction_model),
            Arc::new(mock_cursor_model),
            queue,
        )
        .await
        .unwrap();

        let transactions = subscriber_poller
            .poll_account(
                Pubkey::from_str("DaejccUfXqoAFTiDTxDuMQfQ9oa6crjtR9cT52v1AvGK").unwrap(),
                AccountPollerEnum::GasService,
            )
            .await
            .unwrap();

        assert_eq!(transactions.len(), 1);
        assert_eq!(
            transactions[0],
            SolanaTransaction {
                signature: Signature::default(),
                slot: 1,
                logs: vec![],
                ixs: vec![],
                account_keys: vec![],
                cost_units: 0,
                timestamp: None,
            }
        );
    }

    #[tokio::test]
    async fn test_poll_tx() {
        let mock_cursor_model = MockSubscriberCursor::new();
        let mock_transaction_model = MockSolanaTransactionModel::new();
        let queue = Queue::new("amqp://guest:guest@localhost:5672", "test", 1).await;
        let mut mock_client = MockSolanaRpcClientTrait::new();

        mock_client
            .expect_get_transaction_by_signature()
            .withf(|signature| *signature == Signature::default())
            .returning(|_| {
                Box::pin(async {
                    Ok(SolanaTransaction {
                        signature: Signature::default(),
                        slot: 1,
                        logs: vec![],
                        ixs: vec![],
                        account_keys: vec![],
                        cost_units: 0,
                        timestamp: None,
                    })
                })
            });

        let subscriber_poller = SolanaPoller::new(
            mock_client,
            "test".to_string(),
            Arc::new(mock_transaction_model),
            Arc::new(mock_cursor_model),
            queue,
        )
        .await
        .unwrap();

        let tx = subscriber_poller
            .poll_tx(Signature::default().to_string())
            .await
            .unwrap();

        assert_eq!(
            tx,
            SolanaTransaction {
                signature: Signature::default(),
                slot: 1,
                logs: vec![],
                ixs: vec![],
                account_keys: vec![],
                cost_units: 0,
                timestamp: None,
            }
        );
    }

    // #[tokio::test]
    // async fn test_poll_tx_malformed_signature() {
    //     let mock_cursor_model = MockSubscriberCursor::new();
    //     let mock_transaction_model = MockSolanaTransactionModel::new();
    //     let queue = Queue::new("amqp://guest:guest@localhost:5672", "test", 1).await;
    //     let mock_client = MockSolanaRpcClientTrait::new();

    //     let subscriber_poller = SolanaPoller::new(
    //         mock_client,
    //         "test".to_string(),
    //         Arc::new(mock_transaction_model),
    //         Arc::new(mock_cursor_model),
    //         queue,
    //     )
    //     .await
    //     .unwrap();

    //     let res = subscriber_poller
    //         .poll_tx("malformed_signature".to_string())
    //         .await;
    //     assert!(res.is_err());
    // }
}
