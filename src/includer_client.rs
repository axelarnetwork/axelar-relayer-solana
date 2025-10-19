use async_trait::async_trait;

use axelar_solana_gateway_v2::IncomingMessage;
use bytemuck;
use relayer_core::{error::ClientError, utils::ThreadSafe};
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::CommitmentConfig, pubkey::Pubkey, signature::Signature,
    transaction::Transaction,
};
use std::{sync::Arc, time::Duration};
use tracing::warn;

#[async_trait]
#[cfg_attr(any(test), mockall::automock)]
pub trait IncluderClientTrait: ThreadSafe {
    fn inner(&self) -> &RpcClient;
    async fn send_transaction(&self, transaction: Transaction) -> Result<Signature, ClientError>;
    async fn incoming_message_already_executed(
        &self,
        incoming_message_pda: &Pubkey,
    ) -> Result<bool, ClientError>;
    async fn get_signature_status(&self, signature: &Signature) -> Result<(), ClientError>;
}

#[derive(Clone)]
pub struct IncluderClient {
    client: Arc<RpcClient>,
    max_retries: usize,
    commitment: CommitmentConfig,
}

impl IncluderClient {
    pub fn new(
        url: &str,
        commitment: CommitmentConfig,
        max_retries: usize,
    ) -> Result<Self, ClientError> {
        Ok(Self {
            client: Arc::new(RpcClient::new_with_commitment(url.to_string(), commitment)),
            max_retries,
            commitment,
        })
    }
}

#[async_trait]
impl IncluderClientTrait for IncluderClient {
    fn inner(&self) -> &RpcClient {
        &self.client
    }

    async fn send_transaction(&self, transaction: Transaction) -> Result<Signature, ClientError> {
        let mut retries = 0;
        let mut delay = Duration::from_millis(500);

        loop {
            let res = self
                .client
                .send_and_confirm_transaction(&transaction)
                .await
                .map_err(|e| ClientError::BadRequest(e.to_string()));
            match res {
                Ok(signature) => return Ok(signature),
                Err(e) => {
                    if retries >= self.max_retries {
                        warn!(
                            "Failed to send transaction after {} retries: {}",
                            retries, e
                        );
                        return Err(ClientError::BadRequest(e.to_string()));
                    }

                    warn!(
                        "Transaction send failed (retry {}/{}): {}. Retrying in {:?}...",
                        retries + 1,
                        self.max_retries,
                        e,
                        delay
                    );

                    tokio::time::sleep(delay).await;
                    retries += 1;
                    delay = delay.mul_f32(2.0);
                }
            }
        }
    }

    async fn incoming_message_already_executed(
        &self,
        incoming_message_pda: &Pubkey,
    ) -> Result<bool, ClientError> {
        let raw_incoming_message = self
            .inner()
            .get_account_data(incoming_message_pda)
            .await
            .map_err(|e| ClientError::BadRequest(e.to_string()))?;
        let incoming_message = read(&raw_incoming_message).ok_or_else(|| {
            ClientError::BadRequest("Could not read incoming message".to_string())
        })?;
        Ok(incoming_message.status.is_executed())
    }

    async fn get_signature_status(&self, signature: &Signature) -> Result<(), ClientError> {
        let status = self
            .inner()
            .get_signature_status_with_commitment(signature, self.commitment)
            .await
            .map_err(|e| ClientError::BadRequest(e.to_string()))?;

        match status {
            Some(Ok(_)) => Ok(()),
            Some(Err(e)) => Err(ClientError::BadRequest(e.to_string())),
            None => Err(ClientError::BadRequest("Unknown transaction status".into())),
        }
    }
}

fn read(data: &[u8]) -> Option<&IncomingMessage> {
    let result: &IncomingMessage = bytemuck::try_from_bytes(data).ok()?;
    Some(result)
}
