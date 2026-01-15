use thiserror::Error;

#[derive(Error, Debug)]
pub enum TransactionParsingError {
    #[error("MessageParsingError: {0}")]
    Message(String),
    #[error("GasError: {0}")]
    Gas(String),
    #[error("ITSWithoutPair: {0}")]
    ITSWithoutPair(String),
    #[error("GeneralError: {0}")]
    Generic(String),
    #[error("InvalidAccountAddress: {0}")]
    InvalidAccountAddress(String),
    #[error("InvalidInstructionData: {0}")]
    InvalidInstructionData(String),
    #[error("IndexOverflow: {0}")]
    IndexOverflow(String),
    #[error("InvalidTransaction: {0}")]
    InvalidTransaction(String),
    #[error("CostCacheError: {0}")]
    CostCacheError(String),
}
