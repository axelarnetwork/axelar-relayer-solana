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
}

#[derive(Error, Debug)]
pub enum GasError {
    #[error("ConversionError: {0}")]
    ConversionError(String),
    #[error("GasCalculationError: {0}")]
    GasCalculationError(String),
}
