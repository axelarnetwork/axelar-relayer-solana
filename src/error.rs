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
}

#[derive(Error, Debug)]
pub enum GasError {
    #[error("ConversionError: {0}")]
    ConversionError(String),
    #[error("GasCalculationError: {0}")]
    GasCalculationError(String),
}

#[derive(Error, Debug)]
pub enum GasCalculatorError {
    #[error("GenericError: {0}")]
    Generic(String),
}

#[derive(Error, Debug)]
pub enum GatewayTxError {
    #[error("InitializePayloadVerificationSessionError: {0}")]
    InitializePayloadVerificationSessionError(String),
    #[error("VerifySignatureError: {0}")]
    VerifySignatureError(String),
    #[error("ApproveMessageError: {0}")]
    ApproveMessageError(String),
}

#[derive(Error, Debug)]
pub enum IncluderClientError {
    #[error("GasExceededError: {0}")]
    GasExceededError(String),
    #[error("MaxRetriesExceededError: {0}")]
    MaxRetriesExceededError(String),
    #[error("GenericError: {0}")]
    GenericError(String),
}

#[derive(Error, Debug)]
pub enum TransactionBuilderError {
    #[error("ClientError: {0}")]
    ClientError(String),
    #[error("GenericError: {0}")]
    GenericError(String),
}
