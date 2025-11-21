use solana_sdk::transaction::TransactionError;
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
    #[error("MaxRetriesExceededError: {0}")]
    MaxRetriesExceededError(String),
    #[error("TransactionError: {0}")]
    TransactionError(TransactionError),
    #[error("GenericError: {0}")]
    GenericError(String),
}

#[derive(Error, Debug, Clone)]
pub enum SolanaIncluderError {
    #[error("TransactionError: {0}")]
    TransactionError(TransactionError),
    #[error("GenericError: {0}")]
    GenericError(String),
}

#[derive(Error, Debug)]
pub enum TransactionBuilderError {
    #[error("PayloadDecodeError: {0}")]
    PayloadDecodeError(String),
    #[error("SimulationError: {0}")]
    SimulationError(String),
    #[error("ClientError: {0}")]
    ClientError(String),
    #[error("GenericError: {0}")]
    GenericError(String),
}

#[derive(Error, Debug)]
pub enum RedisInterfaceError {
    #[error("GetAltEntryError: {0}")]
    GetAltEntryError(String),
    #[error("WriteAltEntryError: {0}")]
    WriteAltEntryError(String),
    #[error("SetAltInactiveError: {0}")]
    SetAltInactiveError(String),
    #[error("SetAltFailedError: {0}")]
    SetAltFailedError(String),
    #[error("RemoveAltKeyError: {0}")]
    RemoveAltKeyError(String),
    #[error("UpdateAltRetryCountError: {0}")]
    UpdateAltRetryCountError(String),
    #[error("GenericError: {0}")]
    GenericError(String),
}

#[derive(Error, Debug)]
pub enum FeesClientError {
    #[error("GenericError: {0}")]
    GenericError(String),
}
