use crate::error::TransactionParsingError;

#[derive(Clone, Copy, Debug)]
pub struct InstructionIndex {
    pub outer_index: u64,
    pub inner_index: u64,
}

impl InstructionIndex {
    pub fn new(outer_index: u64, inner_index: u64) -> Self {
        Self {
            outer_index,
            inner_index,
        }
    }

    pub fn serialize(&self) -> String {
        format!("{}.{}", self.outer_index, self.inner_index)
    }

    pub fn deserialize(index: String) -> Result<Self, TransactionParsingError> {
        let parts: Vec<&str> = index.split('.').collect();

        if parts.len() != 2 {
            return Err(TransactionParsingError::Message(format!(
                "Invalid instruction index format: '{}'. Expected format: 'outer.inner'",
                index
            )));
        }

        let outer_index = parts
            .first()
            .ok_or_else(|| {
                TransactionParsingError::Message(
                    "Invalid outer index. Must be a valid u64".to_string(),
                )
            })?
            .parse::<u64>()
            .map_err(|_| {
                TransactionParsingError::Message(
                    "Invalid outer index. Must be a valid u64".to_string(),
                )
            })?;

        let inner_index = parts
            .get(1)
            .ok_or_else(|| {
                TransactionParsingError::Message(
                    "Invalid inner index. Must be a valid u64".to_string(),
                )
            })?
            .parse::<u64>()
            .map_err(|_| {
                TransactionParsingError::Message(
                    "Invalid inner index. Must be a valid u64".to_string(),
                )
            })?;

        Ok(Self {
            outer_index,
            inner_index,
        })
    }
}
