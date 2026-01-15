use crate::error::TransactionParsingError;

#[derive(Clone, Debug, PartialEq)]
pub struct InstructionIndex {
    pub signature: String,
    pub outer_index: u8,
    pub inner_index: u8,
}

impl InstructionIndex {
    pub fn new(signature: String, outer_index: u8, inner_index: u8) -> Self {
        Self {
            signature,
            outer_index,
            inner_index,
        }
    }

    pub fn serialize(&self) -> String {
        format!(
            "{}-{}.{}",
            self.signature, self.outer_index, self.inner_index
        )
    }

    pub fn deserialize(index: String) -> Result<Self, TransactionParsingError> {
        let parts = index.split('-').collect::<Vec<&str>>();
        let signature = parts.first().ok_or_else(|| {
            TransactionParsingError::Message(
                "Invalid signature. Must be a valid string".to_string(),
            )
        })?;
        let indices = parts.get(1).ok_or_else(|| {
            TransactionParsingError::Message("Invalid indices. Must be a valid string".to_string())
        })?;
        let parts: Vec<&str> = indices.split('.').collect();

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
                    "Invalid outer index. Must be a valid u8".to_string(),
                )
            })?
            .parse::<u8>()
            .map_err(|_| {
                TransactionParsingError::Message(
                    "Invalid outer index. Must be a valid u8".to_string(),
                )
            })?;

        let inner_index = parts
            .get(1)
            .ok_or_else(|| {
                TransactionParsingError::Message(
                    "Invalid inner index. Must be a valid u8".to_string(),
                )
            })?
            .parse::<u8>()
            .map_err(|_| {
                TransactionParsingError::Message(
                    "Invalid inner index. Must be a valid u8".to_string(),
                )
            })?;

        Ok(Self {
            signature: signature.to_string(),
            outer_index,
            inner_index,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn is_msg(err: &TransactionParsingError, needle: &str) -> bool {
        match err {
            TransactionParsingError::Message(m) => m.contains(needle),
            _ => false,
        }
    }

    #[test]
    fn happy_path_valid_example() {
        // Well-formed example
        let s = "3Yoe1V1qMFERAVXadHkrnXWQ2STa7Yd8rydoWxouXQrpwtDZGpuVPdmdJSA9HiNQi91aFP5EumZrvAqZcQa84Ens-2.1";
        let idx = InstructionIndex::deserialize(s.to_string()).expect("should parse");
        assert_eq!(
            idx.signature,
            "3Yoe1V1qMFERAVXadHkrnXWQ2STa7Yd8rydoWxouXQrpwtDZGpuVPdmdJSA9HiNQi91aFP5EumZrvAqZcQa84Ens"
        );
        assert_eq!(idx.outer_index, 2);
        assert_eq!(idx.inner_index, 1);
        assert_eq!(idx.serialize(), s);
    }

    #[test]
    fn happy_path_roundtrip_various() {
        let cases = [
            ("Sig1", 0, 0),
            ("Sig2", 123, 125),
            ("LongerSignatureXYZ", 255, 1),
        ];

        for (sig, outer, inner) in cases {
            let idx = InstructionIndex::new(sig.to_string(), outer, inner);
            let serialized = idx.serialize();
            let deserialized = InstructionIndex::deserialize(serialized).unwrap();
            assert_eq!(deserialized, idx);
        }
    }

    #[test]
    fn sad_path_missing_dash() {
        let err = InstructionIndex::deserialize("InvalidNoDashHere".to_string()).unwrap_err();
        assert!(is_msg(&err, "Invalid indices"), "{err:?}");
    }

    #[test]
    fn sad_path_missing_dot() {
        let err = InstructionIndex::deserialize("SignatureOnly-123".to_string()).unwrap_err();
        assert!(is_msg(&err, "Expected"), "{err:?}");
    }

    #[test]
    fn sad_path_non_numeric_outer() {
        let err = InstructionIndex::deserialize("Sig-abc.5".to_string()).unwrap_err();
        assert!(is_msg(&err, "outer index"), "{err:?}");
    }

    #[test]
    fn sad_path_non_numeric_inner() {
        let err = InstructionIndex::deserialize("Sig-5.xyz".to_string()).unwrap_err();
        assert!(is_msg(&err, "inner index"), "{err:?}");
    }

    #[test]
    fn sad_path_extra_dots() {
        let err = InstructionIndex::deserialize("Sig-1.2.3".to_string()).unwrap_err();
        assert!(is_msg(&err, "format"), "{err:?}");
    }

    #[test]
    fn sad_path_empty_outer_or_inner() {
        let err1 = InstructionIndex::deserialize("Sig-.2".to_string()).unwrap_err();
        assert!(is_msg(&err1, "outer"), "{err1:?}");

        let err2 = InstructionIndex::deserialize("Sig-2.".to_string()).unwrap_err();
        assert!(is_msg(&err2, "inner"), "{err2:?}");
    }
}
