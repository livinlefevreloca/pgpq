use arrow_schema::{ArrowError, DataType};
use thiserror::Error;

use crate::pg_schema::PostgresType;

#[derive(Debug, Error)]
pub enum ErrorKind {
    #[error("Type mismatch for column {field}: expected {expected} but got {actual:?}")]
    ColumnTypeMismatch {
        field: String,
        expected: String,
        actual: DataType,
    },
    #[error("Arrow type {tp} for field {field} is not supported (detail: {msg})")]
    TypeNotSupported {
        field: String,
        tp: DataType,
        msg: String,
    },
    #[error("field {field} exceeds the maximum allowed size for binary copy ({size} bytes)")]
    FieldTooLarge { field: String, size: usize },
    #[error("error encoding message: {reason}")]
    Encode {
        // E.g. because Postgres' binary format only supports fields up to 32bits
        reason: String,
    },
    #[error("Type {tp:?} for {field} not supported; supported types are {allowed:?}")]
    EncodingNotSupported {
        field: String,
        tp: PostgresType,
        allowed: Vec<PostgresType>,
    },
    #[error("Encoder {encoder:?} does not support field type {tp:?} for field {field:?}")]
    FieldTypeNotSupported {
        encoder: String,
        tp: DataType,
        field: String,
    },
    #[error("Missing encoder for field {field}")]
    EncoderMissing { field: String },
    #[error("No fields match supplied encoder fields: {fields:?}")]
    UnknownFields { fields: Vec<String> },

    // Decoding
    #[error("Error decoding data: {reason}")]
    Decode { reason: String, name: String },
    #[error("Got invalid binary file header {bytes:?}")]
    InvalidBinaryHeader { bytes: [u8; 11] },
    #[error("Reached EOF in the middle of a tuple. partial tuple: {remaining_bytes:?}")]
    IncompleteDecode { remaining_bytes: Vec<u8> },
    #[error("Expected data size was not found")]
    IncompleteData,
    #[error("Invalid column specification: {spec}")]
    InvalidColumnSpec { spec: String },
    #[error("Invalid column type found while parsing schema: {typ}")]
    UnsupportedColumnType { typ: String },
    #[error("Got an error in an IO Operation: {io_error:?}")]
    IOError { io_error: std::io::Error },
    #[error("Got an error: {name} in Arrow while decoding: {reason}")]
    ArrowErrorDecode { reason: String, name: String },
}

impl From<std::io::Error> for ErrorKind {
    fn from(io_error: std::io::Error) -> Self {
        ErrorKind::IOError { io_error }
    }
}

impl From<ArrowError> for ErrorKind {
    fn from(arrow_error: ArrowError) -> Self {
        ErrorKind::ArrowErrorDecode {
            reason: arrow_error.to_string(),
            name: "ArrowError".to_string(),
        }
    }
}

impl ErrorKind {
    pub(crate) fn field_too_large(field: &str, size: usize) -> ErrorKind {
        ErrorKind::FieldTooLarge {
            field: field.to_string(),
            size,
        }
    }

    pub(crate) fn type_unsupported(field: &str, tp: &DataType, msg: &str) -> ErrorKind {
        ErrorKind::TypeNotSupported {
            field: field.to_string(),
            tp: tp.clone(),
            msg: msg.to_string(),
        }
    }

    pub(crate) fn unsupported_encoding(
        field: &str,
        tp: &PostgresType,
        allowed: &[PostgresType],
    ) -> ErrorKind {
        ErrorKind::EncodingNotSupported {
            field: field.to_string(),
            tp: tp.clone(),
            allowed: allowed.to_owned(),
        }
    }

    pub(crate) fn mismatched_column_type(
        field: &str,
        expected: &str,
        actual: &DataType,
    ) -> ErrorKind {
        ErrorKind::ColumnTypeMismatch {
            field: field.to_string(),
            expected: expected.to_string(),
            actual: actual.clone(),
        }
    }
}
