use thiserror::Error;

use crate::sync_plan::SqliteValue;

#[derive(Error, Debug)]
pub enum EvaluationError<'a> {
    #[error("{name} expects {expected} arguments, got {actual}")]
    ParameterCount {
        name: &'static str,
        expected: usize,
        actual: usize,
    },
    #[error("Expected {name} to be called with a string, got {value}")]
    WrongType {
        name: &'static str,
        value: SqliteValue<'a>,
    },
    #[error("Error parsing JSON")]
    FromJson(#[from] serde_json::Error),

    #[error("Error parsing BSON")]
    FromBsn(#[from] bson::error::Error),
}
