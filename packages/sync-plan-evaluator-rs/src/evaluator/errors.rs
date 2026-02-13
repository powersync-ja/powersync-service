#[derive(Debug, Error)]
pub enum EvaluatorError {
    #[error("json parse error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("unknown sync plan version: {0}")]
    UnknownPlanVersion(String),
    #[error("invalid sync plan: {0}")]
    InvalidPlan(String),
    #[error("unsupported expression: {0}")]
    UnsupportedExpression(String),
    #[error("invalid literal: {0}")]
    InvalidLiteral(String),
}

pub type EvaluatorResult<T> = Result<T, EvaluatorError>;
