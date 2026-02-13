use pg_query::protobuf;
use pg_query::protobuf::node::Node as PgNodeEnum;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use sync_plan_evaluator_rs::model::{
    CaseWhenBranch, ColumnSource, ExternalSource, LookupReference, ParameterLookupScope,
    PartitionKey, SerializedBucketDataSource, SerializedDataSource, SerializedExpandingLookup,
    SerializedParameterIndexLookupCreator, SerializedParameterValue, SerializedStream,
    SerializedStreamQuerier, SerializedSyncPlan, SerializedTablePattern, SqlExpression,
    StreamOptions,
};
use thiserror::Error;

#[derive(Debug, Clone)]
pub struct CompileOptions {
    pub default_schema: String,
}

impl Default for CompileOptions {
    fn default() -> Self {
        Self {
            default_schema: "public".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Severity {
    Warning,
    Fatal,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CompileMessage {
    pub severity: Severity,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CompileResult {
    pub plan: SerializedSyncPlan,
    #[serde(rename = "planJson")]
    pub plan_json: String,
    pub messages: Vec<CompileMessage>,
}

include!("compiler/canonicalize.rs");
include!("compiler/yaml.rs");
include!("compiler/ast.rs");
include!("compiler/sql_parse_select.rs");
include!("compiler/sql_parse_expr.rs");
include!("compiler/sql_parse_utils.rs");
include!("compiler/expr_transform.rs");
include!("compiler/bindings_compile.rs");
include!("compiler/bindings_lookup.rs");
include!("compiler/bindings_analysis.rs");
include!("compiler/classify.rs");
include!("compiler/compile.rs");

#[derive(Debug, Error)]
pub enum CompilerError {
    #[error("yaml parse error: {0}")]
    Parse(String),
    #[error("sql parse error: {0}")]
    Sql(String),
    #[error("unsupported feature: {0}")]
    Unsupported(String),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    include!("compiler/tests_internal.rs");
}
