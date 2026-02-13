use crate::evaluator::{EvalContext, EvaluatorError, EvaluatorResult};
use crate::model::{CaseWhenBranch, ExternalSource, SqlExpression};
use crate::value::{
    cast_as_f64, cast_as_i64, cast_as_text, cast_numeric, compare_sql_values, sqlite_bool,
    sqlite_not, NumericValue,
};
use serde_json::{Number, Value};
use std::cmp::Ordering;

include!("expression/evaluate.rs");
include!("expression/numeric.rs");

#[cfg(test)]
mod tests {
    include!("expression/tests_internal.rs");
}
