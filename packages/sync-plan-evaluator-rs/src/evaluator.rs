use crate::expression::evaluate_expression;
use crate::model::{
    BucketInclusionReason, DynamicBucketQuery, EvaluateRowOptions, EvaluatedParameters,
    EvaluatedRow, JsonMap, LookupRequest, LookupResults, ParameterLookupScope,
    PrepareBucketQueryOptions, PreparedBucketQueries, RequestParameters, ResolvedBucket,
    ScopedParameterLookup, SerializedExpandingLookup, SerializedParameterValue, SerializedSyncPlan,
    SourceTable,
};
use crate::value::{
    filter_json_row, id_from_data, normalize_parameter_value, serialize_value_array, sqlite_bool,
};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use thiserror::Error;

#[derive(Debug, Clone)]
pub struct EvaluatorOptions {
    pub default_schema: String,
}

impl Default for EvaluatorOptions {
    fn default() -> Self {
        Self {
            default_schema: "public".to_string(),
        }
    }
}

#[derive(Debug)]
pub struct SyncPlanEvaluator {
    plan: SerializedSyncPlan,
    options: EvaluatorOptions,
}

impl SyncPlanEvaluator {
    pub fn from_serialized_json(
        plan_json: &str,
        options: EvaluatorOptions,
    ) -> EvaluatorResult<Self> {
        let plan: SerializedSyncPlan = serde_json::from_str(plan_json)?;
        Self::from_plan(plan, options)
    }

    pub fn from_plan(plan: SerializedSyncPlan, options: EvaluatorOptions) -> EvaluatorResult<Self> {
        if plan.version != "unstable" {
            return Err(EvaluatorError::UnknownPlanVersion(plan.version));
        }

        Ok(Self { plan, options })
    }
}

include!("evaluator/core_row.rs");
include!("evaluator/parameter_rows.rs");
include!("evaluator/prepare_queries.rs");
include!("evaluator/resolve_queries.rs");
include!("evaluator/helpers.rs");
include!("evaluator/errors.rs");

#[cfg(test)]
mod tests {
    use super::*;

    include!("evaluator/tests_internal.rs");
}
