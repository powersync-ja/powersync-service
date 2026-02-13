use napi_derive::napi;
use serde::{Deserialize, Serialize};

use crate::model::{
    EvaluateRowOptions, EvaluatedParameters, LookupResults, PrepareBucketQueryOptions,
    PreparedBucketQueries, SourceTable,
};
use crate::{EvaluatorOptions, SyncPlanEvaluator};

#[napi(object)]
pub struct JsEvaluatorOptions {
    pub default_schema: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ParameterRowInput {
    source_table: SourceTable,
    record: crate::model::JsonMap,
}

#[napi]
pub struct NativeSyncPlanEvaluator {
    inner: SyncPlanEvaluator,
}

#[napi]
impl NativeSyncPlanEvaluator {
    #[napi(constructor)]
    pub fn new(plan_json: String, options: Option<JsEvaluatorOptions>) -> napi::Result<Self> {
        let evaluator = SyncPlanEvaluator::from_serialized_json(
            &plan_json,
            EvaluatorOptions {
                default_schema: options
                    .and_then(|value| value.default_schema)
                    .unwrap_or_else(|| "public".to_string()),
            },
        )
        .map_err(|err| napi::Error::from_reason(err.to_string()))?;

        Ok(Self { inner: evaluator })
    }

    #[napi(js_name = "evaluateRowJson")]
    pub fn evaluate_row_json(&self, options_json: String) -> napi::Result<String> {
        let options: EvaluateRowOptions = parse_json("evaluateRow options", &options_json)?;
        let result = self
            .inner
            .evaluate_row(options)
            .map_err(|err| napi::Error::from_reason(err.to_string()))?;
        to_json(&result)
    }

    #[napi(js_name = "evaluateParameterRowJson")]
    pub fn evaluate_parameter_row_json(&self, options_json: String) -> napi::Result<String> {
        let options: ParameterRowInput = parse_json("evaluateParameterRow options", &options_json)?;
        let result: Vec<EvaluatedParameters> = self
            .inner
            .evaluate_parameter_row(&options.source_table, &options.record)
            .map_err(|err| napi::Error::from_reason(err.to_string()))?;
        to_json(&result)
    }

    #[napi(js_name = "prepareBucketQueriesJson")]
    pub fn prepare_bucket_queries_json(&self, options_json: String) -> napi::Result<String> {
        let options: PrepareBucketQueryOptions = parse_json("prepareBucketQueries options", &options_json)?;
        let prepared = self
            .inner
            .prepare_bucket_queries(options)
            .map_err(|err| napi::Error::from_reason(err.to_string()))?;
        to_json(&prepared)
    }

    #[napi(js_name = "resolveBucketQueriesJson")]
    pub fn resolve_bucket_queries_json(
        &self,
        prepared_json: String,
        results_json: String,
    ) -> napi::Result<String> {
        let prepared: PreparedBucketQueries =
            parse_json("resolveBucketQueries prepared", &prepared_json)?;
        let results: Vec<LookupResults> = parse_json("resolveBucketQueries results", &results_json)?;
        let resolved = self
            .inner
            .resolve_bucket_queries(&prepared, &results)
            .map_err(|err| napi::Error::from_reason(err.to_string()))?;
        to_json(&resolved)
    }
}

fn parse_json<T: for<'de> Deserialize<'de>>(label: &str, value: &str) -> napi::Result<T> {
    serde_json::from_str(value).map_err(|err| {
        napi::Error::from_reason(format!("invalid JSON for {label}: {}", err))
    })
}

fn to_json<T: Serialize>(value: &T) -> napi::Result<String> {
    serde_json::to_string(value)
        .map_err(|err| napi::Error::from_reason(format!("failed to serialize JSON: {}", err)))
}
