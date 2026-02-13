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

    pub fn evaluate_row(&self, options: EvaluateRowOptions) -> EvaluatorResult<Vec<EvaluatedRow>> {
        let mut results = Vec::new();

        for bucket in &self.plan.buckets {
            for source_index in &bucket.sources {
                let source = self.plan.data_sources.get(*source_index).ok_or_else(|| {
                    EvaluatorError::InvalidPlan(format!(
                        "missing data source index: {source_index}"
                    ))
                })?;

                if !table_matches(&source.table, &options.source_table) {
                    continue;
                }

                let context = EvalContext {
                    row: Some(&options.record),
                    request: &RequestParameters::default(),
                };

                if !all_filters_match(&source.filters, &context)? {
                    continue;
                }

                let mut data = Map::new();
                for column in &source.columns {
                    match column {
                        crate::model::ColumnSource::Star(v) if v == "star" => {
                            for (key, value) in filter_json_row(&options.record) {
                                data.entry(key).or_insert(value);
                            }
                        }
                        crate::model::ColumnSource::Expression { expr, alias } => {
                            let value = evaluate_expression(expr, &context)?;
                            if matches!(value, Value::Null | Value::String(_) | Value::Number(_)) {
                                data.insert(alias.clone(), value);
                            }
                        }
                        _ => {}
                    }
                }

                let mut partition_values = Vec::new();
                let mut partition_valid = true;
                for partition in &source.partition_by {
                    let value = evaluate_expression(&partition.expr, &context)?;
                    if let Some(normalized) = normalize_parameter_value(&value) {
                        partition_values.push(normalized);
                    } else {
                        partition_valid = false;
                        break;
                    }
                }

                if !partition_valid {
                    continue;
                }

                let bucket = format!(
                    "{}{}",
                    bucket.unique_name,
                    serialize_value_array(&partition_values)
                );
                let id = id_from_data(&data);
                let output_table = source
                    .output_table_name
                    .clone()
                    .unwrap_or_else(|| options.source_table.name.clone());

                results.push(EvaluatedRow {
                    bucket,
                    table: output_table,
                    id,
                    data,
                });
            }
        }

        Ok(results)
    }

    pub fn evaluate_parameter_row(
        &self,
        source_table: &SourceTable,
        row: &JsonMap,
    ) -> EvaluatorResult<Vec<EvaluatedParameters>> {
        let mut results = Vec::new();

        for parameter_index in &self.plan.parameter_indexes {
            if !table_matches(&parameter_index.table, source_table) {
                continue;
            }

            let context = EvalContext {
                row: Some(row),
                request: &RequestParameters::default(),
            };

            if !all_filters_match(&parameter_index.filters, &context)? {
                continue;
            }

            let mut output_values = Vec::new();
            for output in &parameter_index.output {
                let value = evaluate_expression(output, &context)?;
                if let Some(value) = normalize_parameter_value(&value) {
                    output_values.push(value);
                } else {
                    output_values.clear();
                    break;
                }
            }
            if output_values.is_empty() && !parameter_index.output.is_empty() {
                continue;
            }

            let mut partition_values = Vec::new();
            for partition in &parameter_index.partition_by {
                let value = evaluate_expression(&partition.expr, &context)?;
                if let Some(value) = normalize_parameter_value(&value) {
                    partition_values.push(value);
                } else {
                    partition_values.clear();
                    break;
                }
            }
            if partition_values.len() != parameter_index.partition_by.len() {
                continue;
            }

            let scope = &parameter_index.lookup_scope;
            let lookup_values = scoped_lookup_values(scope, &partition_values);
            let lookup = ScopedParameterLookup {
                serialized_representation: serialize_value_array(&lookup_values),
                values: lookup_values,
            };

            let mut bucket_param_row = JsonMap::new();
            for (index, value) in output_values.into_iter().enumerate() {
                bucket_param_row.insert(index.to_string(), value);
            }

            results.push(EvaluatedParameters {
                lookup,
                bucket_parameters: vec![bucket_param_row],
            });
        }

        Ok(results)
    }

    pub fn prepare_bucket_queries(
        &self,
        options: PrepareBucketQueryOptions,
    ) -> EvaluatorResult<PreparedBucketQueries> {
        let mut static_buckets = Vec::new();
        let mut dynamic_queries = Vec::new();

        for stream in &self.plan.streams {
            let requested_streams = options
                .streams
                .get(&stream.stream.name)
                .cloned()
                .unwrap_or_default();

            let mut stream_subscriptions: Vec<(RequestParameters, BucketInclusionReason)> =
                Vec::new();

            for requested in &requested_streams {
                let mut parameters = options.global_parameters.clone();
                parameters.subscription = requested
                    .parameters
                    .clone()
                    .unwrap_or(Value::Object(Map::new()));
                stream_subscriptions.push((
                    parameters,
                    BucketInclusionReason::Subscription {
                        subscription: requested.opaque_id,
                    },
                ));
            }

            let has_explicit_default = requested_streams
                .iter()
                .any(|stream| stream.parameters.is_none());
            if stream.stream.is_subscribed_by_default
                && options.has_default_streams
                && !has_explicit_default
            {
                stream_subscriptions.push((
                    options.global_parameters.clone(),
                    BucketInclusionReason::Default("default".to_string()),
                ));
            }

            for (subscription_params, inclusion_reason) in stream_subscriptions {
                for querier in &stream.queriers {
                    let context = EvalContext {
                        row: None,
                        request: &subscription_params,
                    };

                    if !all_filters_match(&querier.request_filters, &context)? {
                        continue;
                    }

                    if querier.lookup_stages.is_empty() {
                        let values = self
                            .instantiate_without_lookups(&querier.source_instantiation, &context)?;
                        for instance in cartesian_product(values) {
                            let bucket = self.bucket_from_querier(querier.bucket, &instance)?;
                            static_buckets.push(ResolvedBucket {
                                bucket,
                                priority: stream.stream.priority,
                                definition: stream.stream.name.clone(),
                                inclusion_reasons: vec![inclusion_reason.clone()],
                            });
                        }
                    } else {
                        let lookup_requests = self.prepare_lookup_requests(querier, &context)?;

                        let bucket_prefix = self
                            .plan
                            .buckets
                            .get(querier.bucket)
                            .ok_or_else(|| {
                                EvaluatorError::InvalidPlan(format!(
                                    "missing bucket index: {}",
                                    querier.bucket
                                ))
                            })?
                            .unique_name
                            .clone();

                        dynamic_queries.push(DynamicBucketQuery {
                            descriptor: stream.stream.name.clone(),
                            priority: stream.stream.priority,
                            bucket_prefix,
                            inclusion_reason: inclusion_reason.clone(),
                            lookup_stages: querier.lookup_stages.clone(),
                            lookup_requests,
                            source_instantiation: querier.source_instantiation.clone(),
                        });
                    }
                }
            }
        }

        Ok(PreparedBucketQueries {
            static_buckets,
            dynamic_queries,
        })
    }

    pub fn resolve_bucket_queries(
        &self,
        prepared: &PreparedBucketQueries,
        results: &[LookupResults],
    ) -> EvaluatorResult<Vec<ResolvedBucket>> {
        let mut all_buckets = prepared.static_buckets.clone();

        let mut lookup_rows_by_key: HashMap<String, Vec<JsonMap>> = HashMap::new();
        for result in results {
            lookup_rows_by_key.insert(
                result.lookup.serialized_representation.clone(),
                result.rows.clone(),
            );
        }

        for query in &prepared.dynamic_queries {
            let mut staged_rows: HashMap<(usize, usize), Vec<Vec<Value>>> = HashMap::new();
            let request_values: HashMap<(usize, usize), Vec<ScopedParameterLookup>> = query
                .lookup_requests
                .iter()
                .map(|request| ((request.stage_id, request.id_in_stage), request.values.clone()))
                .collect();

            for (stage_id, stage) in query.lookup_stages.iter().enumerate() {
                for (id_in_stage, lookup) in stage.iter().enumerate() {
                    let scoped_values = if let Some(values) = request_values.get(&(stage_id, id_in_stage))
                    {
                        values.clone()
                    } else {
                        match lookup {
                            SerializedExpandingLookup::Parameter {
                                lookup,
                                instantiation,
                            } => {
                                let parameter_index =
                                    self.plan.parameter_indexes.get(*lookup).ok_or_else(|| {
                                        EvaluatorError::InvalidPlan(format!(
                                            "missing parameter lookup index: {lookup}"
                                        ))
                                    })?;
                                let scope = &parameter_index.lookup_scope;
                                let inputs =
                                    self.resolve_parameter_values(instantiation, &staged_rows)?;
                                cartesian_product(inputs)
                                    .into_iter()
                                    .map(|values| {
                                        let scoped_values = scoped_lookup_values(scope, &values);
                                        ScopedParameterLookup {
                                            serialized_representation: serialize_value_array(
                                                &scoped_values,
                                            ),
                                            values: scoped_values,
                                        }
                                    })
                                    .collect()
                            }
                            SerializedExpandingLookup::TableValued { .. } => Vec::new(),
                        }
                    };

                    let mut rows = Vec::new();
                    for lookup in &scoped_values {
                        if let Some(found) = lookup_rows_by_key.get(&lookup.serialized_representation)
                        {
                            for row in found {
                                rows.push(indexed_row_to_vec(row));
                            }
                        }
                    }
                    staged_rows.insert((stage_id, id_in_stage), rows);
                }
            }

            let instantiated =
                self.resolve_parameter_values(&query.source_instantiation, &staged_rows)?;
            for instance in cartesian_product(instantiated) {
                all_buckets.push(ResolvedBucket {
                    bucket: format!(
                        "{}{}",
                        query.bucket_prefix,
                        serialize_value_array(&instance)
                    ),
                    priority: query.priority,
                    definition: query.descriptor.clone(),
                    inclusion_reasons: vec![query.inclusion_reason.clone()],
                });
            }
        }

        Ok(all_buckets)
    }

    fn bucket_from_querier(
        &self,
        bucket_index: usize,
        values: &[Value],
    ) -> EvaluatorResult<String> {
        let bucket = self.plan.buckets.get(bucket_index).ok_or_else(|| {
            EvaluatorError::InvalidPlan(format!("missing bucket index for querier: {bucket_index}"))
        })?;

        Ok(format!(
            "{}{}",
            bucket.unique_name,
            serialize_value_array(values)
        ))
    }

    fn instantiate_without_lookups(
        &self,
        values: &[SerializedParameterValue],
        context: &EvalContext<'_>,
    ) -> EvaluatorResult<Vec<Vec<Value>>> {
        let mut out = Vec::with_capacity(values.len());
        for value in values {
            let result = match value {
                SerializedParameterValue::Request { expr } => {
                    let value = evaluate_expression(expr, context)?;
                    normalize_parameter_value(&value)
                        .map(|v| vec![v])
                        .unwrap_or_default()
                }
                SerializedParameterValue::Intersection { values } => {
                    let items = self.instantiate_without_lookups(values, context)?;
                    intersection(items)
                }
                SerializedParameterValue::Lookup { .. } => {
                    return Err(EvaluatorError::UnsupportedExpression(
                        "lookup references require resolve_bucket_queries()".to_string(),
                    ))
                }
            };

            out.push(result);
        }

        Ok(out)
    }

    fn prepare_lookup_requests(
        &self,
        querier: &crate::model::SerializedStreamQuerier,
        context: &EvalContext<'_>,
    ) -> EvaluatorResult<Vec<LookupRequest>> {
        let mut requests = Vec::new();

        for (stage_id, stage) in querier.lookup_stages.iter().enumerate() {
            for (id_in_stage, lookup) in stage.iter().enumerate() {
                if let SerializedExpandingLookup::Parameter {
                    lookup,
                    instantiation,
                } = lookup
                {
                    let parameter_index =
                        self.plan.parameter_indexes.get(*lookup).ok_or_else(|| {
                            EvaluatorError::InvalidPlan(format!(
                                "missing parameter lookup index: {lookup}"
                            ))
                        })?;
                    let scope = &parameter_index.lookup_scope;

                    let input_values = match self.instantiate_without_lookups(instantiation, context)
                    {
                        Ok(values) => values,
                        Err(EvaluatorError::UnsupportedExpression(_)) => {
                            continue;
                        }
                        Err(error) => return Err(error),
                    };
                    let mut lookup_values = Vec::new();
                    for values in cartesian_product(input_values) {
                        let scoped_values = scoped_lookup_values(scope, &values);
                        lookup_values.push(ScopedParameterLookup {
                            serialized_representation: serialize_value_array(&scoped_values),
                            values: scoped_values,
                        });
                    }

                    requests.push(LookupRequest {
                        stage_id,
                        id_in_stage,
                        values: lookup_values,
                    });
                }
            }
        }

        Ok(requests)
    }

    fn resolve_parameter_values(
        &self,
        source_instantiation: &[SerializedParameterValue],
        staged_rows: &HashMap<(usize, usize), Vec<Vec<Value>>>,
    ) -> EvaluatorResult<Vec<Vec<Value>>> {
        let mut values = Vec::new();

        for value in source_instantiation {
            let candidates = match value {
                SerializedParameterValue::Request { .. } => {
                    return Err(EvaluatorError::UnsupportedExpression(
                        "request references are expected to be resolved in prepare_bucket_queries()".to_string(),
                    ))
                }
                SerializedParameterValue::Lookup {
                    lookup,
                    result_index,
                } => {
                    let rows = staged_rows
                        .get(&(lookup.stage_id, lookup.id_in_stage))
                        .cloned()
                        .unwrap_or_default();

                    let mut out = Vec::new();
                    for row in rows {
                        if let Some(value) = row.get(*result_index) {
                            out.push(value.clone());
                        }
                    }
                    out
                }
                SerializedParameterValue::Intersection { values } => {
                    let grouped = self.resolve_parameter_values(values, staged_rows)?;
                    intersection(grouped)
                }
            };

            values.push(candidates);
        }

        Ok(values)
    }

    pub fn plan(&self) -> &SerializedSyncPlan {
        &self.plan
    }

    pub fn default_schema(&self) -> &str {
        &self.options.default_schema
    }
}

pub(crate) fn all_filters_match(
    filters: &[crate::model::SqlExpression],
    context: &EvalContext<'_>,
) -> EvaluatorResult<bool> {
    for filter in filters {
        let value = evaluate_expression(filter, context)?;
        if !sqlite_bool(&value) {
            return Ok(false);
        }
    }

    Ok(true)
}

pub(crate) fn table_matches(
    pattern: &crate::model::SerializedTablePattern,
    table: &SourceTable,
) -> bool {
    if let Some(connection) = &pattern.connection {
        if connection != &table.connection_tag {
            return false;
        }
    }

    if let Some(schema) = &pattern.schema {
        if schema != &table.schema {
            return false;
        }
    }

    if pattern.table.ends_with('%') {
        table.name.starts_with(pattern.table.trim_end_matches('%'))
    } else {
        table.name == pattern.table
    }
}

fn scoped_lookup_values(scope: &ParameterLookupScope, values: &[Value]) -> Vec<Value> {
    let mut result = Vec::with_capacity(values.len() + 2);
    result.push(Value::String(scope.lookup_name.clone()));
    result.push(Value::String(scope.query_id.clone()));
    result.extend(values.to_vec());
    result
}

fn indexed_row_to_vec(row: &JsonMap) -> Vec<Value> {
    let mut parsed: BTreeMap<usize, Value> = BTreeMap::new();
    for (key, value) in row {
        if let Ok(index) = key.parse::<usize>() {
            parsed.insert(index, value.clone());
        }
    }

    parsed.into_values().collect()
}

fn intersection(groups: Vec<Vec<Value>>) -> Vec<Value> {
    if groups.is_empty() {
        return Vec::new();
    }

    let mut current: BTreeSet<String> = groups[0]
        .iter()
        .map(serialize_value_single)
        .collect::<BTreeSet<_>>();

    for values in groups.iter().skip(1) {
        let next = values
            .iter()
            .map(serialize_value_single)
            .collect::<BTreeSet<_>>();
        current = current.intersection(&next).cloned().collect();
        if current.is_empty() {
            break;
        }
    }

    current
        .into_iter()
        .filter_map(|value| serde_json::from_str::<Value>(&value).ok())
        .collect()
}

fn serialize_value_single(value: &Value) -> String {
    serialize_value_array(std::slice::from_ref(value))
        .trim_start_matches('[')
        .trim_end_matches(']')
        .to_string()
}

fn cartesian_product(values: Vec<Vec<Value>>) -> Vec<Vec<Value>> {
    if values.is_empty() {
        return vec![Vec::new()];
    }

    let mut result: Vec<Vec<Value>> = vec![Vec::new()];
    for group in values {
        if group.is_empty() {
            return Vec::new();
        }

        let mut next = Vec::new();
        for prefix in &result {
            for value in &group {
                let mut item = prefix.clone();
                item.push(value.clone());
                next.push(item);
            }
        }
        result = next;
    }

    result
}

pub(crate) struct EvalContext<'a> {
    pub row: Option<&'a JsonMap>,
    pub request: &'a RequestParameters,
}

impl<'a> EvalContext<'a> {
    pub fn request_source(&self, request: &str) -> Value {
        match request {
            "auth" => self.request.auth.clone(),
            "connection" => self.request.connection.clone(),
            "subscription" => self.request.subscription.clone(),
            _ => Value::Null,
        }
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_pattern_matches_wildcards() {
        let pattern = crate::model::SerializedTablePattern {
            connection: None,
            schema: None,
            table: "users%".to_string(),
        };

        let table = SourceTable {
            connection_tag: "default".to_string(),
            schema: "public".to_string(),
            name: "users_v2".to_string(),
        };

        assert!(table_matches(&pattern, &table));
    }
}
