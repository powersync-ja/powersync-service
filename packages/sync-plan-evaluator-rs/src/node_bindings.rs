use napi::bindgen_prelude::{BigInt, Buffer, FromNapiValue, Object, Uint8Array};
use napi::{JsNumber, JsUnknown, ValueType};
use napi_derive::napi;
use serde::de::{DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Serialize};
use serde_json::{Number, Value};
use std::collections::BTreeSet;
use std::fmt;
use std::sync::{Arc, Mutex};

use crate::model::{
    EvaluateRowOptions, EvaluatedParameters, LookupResults, PrepareBucketQueryOptions,
    PreparedBucketQueries, SourceTable,
};
use crate::{EvaluatorOptions, RowParseRequirements, SyncPlanEvaluator};

const BYTES_MARKER_KEY: &str = "__bytes";
const JS_OBJECT_PARSE_DEPTH_LIMIT: usize = 64;

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
    prepared_source_tables: Mutex<Vec<Option<Arc<PreparedSourceTable>>>>,
}

#[derive(Debug, Clone)]
struct PreparedSourceTable {
    source_table: SourceTable,
    parse_requirements: RowParseRequirements,
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

        Ok(Self {
            inner: evaluator,
            prepared_source_tables: Mutex::new(Vec::new()),
        })
    }

    #[napi(js_name = "evaluateRowJson")]
    pub fn evaluate_row_json(&self, options_json: String) -> napi::Result<String> {
        let options: EvaluateRowOptions = parse_json("evaluateRow options", &options_json)?;
        let result = self
            .inner
            .evaluate_row_ref(&options)
            .map_err(|err| napi::Error::from_reason(err.to_string()))?;
        to_json(&result)
    }

    #[napi(js_name = "evaluateRowObjectJson")]
    pub fn evaluate_row_object_json(
        &self,
        source_table: Object,
        record: Object,
    ) -> napi::Result<String> {
        let source_table = parse_source_table_object(&source_table)?;
        let parse_requirements = self.inner.row_parse_requirements(&source_table);
        let record = parse_record_object_with_requirements(&record, &parse_requirements)?;
        let result = self
            .inner
            .evaluate_row_parts(&source_table, &record)
            .map_err(|err| napi::Error::from_reason(err.to_string()))?;
        to_json(&result)
    }

    #[napi(js_name = "prepareEvaluateRowSourceTableJson")]
    pub fn prepare_evaluate_row_source_table_json(&self, source_table_json: String) -> napi::Result<u32> {
        let source_table: SourceTable =
            parse_json("prepareEvaluateRow sourceTable", &source_table_json)?;
        self.store_prepared_source_table(source_table)
    }

    #[napi(js_name = "prepareEvaluateRowSourceTableObject")]
    pub fn prepare_evaluate_row_source_table_object(&self, source_table: Object) -> napi::Result<u32> {
        let source_table = parse_source_table_object(&source_table)?;
        self.store_prepared_source_table(source_table)
    }

    #[napi(js_name = "evaluateRowWithPreparedSourceTableJson")]
    pub fn evaluate_row_with_prepared_source_table_json(
        &self,
        prepared_source_table_id: u32,
        record_json: String,
    ) -> napi::Result<String> {
        let source_table =
            get_prepared_source_table(&self.prepared_source_tables, prepared_source_table_id)?;

        let record: crate::model::JsonMap =
            parse_record_with_requirements(&record_json, &source_table.parse_requirements)
                .map_err(|err| {
                    napi::Error::from_reason(format!("invalid JSON for evaluateRow record: {}", err))
                })?;
        let result = self
            .inner
            .evaluate_row_parts(&source_table.source_table, &record)
            .map_err(|err| napi::Error::from_reason(err.to_string()))?;
        to_json(&result)
    }

    #[napi(js_name = "evaluateRowWithPreparedSourceTableObjectJson")]
    pub fn evaluate_row_with_prepared_source_table_object_json(
        &self,
        prepared_source_table_id: u32,
        record: Object,
    ) -> napi::Result<String> {
        let source_table =
            get_prepared_source_table(&self.prepared_source_tables, prepared_source_table_id)?;
        let record =
            parse_record_object_with_requirements(&record, &source_table.parse_requirements)?;
        let result = self
            .inner
            .evaluate_row_parts(&source_table.source_table, &record)
            .map_err(|err| napi::Error::from_reason(err.to_string()))?;
        to_json(&result)
    }

    #[napi(js_name = "benchmarkParseRecordMinimalJson")]
    pub fn benchmark_parse_record_minimal_json(
        &self,
        prepared_source_table_id: u32,
        record_json: String,
    ) -> napi::Result<u32> {
        let source_table =
            get_prepared_source_table(&self.prepared_source_tables, prepared_source_table_id)?;
        let record: crate::model::JsonMap =
            parse_record_with_requirements(&record_json, &source_table.parse_requirements)
                .map_err(|err| {
                    napi::Error::from_reason(format!(
                        "invalid JSON for benchmark parse record: {}",
                        err
                    ))
                })?;
        u32::try_from(record.len())
            .map_err(|_| napi::Error::from_reason("record has too many columns".to_string()))
    }

    #[napi(js_name = "benchmarkParseAndSerializeRecordMinimalJson")]
    pub fn benchmark_parse_and_serialize_record_minimal_json(
        &self,
        prepared_source_table_id: u32,
        record_json: String,
    ) -> napi::Result<String> {
        let source_table =
            get_prepared_source_table(&self.prepared_source_tables, prepared_source_table_id)?;
        let record: crate::model::JsonMap =
            parse_record_with_requirements(&record_json, &source_table.parse_requirements)
                .map_err(|err| {
                    napi::Error::from_reason(format!(
                        "invalid JSON for benchmark parse+serialize record: {}",
                        err
                    ))
                })?;
        to_json(&record)
    }

    #[napi(js_name = "releasePreparedSourceTableJson")]
    pub fn release_prepared_source_table_json(&self, prepared_source_table_id: u32) -> napi::Result<bool> {
        let mut slots = self.prepared_source_tables.lock().map_err(|_| {
            napi::Error::from_reason("failed to lock prepared sourceTable cache".to_string())
        })?;
        let index = prepared_source_table_id as usize;
        if let Some(slot) = slots.get_mut(index) {
            let had_value = slot.is_some();
            *slot = None;
            Ok(had_value)
        } else {
            Ok(false)
        }
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

    #[napi(js_name = "evaluateParameterRowObjectJson")]
    pub fn evaluate_parameter_row_object_json(
        &self,
        source_table: Object,
        record: Object,
    ) -> napi::Result<String> {
        let source_table = parse_source_table_object(&source_table)?;
        let record = parse_record_object_full(&record)?;
        let result: Vec<EvaluatedParameters> = self
            .inner
            .evaluate_parameter_row(&source_table, &record)
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

    fn store_prepared_source_table(&self, source_table: SourceTable) -> napi::Result<u32> {
        let parse_requirements = self.inner.row_parse_requirements(&source_table);
        let prepared = Arc::new(PreparedSourceTable {
            source_table,
            parse_requirements,
        });
        let mut slots = self.prepared_source_tables.lock().map_err(|_| {
            napi::Error::from_reason("failed to lock prepared sourceTable cache".to_string())
        })?;

        let mut available_slot = None;
        for (index, slot) in slots.iter_mut().enumerate() {
            if slot.is_none() {
                available_slot = Some(index);
                *slot = Some(prepared.clone());
                break;
            }
        }

        let slot_index = if let Some(index) = available_slot {
            index
        } else {
            slots.push(Some(prepared));
            slots.len() - 1
        };

        u32::try_from(slot_index)
            .map_err(|_| napi::Error::from_reason("too many prepared source tables".to_string()))
    }
}

fn parse_source_table_object(source_table: &Object) -> napi::Result<SourceTable> {
    Ok(SourceTable {
        connection_tag: parse_required_string_field(
            source_table,
            "connectionTag",
            "sourceTable.connectionTag",
        )?,
        schema: parse_required_string_field(source_table, "schema", "sourceTable.schema")?,
        name: parse_required_string_field(source_table, "name", "sourceTable.name")?,
    })
}

fn parse_required_string_field(
    object: &Object,
    field_name: &str,
    label: &str,
) -> napi::Result<String> {
    let value = object
        .get::<_, JsUnknown>(field_name)?
        .ok_or_else(|| napi::Error::from_reason(format!("missing required field: {label}")))?;

    match value.get_type()? {
        ValueType::String => <String as FromNapiValue>::from_unknown(value)
            .map_err(|err| napi::Error::from_reason(format!("invalid string for {label}: {err}"))),
        other => Err(napi::Error::from_reason(format!(
            "expected string for {label}, got {other:?}"
        ))),
    }
}

fn parse_record_object_full(record: &Object) -> napi::Result<crate::model::JsonMap> {
    let mut out = crate::model::JsonMap::new();
    for key in Object::keys(record)? {
        let Some(value) = record.get::<_, JsUnknown>(&key)? else {
            continue;
        };
        if let Some(parsed) = js_unknown_to_json_value(value, false, 0)? {
            out.insert(key, parsed);
        }
    }
    Ok(out)
}

fn parse_record_object_with_requirements(
    record: &Object,
    requirements: &RowParseRequirements,
) -> napi::Result<crate::model::JsonMap> {
    let mut out = crate::model::JsonMap::new();

    for key in Object::keys(record)? {
        let Some(value) = record.get::<_, JsUnknown>(&key)? else {
            continue;
        };

        if requirements.full_columns.contains(&key) {
            if let Some(parsed) = js_unknown_to_json_value(value, false, 0)? {
                out.insert(key, parsed);
            }
            continue;
        }

        if requirements.include_star_scalars {
            if let Some(parsed) = js_unknown_to_json_value(value, true, 0)? {
                out.insert(key, parsed);
            }
        }
    }

    Ok(out)
}

fn js_unknown_to_json_value(
    value: JsUnknown,
    scalars_only: bool,
    depth: usize,
) -> napi::Result<Option<Value>> {
    if depth > JS_OBJECT_PARSE_DEPTH_LIMIT {
        return Err(napi::Error::from_reason(format!(
            "object nesting exceeds the supported depth of {JS_OBJECT_PARSE_DEPTH_LIMIT}"
        )));
    }

    match value.get_type()? {
        ValueType::Undefined => Ok(None),
        ValueType::Null => Ok(Some(Value::Null)),
        ValueType::Boolean => {
            let value = <bool as FromNapiValue>::from_unknown(value)?;
            Ok(Some(Value::Bool(value)))
        }
        ValueType::String => {
            let value = <String as FromNapiValue>::from_unknown(value)?;
            Ok(Some(Value::String(value)))
        }
        ValueType::Number => {
            let number: JsNumber = unsafe { value.cast() };
            let number = number.get_double()?;
            if !number.is_finite() {
                return Ok(Some(Value::Null));
            }

            let Some(number) = Number::from_f64(number) else {
                return Ok(Some(Value::Null));
            };
            Ok(Some(Value::Number(number)))
        }
        ValueType::BigInt => {
            let bigint: BigInt = <BigInt as FromNapiValue>::from_unknown(value)?;
            let (value, lossless) = bigint.get_i64();
            if !lossless {
                return Err(napi::Error::from_reason(
                    "BigInt value is outside the supported int64 range".to_string(),
                ));
            }
            Ok(Some(Value::Number(Number::from(value))))
        }
        ValueType::Object => {
            let object: Object = unsafe { value.cast() };
            if object.is_buffer()? {
                if scalars_only {
                    return Ok(None);
                }

                let bytes: Buffer = <Buffer as FromNapiValue>::from_unknown(value)?;
                return Ok(Some(bytes_marker_value(bytes.as_ref())));
            }

            if object.is_typedarray()? {
                if scalars_only {
                    return Ok(None);
                }

                let bytes: Uint8Array = <Uint8Array as FromNapiValue>::from_unknown(value)
                    .map_err(|_| {
                        napi::Error::from_reason(
                            "Only Uint8Array is supported for typed array row values".to_string(),
                        )
                    })?;
                return Ok(Some(bytes_marker_value(bytes.as_ref())));
            }

            if scalars_only {
                return Ok(None);
            }

            if object.is_array()? {
                let length = object.get_array_length_unchecked()?;
                let mut out = Vec::with_capacity(length as usize);
                for index in 0..length {
                    let element: JsUnknown = object.get_element(index)?;
                    if let Some(parsed) = js_unknown_to_json_value(element, false, depth + 1)? {
                        out.push(parsed);
                    } else {
                        out.push(Value::Null);
                    }
                }
                return Ok(Some(Value::Array(out)));
            }

            let mut out = crate::model::JsonMap::new();
            for key in Object::keys(&object)? {
                let Some(entry) = object.get::<_, JsUnknown>(&key)? else {
                    continue;
                };

                if let Some(parsed) = js_unknown_to_json_value(entry, false, depth + 1)? {
                    out.insert(key, parsed);
                }
            }
            Ok(Some(Value::Object(out)))
        }
        ValueType::Function | ValueType::Symbol | ValueType::External => Ok(None),
        _ => Ok(None),
    }
}

fn bytes_marker_value(bytes: &[u8]) -> Value {
    let values = bytes
        .iter()
        .map(|value| Value::Number(Number::from(*value)))
        .collect::<Vec<_>>();
    let mut out = crate::model::JsonMap::new();
    out.insert(BYTES_MARKER_KEY.to_string(), Value::Array(values));
    Value::Object(out)
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

fn get_prepared_source_table(
    cache: &Mutex<Vec<Option<Arc<PreparedSourceTable>>>>,
    prepared_source_table_id: u32,
) -> napi::Result<Arc<PreparedSourceTable>> {
    let slots = cache.lock().map_err(|_| {
        napi::Error::from_reason("failed to lock prepared sourceTable cache".to_string())
    })?;
    let index = prepared_source_table_id as usize;
    slots
        .get(index)
        .and_then(|slot| slot.as_ref())
        .cloned()
        .ok_or_else(|| {
            napi::Error::from_reason(format!(
                "unknown prepared source table id: {prepared_source_table_id}"
            ))
        })
}

fn parse_record_with_requirements(
    value: &str,
    requirements: &RowParseRequirements,
) -> serde_json::Result<crate::model::JsonMap> {
    let mut deserializer = serde_json::Deserializer::from_str(value);
    let seed = FilteredMapSeed {
        include_star_scalars: requirements.include_star_scalars,
        full_columns: &requirements.full_columns,
    };
    seed.deserialize(&mut deserializer)
}

struct FilteredMapSeed<'a> {
    include_star_scalars: bool,
    full_columns: &'a BTreeSet<String>,
}

impl<'de> DeserializeSeed<'de> for FilteredMapSeed<'_> {
    type Value = crate::model::JsonMap;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(FilteredMapVisitor {
            include_star_scalars: self.include_star_scalars,
            full_columns: self.full_columns,
        })
    }
}

struct FilteredMapVisitor<'a> {
    include_star_scalars: bool,
    full_columns: &'a BTreeSet<String>,
}

impl<'de> Visitor<'de> for FilteredMapVisitor<'_> {
    type Value = crate::model::JsonMap;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut out = crate::model::JsonMap::new();

        while let Some(key) = map.next_key::<String>()? {
            if self.full_columns.contains(&key) {
                let value = map.next_value::<Value>()?;
                out.insert(key, value);
                continue;
            }

            if self.include_star_scalars {
                let scalar = map.next_value_seed(ScalarOrSkipSeed)?;
                if let Some(value) = scalar {
                    out.insert(key, value);
                }
            } else {
                map.next_value::<IgnoredAny>()?;
            }
        }

        Ok(out)
    }
}

struct ScalarOrSkipSeed;

impl<'de> DeserializeSeed<'de> for ScalarOrSkipSeed {
    type Value = Option<Value>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(ScalarOrSkipVisitor)
    }
}

struct ScalarOrSkipVisitor;

impl<'de> Visitor<'de> for ScalarOrSkipVisitor {
    type Value = Option<Value>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("any JSON value")
    }

    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Some(Value::Bool(value)))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Some(Value::Number(Number::from(value))))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Some(Value::Number(Number::from(value))))
    }

    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        match Number::from_f64(value) {
            Some(number) => Ok(Some(Value::Number(number))),
            None => Err(E::custom("invalid floating point number")),
        }
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Some(Value::String(value.to_owned())))
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Some(Value::String(value)))
    }

    fn visit_none<E>(self) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Some(Value::Null))
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Some(Value::Null))
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        while seq.next_element::<IgnoredAny>()?.is_some() {}
        Ok(None)
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        while map.next_entry::<IgnoredAny, IgnoredAny>()?.is_some() {}
        Ok(None)
    }
}
