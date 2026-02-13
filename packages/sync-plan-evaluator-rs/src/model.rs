use serde::{Deserialize, Serialize};
use serde_json::Value;

pub type JsonMap = serde_json::Map<String, Value>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SerializedSyncPlan {
    pub version: String,
    #[serde(rename = "dataSources")]
    pub data_sources: Vec<SerializedDataSource>,
    pub buckets: Vec<SerializedBucketDataSource>,
    #[serde(rename = "parameterIndexes")]
    pub parameter_indexes: Vec<SerializedParameterIndexLookupCreator>,
    pub streams: Vec<SerializedStream>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SerializedBucketDataSource {
    pub hash: u32,
    #[serde(rename = "uniqueName")]
    pub unique_name: String,
    pub sources: Vec<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SerializedTablePattern {
    pub connection: Option<String>,
    pub schema: Option<String>,
    pub table: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SerializedDataSource {
    pub table: SerializedTablePattern,
    #[serde(rename = "outputTableName")]
    pub output_table_name: Option<String>,
    pub hash: u32,
    pub columns: Vec<ColumnSource>,
    pub filters: Vec<SqlExpression>,
    #[serde(rename = "partitionBy")]
    pub partition_by: Vec<PartitionKey>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum ColumnSource {
    Star(String),
    Expression { expr: SqlExpression, alias: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SerializedParameterIndexLookupCreator {
    pub table: SerializedTablePattern,
    pub hash: u32,
    #[serde(rename = "lookupScope")]
    pub lookup_scope: ParameterLookupScope,
    pub output: Vec<SqlExpression>,
    pub filters: Vec<SqlExpression>,
    #[serde(rename = "partitionBy")]
    pub partition_by: Vec<PartitionKey>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SerializedStream {
    pub stream: StreamOptions,
    pub queriers: Vec<SerializedStreamQuerier>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StreamOptions {
    pub name: String,
    #[serde(rename = "isSubscribedByDefault")]
    pub is_subscribed_by_default: bool,
    pub priority: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SerializedStreamQuerier {
    #[serde(rename = "requestFilters")]
    pub request_filters: Vec<SqlExpression>,
    #[serde(rename = "lookupStages")]
    pub lookup_stages: Vec<Vec<SerializedExpandingLookup>>,
    pub bucket: usize,
    #[serde(rename = "sourceInstantiation")]
    pub source_instantiation: Vec<SerializedParameterValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum SerializedExpandingLookup {
    #[serde(rename = "parameter")]
    Parameter {
        lookup: usize,
        instantiation: Vec<SerializedParameterValue>,
    },
    #[serde(rename = "table_valued")]
    TableValued {
        #[serde(rename = "functionName")]
        function_name: String,
        #[serde(rename = "functionInputs")]
        function_inputs: Vec<SqlExpression>,
        outputs: Vec<SqlExpression>,
        filters: Vec<SqlExpression>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum SerializedParameterValue {
    #[serde(rename = "request")]
    Request { expr: SqlExpression },
    #[serde(rename = "lookup")]
    Lookup {
        lookup: LookupReference,
        #[serde(rename = "resultIndex")]
        result_index: usize,
    },
    #[serde(rename = "intersection")]
    Intersection {
        values: Vec<SerializedParameterValue>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LookupReference {
    #[serde(rename = "stageId")]
    pub stage_id: usize,
    #[serde(rename = "idInStage")]
    pub id_in_stage: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PartitionKey {
    pub expr: SqlExpression,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ParameterLookupScope {
    #[serde(rename = "lookupName")]
    pub lookup_name: String,
    #[serde(rename = "queryId")]
    pub query_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum SqlExpression {
    #[serde(rename = "data")]
    Data { source: ExternalSource },
    #[serde(rename = "unary")]
    Unary {
        operand: Box<SqlExpression>,
        operator: String,
    },
    #[serde(rename = "binary")]
    Binary {
        left: Box<SqlExpression>,
        operator: String,
        right: Box<SqlExpression>,
    },
    #[serde(rename = "between")]
    Between {
        value: Box<SqlExpression>,
        low: Box<SqlExpression>,
        high: Box<SqlExpression>,
    },
    #[serde(rename = "scalar_in")]
    ScalarIn {
        target: Box<SqlExpression>,
        #[serde(rename = "in")]
        in_values: Vec<SqlExpression>,
    },
    #[serde(rename = "case_when")]
    CaseWhen {
        operand: Option<Box<SqlExpression>>,
        whens: Vec<CaseWhenBranch>,
        #[serde(rename = "else")]
        else_expr: Option<Box<SqlExpression>>,
    },
    #[serde(rename = "cast")]
    Cast {
        operand: Box<SqlExpression>,
        #[serde(rename = "cast_as")]
        cast_as: String,
    },
    #[serde(rename = "function")]
    Function {
        function: String,
        parameters: Vec<SqlExpression>,
    },
    #[serde(rename = "lit_null")]
    LitNull,
    #[serde(rename = "lit_double")]
    LitDouble { value: f64 },
    #[serde(rename = "lit_int")]
    LitInt { base10: String },
    #[serde(rename = "lit_string")]
    LitString { value: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CaseWhenBranch {
    pub when: SqlExpression,
    pub then: SqlExpression,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum ExternalSource {
    Column { column: String },
    Request { request: String },
    Other(Value),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SourceTable {
    #[serde(rename = "connectionTag")]
    pub connection_tag: String,
    pub schema: String,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EvaluateRowOptions {
    #[serde(rename = "sourceTable")]
    pub source_table: SourceTable,
    pub record: JsonMap,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EvaluatedRow {
    pub bucket: String,
    pub table: String,
    pub id: String,
    pub data: JsonMap,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ScopedParameterLookup {
    pub values: Vec<Value>,
    #[serde(rename = "serializedRepresentation")]
    pub serialized_representation: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EvaluatedParameters {
    pub lookup: ScopedParameterLookup,
    #[serde(rename = "bucketParameters")]
    pub bucket_parameters: Vec<JsonMap>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BucketDescription {
    pub bucket: String,
    pub priority: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResolvedBucket {
    pub bucket: String,
    pub priority: u8,
    pub definition: String,
    #[serde(rename = "inclusion_reasons")]
    pub inclusion_reasons: Vec<BucketInclusionReason>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum BucketInclusionReason {
    Default(String),
    Subscription { subscription: i64 },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RequestedStream {
    pub parameters: Option<Value>,
    #[serde(rename = "opaque_id")]
    pub opaque_id: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RequestParameters {
    pub auth: Value,
    pub connection: Value,
    pub subscription: Value,
}

impl Default for RequestParameters {
    fn default() -> Self {
        Self {
            auth: Value::Object(JsonMap::new()),
            connection: Value::Object(JsonMap::new()),
            subscription: Value::Object(JsonMap::new()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PrepareBucketQueryOptions {
    #[serde(rename = "globalParameters")]
    pub global_parameters: RequestParameters,
    #[serde(rename = "hasDefaultStreams")]
    pub has_default_streams: bool,
    pub streams: std::collections::BTreeMap<String, Vec<RequestedStream>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PreparedBucketQueries {
    #[serde(rename = "staticBuckets")]
    pub static_buckets: Vec<ResolvedBucket>,
    #[serde(rename = "dynamicQueries")]
    pub dynamic_queries: Vec<DynamicBucketQuery>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DynamicBucketQuery {
    pub descriptor: String,
    pub priority: u8,
    pub bucket_prefix: String,
    #[serde(rename = "inclusionReason")]
    pub inclusion_reason: BucketInclusionReason,
    #[serde(rename = "lookupStages")]
    pub lookup_stages: Vec<Vec<SerializedExpandingLookup>>,
    #[serde(rename = "lookupRequests")]
    pub lookup_requests: Vec<LookupRequest>,
    #[serde(rename = "sourceInstantiation")]
    pub source_instantiation: Vec<SerializedParameterValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LookupRequest {
    #[serde(rename = "stageId")]
    pub stage_id: usize,
    #[serde(rename = "idInStage")]
    pub id_in_stage: usize,
    pub values: Vec<ScopedParameterLookup>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LookupResults {
    pub lookup: ScopedParameterLookup,
    pub rows: Vec<JsonMap>,
}
