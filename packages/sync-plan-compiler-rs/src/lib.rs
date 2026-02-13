use pg_query::protobuf;
use pg_query::protobuf::node::Node as PgNodeEnum;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use sync_plan_evaluator_rs::model::{
    CaseWhenBranch, ColumnSource, ExternalSource, LookupReference, PartitionKey,
    ParameterLookupScope, SerializedBucketDataSource, SerializedDataSource, SerializedExpandingLookup,
    SerializedParameterIndexLookupCreator, SerializedParameterValue, SerializedStream,
    SerializedStreamQuerier, SerializedSyncPlan, SerializedTablePattern, SqlExpression, StreamOptions,
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

pub fn compile_sync_plan(
    yaml: &str,
    options: CompileOptions,
) -> Result<CompileResult, CompilerError> {
    let parsed = parse_sync_yaml(yaml)?;

    if !parsed.sync_config_compiler {
        return Err(CompilerError::Unsupported(
            "config.sync_config_compiler must be true for sync-plan compiler".to_string(),
        ));
    }

    if parsed.edition < 2 {
        return Err(CompilerError::Unsupported(
            "config.edition must be 2 or higher".to_string(),
        ));
    }

    if parsed.streams.is_empty() {
        return Err(CompilerError::Parse("streams are required".to_string()));
    }

    let mut data_sources = Vec::<SerializedDataSource>::new();
    let mut buckets = Vec::<SerializedBucketDataSource>::new();
    let mut parameter_indexes = Vec::<SerializedParameterIndexLookupCreator>::new();
    let mut parameter_index_signatures = HashMap::<String, usize>::new();
    let mut streams = Vec::<SerializedStream>::new();
    let mut messages: Vec<CompileMessage> = Vec::new();

    for stream in parsed.streams {
        let mut stream_queriers = Vec::new();
        let mut stream_bucket_indexes = HashMap::<String, usize>::new();
        let mut stream_querier_indexes = HashMap::<String, usize>::new();

        for (query_index, query_sql) in stream.queries.iter().enumerate() {
            let expanded_query_sql = expand_with_queries(query_sql, &stream.with_queries);
            let normalized_query_sql = normalize_sql_for_pg_query(&expanded_query_sql);

            let query = parse_select_query(&normalized_query_sql).map_err(|e| {
                CompilerError::Sql(format!(
                    "stream '{}' query {}: {e}",
                    stream.name, query_index
                ))
            })?;

            let output_table = query
                .tables
                .iter()
                .find(|table| table.alias == query.output_alias)
                .ok_or_else(|| {
                    CompilerError::Sql(format!(
                        "stream '{}' query {}: output table alias '{}' not found in FROM clause",
                        stream.name, query_index, query.output_alias
                    ))
                })?;

            let effective_query = output_table
                .derived_query
                .as_deref()
                .filter(|_| query.where_expr.is_none() && query.join_equalities.is_empty())
                .unwrap_or(&query);
            let effective_output_table = effective_query
                .tables
                .iter()
                .find(|table| table.alias == effective_query.output_alias)
                .ok_or_else(|| {
                    CompilerError::Sql(format!(
                        "stream '{}' query {}: effective output table alias '{}' not found",
                        stream.name, query_index, effective_query.output_alias
                    ))
                })?;

            let output_table_name =
                if effective_output_table.table.contains('%') && !effective_output_table.explicit_alias
                {
                None
            } else {
                Some(effective_output_table.alias.clone())
            };

            let compiled_bindings = compile_query_bindings(
                effective_query,
                &expanded_query_sql,
                &mut parameter_indexes,
                &mut parameter_index_signatures,
            )?;

            let selected_columns: &[SelectItemAst] = if query.where_expr.is_none()
                && query.join_equalities.is_empty()
                && query.columns.len() == 1
                && matches!(query.columns.first(), Some(SelectItemAst::Star { .. }))
                && output_table.derived_columns.is_some()
            {
                output_table.derived_columns.as_ref().unwrap().as_slice()
            } else {
                &query.columns
            };

            let mut columns = Vec::new();
            for column in selected_columns {
                match column {
                    SelectItemAst::Star { .. } => {
                        columns.push(ColumnSource::Star("star".to_string()))
                    }
                    SelectItemAst::Expr { expr, alias } => {
                        let compiled_expr = transform_expr(expr)?;
                        let output_alias = alias
                            .clone()
                            .or_else(|| column_alias_from_expr(expr))
                            .ok_or_else(|| {
                            CompilerError::Unsupported(
                                "expressions in SELECT list must define an alias".to_string(),
                            )
                        })?;

                        columns.push(ColumnSource::Expression {
                            expr: compiled_expr,
                            alias: output_alias,
                        });
                    }
                }
            }

            let table_pattern = parse_table_pattern(
                &effective_output_table.table,
                effective_output_table.schema.as_deref(),
                &options.default_schema,
            );
            let mut data_source = SerializedDataSource {
                table: table_pattern,
                output_table_name,
                hash: 0,
                columns,
                filters: compiled_bindings.data_filters.clone(),
                partition_by: compiled_bindings.partition_by.clone(),
            };
            data_source.hash = hash_json(&data_source)?;

            let data_source_index = data_sources.len();
            data_sources.push(data_source);

            let querier_signature =
                serde_json::to_string(&(
                    compiled_bindings.request_filters.clone(),
                    compiled_bindings.source_instantiation.clone(),
                ))?;
            let bucket_index = if let Some(index) = stream_bucket_indexes.get(&querier_signature) {
                let index = *index;
                let bucket = buckets.get_mut(index).ok_or_else(|| {
                    CompilerError::Parse("invalid internal bucket index".to_string())
                })?;

                if !bucket.sources.contains(&data_source_index) {
                    bucket.sources.push(data_source_index);
                    bucket.hash = hash_json(bucket)?;
                }
                index
            } else {
                let unique_name = format!("{}|{}", stream.name, query_index);
                let mut bucket = SerializedBucketDataSource {
                    hash: 0,
                    unique_name,
                    sources: vec![data_source_index],
                };
                bucket.hash = hash_json(&bucket)?;

                let index = buckets.len();
                buckets.push(bucket);
                stream_bucket_indexes.insert(querier_signature.clone(), index);
                index
            };

            if !stream_querier_indexes.contains_key(&querier_signature) {
                let querier_index = stream_queriers.len();
                stream_querier_indexes.insert(querier_signature, querier_index);
                stream_queriers.push(SerializedStreamQuerier {
                    request_filters: compiled_bindings.request_filters,
                    lookup_stages: compiled_bindings.lookup_stages,
                    bucket: bucket_index,
                    source_instantiation: compiled_bindings.source_instantiation,
                });
            }
        }

        streams.push(SerializedStream {
            stream: StreamOptions {
                name: stream.name,
                is_subscribed_by_default: stream.auto_subscribe,
                priority: stream.priority,
            },
            queriers: stream_queriers,
        });
    }

    canonicalize_sources_and_buckets(&mut data_sources, &mut buckets, &mut streams)?;

    let plan = SerializedSyncPlan {
        version: "unstable".to_string(),
        data_sources,
        buckets,
        parameter_indexes,
        streams,
    };

    let plan_json = serde_json::to_string(&plan)?;
    messages.retain(|m| matches!(m.severity, Severity::Warning));

    Ok(CompileResult {
        plan,
        plan_json,
        messages,
    })
}

fn hash_json<T: Serialize>(value: &T) -> Result<u32, CompilerError> {
    let json = serde_json::to_string(value)?;
    let mut hash: u32 = 2166136261;
    for byte in json.as_bytes() {
        hash ^= *byte as u32;
        hash = hash.wrapping_mul(16777619);
    }

    Ok(hash)
}

fn canonicalize_sources_and_buckets(
    data_sources: &mut Vec<SerializedDataSource>,
    buckets: &mut Vec<SerializedBucketDataSource>,
    streams: &mut Vec<SerializedStream>,
) -> Result<(), CompilerError> {
    let mut source_signature_map = HashMap::<String, usize>::new();
    let mut source_remap = vec![0; data_sources.len()];
    let mut canonical_sources = Vec::<SerializedDataSource>::new();

    for (old_index, source) in data_sources.iter().cloned().enumerate() {
        let signature = serde_json::to_string(&(
            source.table.clone(),
            source.output_table_name.clone(),
            source.columns.clone(),
            source.filters.clone(),
            source.partition_by.clone(),
        ))?;

        if let Some(index) = source_signature_map.get(&signature) {
            source_remap[old_index] = *index;
        } else {
            let mut source = source;
            source.hash = hash_json(&source)?;
            let index = canonical_sources.len();
            source_signature_map.insert(signature, index);
            source_remap[old_index] = index;
            canonical_sources.push(source);
        }
    }
    *data_sources = canonical_sources;

    let mut bucket_signature_map = HashMap::<String, usize>::new();
    let mut bucket_remap = vec![0; buckets.len()];
    let mut canonical_buckets = Vec::<SerializedBucketDataSource>::new();

    for (old_index, bucket) in buckets.iter().cloned().enumerate() {
        let mut sources = bucket
            .sources
            .into_iter()
            .map(|index| source_remap[index])
            .collect::<Vec<_>>();
        sources.sort_unstable();
        sources.dedup();

        let signature = serde_json::to_string(&sources)?;
        if let Some(index) = bucket_signature_map.get(&signature) {
            bucket_remap[old_index] = *index;
        } else {
            let index = canonical_buckets.len();
            let mut canonical_bucket = SerializedBucketDataSource {
                hash: 0,
                unique_name: bucket.unique_name,
                sources,
            };
            canonical_bucket.hash = hash_json(&canonical_bucket)?;
            bucket_signature_map.insert(signature, index);
            bucket_remap[old_index] = index;
            canonical_buckets.push(canonical_bucket);
        }
    }
    *buckets = canonical_buckets;

    for stream in streams {
        for querier in &mut stream.queriers {
            querier.bucket = bucket_remap[querier.bucket];
        }
    }

    Ok(())
}

#[derive(Debug, Clone)]
struct ParsedSyncYaml {
    edition: i64,
    sync_config_compiler: bool,
    streams: Vec<ParsedStream>,
}

#[derive(Debug, Clone)]
struct ParsedStream {
    name: String,
    auto_subscribe: bool,
    priority: u8,
    queries: Vec<String>,
    with_queries: HashMap<String, String>,
}

fn parse_sync_yaml(yaml: &str) -> Result<ParsedSyncYaml, CompilerError> {
    let mut edition = 1;
    let mut sync_config_compiler = false;
    let mut streams = Vec::<ParsedStream>::new();

    enum Section {
        None,
        Config,
        Streams,
    }

    let mut section = Section::None;
    let mut current_stream: Option<usize> = None;
    let mut in_queries = false;

    #[derive(Debug, Clone)]
    enum BlockTarget {
        Query { stream_index: usize },
        With { stream_index: usize, name: String },
    }

    let mut in_with = false;
    let mut block_target: Option<BlockTarget> = None;
    let mut block_indent: usize = 0;
    let mut block_content_indent: usize = 0;
    let mut block_text = String::new();

    for (line_index, raw_line) in yaml.lines().enumerate() {
        let indent = raw_line.chars().take_while(|c| *c == ' ').count();

        if let Some(target) = block_target.clone() {
            if indent > block_indent {
                let content = if raw_line.len() >= block_content_indent {
                    &raw_line[block_content_indent..]
                } else {
                    ""
                };
                block_text.push_str(content.trim_end());
                block_text.push('\n');
                continue;
            }

            let value = block_text.trim_end_matches('\n').to_string();
            match target {
                BlockTarget::Query { stream_index } => {
                    streams[stream_index].queries.push(value);
                }
                BlockTarget::With { stream_index, name } => {
                    streams[stream_index].with_queries.insert(name, value);
                }
            }
            block_target = None;
            block_text.clear();
        }

        let line = raw_line.trim_end();
        if line.trim().is_empty() {
            continue;
        }

        let stripped = strip_comment(line);
        let trimmed = stripped.trim();
        if trimmed.is_empty() {
            continue;
        }

        if indent == 0 {
            current_stream = None;
            in_queries = false;
            in_with = false;

            match trimmed {
                "config:" => {
                    section = Section::Config;
                    continue;
                }
                "streams:" => {
                    section = Section::Streams;
                    continue;
                }
                _ => {
                    section = Section::None;
                    continue;
                }
            }
        }

        match section {
            Section::Config => {
                if indent != 2 {
                    continue;
                }
                let (key, value) = parse_key_value(trimmed)
                    .map_err(|e| CompilerError::Parse(format!("line {}: {e}", line_index + 1)))?;

                match key {
                    "edition" => {
                        edition = value.parse::<i64>().map_err(|_| {
                            CompilerError::Parse(format!(
                                "line {}: config.edition must be an integer",
                                line_index + 1
                            ))
                        })?;
                    }
                    "sync_config_compiler" => {
                        sync_config_compiler = parse_bool(value).ok_or_else(|| {
                            CompilerError::Parse(format!(
                                "line {}: config.sync_config_compiler must be true/false",
                                line_index + 1
                            ))
                        })?;
                    }
                    _ => {}
                }
            }
            Section::Streams => {
                if indent == 2 && trimmed.ends_with(':') {
                    let name = trimmed.trim_end_matches(':').trim().to_string();
                    streams.push(ParsedStream {
                        name,
                        auto_subscribe: false,
                        priority: 3,
                        queries: Vec::new(),
                        with_queries: HashMap::new(),
                    });
                    current_stream = Some(streams.len() - 1);
                    in_queries = false;
                    in_with = false;
                    continue;
                }

                let Some(stream_index) = current_stream else {
                    continue;
                };

                if indent == 4 {
                    let (key, value) = parse_key_value(trimmed).map_err(|e| {
                        CompilerError::Parse(format!("line {}: {e}", line_index + 1))
                    })?;

                    match key {
                        "auto_subscribe" => {
                            streams[stream_index].auto_subscribe =
                                parse_bool(value).ok_or_else(|| {
                                    CompilerError::Parse(format!(
                                        "line {}: auto_subscribe must be true/false",
                                        line_index + 1
                                    ))
                                })?;
                            in_queries = false;
                            in_with = false;
                        }
                        "priority" => {
                            let parsed = value.parse::<u8>().map_err(|_| {
                                CompilerError::Parse(format!(
                                    "line {}: priority must be an integer",
                                    line_index + 1
                                ))
                            })?;
                            if parsed > 3 {
                                return Err(CompilerError::Parse(format!(
                                    "line {}: priority must be between 0 and 3",
                                    line_index + 1
                                )));
                            }
                            streams[stream_index].priority = parsed;
                            in_queries = false;
                            in_with = false;
                        }
                        "query" => {
                            if value == "|" {
                                block_target = Some(BlockTarget::Query { stream_index });
                                block_indent = indent;
                                block_content_indent = indent + 2;
                                block_text.clear();
                            } else {
                                streams[stream_index]
                                    .queries
                                    .push(unquote(value).to_string());
                            }
                            in_queries = false;
                            in_with = false;
                        }
                        "queries" => {
                            in_queries = true;
                            in_with = false;
                        }
                        "with" => {
                            in_with = true;
                            in_queries = false;
                        }
                        _ => {
                            in_queries = false;
                            in_with = false;
                        }
                    }
                } else if indent == 6 && in_queries {
                    let item = trimmed.strip_prefix("-").ok_or_else(|| {
                        CompilerError::Parse(format!(
                            "line {}: expected list item in queries",
                            line_index + 1
                        ))
                    })?;
                    streams[stream_index]
                        .queries
                        .push(unquote(item.trim()).to_string());
                } else if indent == 6 && in_with {
                    let (name, value) = parse_key_value(trimmed).map_err(|e| {
                        CompilerError::Parse(format!("line {}: {e}", line_index + 1))
                    })?;

                    if value == "|" {
                        block_target = Some(BlockTarget::With {
                            stream_index,
                            name: name.to_string(),
                        });
                        block_indent = indent;
                        block_content_indent = indent + 2;
                        block_text.clear();
                    } else {
                        streams[stream_index]
                            .with_queries
                            .insert(name.to_string(), unquote(value).to_string());
                    }
                }
            }
            Section::None => {}
        }
    }

    if let Some(target) = block_target {
        let value = block_text.trim_end_matches('\n').to_string();
        match target {
            BlockTarget::Query { stream_index } => {
                streams[stream_index].queries.push(value);
            }
            BlockTarget::With { stream_index, name } => {
                streams[stream_index].with_queries.insert(name, value);
            }
        }
    }

    for stream in &streams {
        if stream.queries.is_empty() {
            return Err(CompilerError::Parse(format!(
                "stream '{}' must define query or queries",
                stream.name
            )));
        }
    }

    Ok(ParsedSyncYaml {
        edition,
        sync_config_compiler,
        streams,
    })
}

fn strip_comment(line: &str) -> String {
    let mut in_single = false;
    let mut result = String::with_capacity(line.len());
    let mut chars = line.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == '\'' {
            in_single = !in_single;
            result.push(ch);
            continue;
        }

        if ch == '#' && !in_single {
            break;
        }

        result.push(ch);
    }

    result
}

fn parse_key_value(input: &str) -> Result<(&str, &str), &'static str> {
    let Some((key, value)) = input.split_once(':') else {
        return Err("expected key: value");
    };

    Ok((key.trim(), value.trim()))
}

fn parse_bool(input: &str) -> Option<bool> {
    match input {
        "true" => Some(true),
        "false" => Some(false),
        _ => None,
    }
}

fn unquote(input: &str) -> &str {
    if input.len() >= 2
        && ((input.starts_with('"') && input.ends_with('"'))
            || (input.starts_with('\'') && input.ends_with('\'')))
    {
        &input[1..input.len() - 1]
    } else {
        input
    }
}

fn parse_table_pattern(
    table: &str,
    schema: Option<&str>,
    default_schema: &str,
) -> SerializedTablePattern {
    let table = unquote(table).to_string();
    let _ = default_schema;
    SerializedTablePattern {
        connection: schema.map(|_| "default".to_string()),
        schema: schema.map(|value| value.to_string()),
        table,
    }
}

fn expand_with_queries(query_sql: &str, with_queries: &HashMap<String, String>) -> String {
    let mut expanded = query_sql.to_string();
    for (name, cte_sql) in with_queries {
        let inferred_column = infer_single_output_column(cte_sql);
        let from_pattern = format!("FROM {name}");
        let from_replacement = format!("FROM ({cte_sql}) AS {name}");
        expanded = expanded.replace(&from_pattern, &from_replacement);

        let from_pattern_lower = format!("from {name}");
        let from_replacement_lower = format!("from ({cte_sql}) as {name}");
        expanded = expanded.replace(&from_pattern_lower, &from_replacement_lower);

        let in_pattern = format!("IN {name}");
        let in_replacement = if let Some(column) = &inferred_column {
            format!("IN (SELECT {column} FROM ({cte_sql}) AS {name})")
        } else {
            format!("IN (SELECT * FROM ({cte_sql}) AS {name})")
        };
        expanded = expanded.replace(&in_pattern, &in_replacement);

        let in_pattern_lower = format!("in {name}");
        let in_replacement_lower = if let Some(column) = &inferred_column {
            format!("in (select {column} from ({cte_sql}) as {name})")
        } else {
            format!("in (select * from ({cte_sql}) as {name})")
        };
        expanded = expanded.replace(&in_pattern_lower, &in_replacement_lower);
    }

    expanded
}

fn infer_single_output_column(sql: &str) -> Option<String> {
    let normalized = normalize_sql_for_pg_query(sql);
    let parsed = parse_select_query(&normalized).ok()?;
    let column = parsed.columns.first()?;
    let SelectItemAst::Expr { expr, alias } = column else {
        return None;
    };
    alias
        .clone()
        .or_else(|| column_alias_from_expr(expr))
        .or_else(|| resolve_identifier(expr, &parsed).map(|column| column.column))
}

fn normalize_sql_for_pg_query(sql: &str) -> String {
    let lower = sql.to_ascii_lowercase();
    let mut output = String::new();
    let mut cursor = 0usize;

    while let Some(found) = lower[cursor..].find(" in array[") {
        let start = cursor + found;
        output.push_str(&sql[cursor..start]);
        output.push_str(" IN (");

        let array_start = start + " in array[".len();
        let mut index = array_start;
        let mut bracket_depth = 1i32;
        let chars = sql.as_bytes();

        while index < chars.len() && bracket_depth > 0 {
            match chars[index] {
                b'[' => bracket_depth += 1,
                b']' => bracket_depth -= 1,
                _ => {}
            }
            index += 1;
        }

        if bracket_depth != 0 {
            output.push_str(&sql[array_start..]);
            cursor = sql.len();
            break;
        }

        output.push_str(&sql[array_start..index - 1]);
        output.push(')');
        cursor = index;
    }

    output.push_str(&sql[cursor..]);
    output
}

#[derive(Debug, Clone)]
struct SelectQueryAst {
    tables: Vec<TableAst>,
    columns: Vec<SelectItemAst>,
    output_alias: String,
    where_expr: Option<ExprAst>,
    join_equalities: Vec<JoinEqualityAst>,
}

#[derive(Debug, Clone)]
struct TableAst {
    table: String,
    schema: Option<String>,
    alias: String,
    explicit_alias: bool,
    derived_query: Option<Box<SelectQueryAst>>,
    derived_columns: Option<Vec<SelectItemAst>>,
}

#[derive(Debug, Clone)]
struct JoinEqualityAst {
    left: QualifiedColumnAst,
    right: QualifiedColumnAst,
}

#[derive(Debug, Clone)]
struct QualifiedColumnAst {
    table_alias: String,
    column: String,
}

#[derive(Debug, Clone)]
struct LookupStageBuilder {
    table_alias: String,
    table: String,
    schema: Option<String>,
    partition_column: String,
    output_column: String,
    instantiation: SerializedParameterValue,
    filters: Vec<SqlExpression>,
}

#[derive(Debug, Clone)]
struct CompiledQueryBindings {
    data_filters: Vec<SqlExpression>,
    request_filters: Vec<SqlExpression>,
    partition_by: Vec<PartitionKey>,
    source_instantiation: Vec<SerializedParameterValue>,
    lookup_stages: Vec<Vec<SerializedExpandingLookup>>,
}

#[derive(Debug, Clone)]
enum SelectItemAst {
    Star { qualifier: Option<String> },
    Expr {
        expr: ExprAst,
        alias: Option<String>,
    },
}

#[derive(Debug, Clone)]
enum ExprAst {
    Identifier(String),
    String(String),
    Int(String),
    Double(f64),
    Bool(bool),
    Null,
    Function {
        name: String,
        args: Vec<ExprAst>,
    },
    Unary {
        op: String,
        operand: Box<ExprAst>,
    },
    Binary {
        left: Box<ExprAst>,
        op: String,
        right: Box<ExprAst>,
    },
    ScalarIn {
        target: Box<ExprAst>,
        values: Vec<ExprAst>,
    },
    Between {
        value: Box<ExprAst>,
        low: Box<ExprAst>,
        high: Box<ExprAst>,
    },
    Cast {
        expr: Box<ExprAst>,
        cast_as: String,
    },
    CaseWhen {
        operand: Option<Box<ExprAst>>,
        whens: Vec<(ExprAst, ExprAst)>,
        else_expr: Option<Box<ExprAst>>,
    },
    SubqueryIn {
        target: Box<ExprAst>,
        subquery: Box<SelectQueryAst>,
    },
}

fn parse_select_query(sql: &str) -> Result<SelectQueryAst, CompilerError> {
    let parsed = pg_query::parse(sql)
        .map_err(|e| CompilerError::Sql(format!("pg_query parse error: {e}")))?;

    if parsed.protobuf.stmts.len() != 1 {
        return Err(CompilerError::Unsupported(
            "only one SQL statement is supported in stream queries".to_string(),
        ));
    }

    let raw_stmt = parsed
        .protobuf
        .stmts
        .first()
        .ok_or_else(|| CompilerError::Sql("missing parsed SQL statement".to_string()))?;
    let stmt = raw_stmt
        .stmt
        .as_ref()
        .and_then(|node| node.node.as_ref())
        .ok_or_else(|| CompilerError::Sql("missing SQL statement AST".to_string()))?;

    let select = match stmt {
        PgNodeEnum::SelectStmt(select) => select,
        _ => {
            return Err(CompilerError::Unsupported(
                "only SELECT statements are supported in stream queries".to_string(),
            ));
        }
    };

    parse_select_stmt(select)
}

fn parse_select_stmt(select: &protobuf::SelectStmt) -> Result<SelectQueryAst, CompilerError> {
    if select.larg.is_some() || select.rarg.is_some() || select.op != 1 {
        return Err(CompilerError::Unsupported(
            "set operations are unsupported in this compiler subset".to_string(),
        ));
    }

    if select.from_clause.len() != 1 {
        return Err(CompilerError::Unsupported(
            "exactly one FROM source is supported in this compiler subset".to_string(),
        ));
    }

    let mut tables = Vec::new();
    let mut join_equalities = Vec::new();
    parse_from_item_collect(&select.from_clause[0], &mut tables, &mut join_equalities)?;

    if tables.is_empty() {
        return Err(CompilerError::Unsupported(
            "FROM clause must include at least one table source".to_string(),
        ));
    }

    let mut columns = Vec::new();
    for target in &select.target_list {
        columns.push(parse_select_item(target)?);
    }

    let mut star_qualifier: Option<Option<String>> = None;
    for item in &columns {
        if let SelectItemAst::Star { qualifier } = item {
            match &star_qualifier {
                None => star_qualifier = Some(qualifier.clone()),
                Some(existing) if *existing == *qualifier => {}
                _ => {
                    return Err(CompilerError::Unsupported(
                        "multiple star qualifiers in one SELECT list are unsupported".to_string(),
                    ));
                }
            }
        }
    }

    let output_alias = match star_qualifier {
        Some(Some(alias)) => alias,
        Some(None) => tables[0].alias.clone(),
        None => tables[0].alias.clone(),
    };

    let where_expr = select
        .where_clause
        .as_ref()
        .map(|value| parse_expr_node(value.as_ref()))
        .transpose()?;

    Ok(SelectQueryAst {
        tables,
        columns,
        output_alias,
        where_expr,
        join_equalities,
    })
}

fn parse_from_item_collect(
    node: &protobuf::Node,
    tables: &mut Vec<TableAst>,
    join_equalities: &mut Vec<JoinEqualityAst>,
) -> Result<(), CompilerError> {
    match node.node.as_ref() {
        Some(PgNodeEnum::RangeVar(range)) => {
            let alias = range
                .alias
                .as_ref()
                .map(|alias| alias.aliasname.clone())
                .filter(|alias| !alias.is_empty());
            let schema = if range.schemaname.is_empty() {
                None
            } else {
                Some(range.schemaname.clone())
            };
            let explicit_alias = alias.is_some();
            let alias = alias.unwrap_or_else(|| range.relname.clone());

            tables.push(TableAst {
                table: range.relname.clone(),
                schema,
                alias,
                explicit_alias,
                derived_query: None,
                derived_columns: None,
            });
            Ok(())
        }
        Some(PgNodeEnum::JoinExpr(join_expr)) => {
            let left = join_expr.larg.as_ref().ok_or_else(|| {
                CompilerError::Sql("JOIN is missing left relation".to_string())
            })?;
            let right = join_expr.rarg.as_ref().ok_or_else(|| {
                CompilerError::Sql("JOIN is missing right relation".to_string())
            })?;

            parse_from_item_collect(left.as_ref(), tables, join_equalities)?;
            parse_from_item_collect(right.as_ref(), tables, join_equalities)?;

            if let Some(quals) = &join_expr.quals {
                let join_expr = parse_expr_node(quals.as_ref())?;
                collect_join_equalities(&join_expr, join_equalities)?;
            }

            Ok(())
        }
        Some(PgNodeEnum::RangeSubselect(range_subselect)) => {
            let alias = range_subselect
                .alias
                .as_ref()
                .map(|alias| alias.aliasname.clone())
                .filter(|alias| !alias.is_empty())
                .ok_or_else(|| {
                    CompilerError::Unsupported(
                        "subqueries in FROM must have an alias in this compiler subset".to_string(),
                    )
                })?;
            let subquery = range_subselect.subquery.as_ref().ok_or_else(|| {
                CompilerError::Sql("FROM subquery is missing SELECT body".to_string())
            })?;
            let Some(PgNodeEnum::SelectStmt(select)) = subquery.node.as_ref() else {
                return Err(CompilerError::Unsupported(
                    "FROM subquery must be a SELECT statement".to_string(),
                ));
            };
            let inner_query = parse_select_stmt(select)?;
            let inner_output_table = inner_query
                .tables
                .iter()
                .find(|table| table.alias == inner_query.output_alias)
                .ok_or_else(|| {
                    CompilerError::Unsupported(
                        "subquery output table could not be resolved".to_string(),
                    )
                })?;

            let mut derived_columns = Vec::new();
            for column in &inner_query.columns {
                let SelectItemAst::Expr { expr, alias } = column else {
                    return Err(CompilerError::Unsupported(
                        "* columns are not allowed in subqueries or common table expressions"
                            .to_string(),
                    ));
                };
                let resolved_alias = alias
                    .clone()
                    .or_else(|| column_alias_from_expr(expr))
                    .ok_or_else(|| {
                        CompilerError::Unsupported(
                            "subquery result columns must have a name".to_string(),
                        )
                    })?;
                derived_columns.push(SelectItemAst::Expr {
                    expr: expr.clone(),
                    alias: Some(resolved_alias),
                });
            }

            if let Some(alias_def) = &range_subselect.alias {
                if !alias_def.colnames.is_empty() {
                    if alias_def.colnames.len() != derived_columns.len() {
                        return Err(CompilerError::Unsupported(format!(
                            "Expected this subquery to have {} columns, it actually has {}",
                            alias_def.colnames.len(),
                            derived_columns.len()
                        )));
                    }
                    for (index, column_name) in alias_def.colnames.iter().enumerate() {
                        let Some(PgNodeEnum::String(value)) = column_name.node.as_ref() else {
                            return Err(CompilerError::Unsupported(
                                "subquery column aliases must be identifiers".to_string(),
                            ));
                        };
                        if let SelectItemAst::Expr { alias, .. } = &mut derived_columns[index] {
                            *alias = Some(value.sval.clone());
                        }
                    }
                }
            }

            tables.push(TableAst {
                table: inner_output_table.table.clone(),
                schema: inner_output_table.schema.clone(),
                alias,
                explicit_alias: true,
                derived_query: Some(Box::new(inner_query)),
                derived_columns: Some(derived_columns),
            });
            Ok(())
        }
        Some(PgNodeEnum::RangeFunction(_)) => Err(CompilerError::Unsupported(
            "table-valued functions in FROM are unsupported in this compiler subset".to_string(),
        )),
        _ => Err(CompilerError::Unsupported(
            "unsupported FROM clause source in this compiler subset".to_string(),
        )),
    }
}

fn collect_join_equalities(
    expr: &ExprAst,
    join_equalities: &mut Vec<JoinEqualityAst>,
) -> Result<(), CompilerError> {
    match expr {
        ExprAst::Binary { left, op, right } if op.eq_ignore_ascii_case("and") => {
            collect_join_equalities(left, join_equalities)?;
            collect_join_equalities(right, join_equalities)?;
            Ok(())
        }
        ExprAst::Binary { left, op, right } if op == "=" => {
            let left = parse_qualified_column(left).ok_or_else(|| {
                CompilerError::Unsupported(
                    "JOIN conditions must compare qualified table columns".to_string(),
                )
            })?;
            let right = parse_qualified_column(right).ok_or_else(|| {
                CompilerError::Unsupported(
                    "JOIN conditions must compare qualified table columns".to_string(),
                )
            })?;
            join_equalities.push(JoinEqualityAst { left, right });
            Ok(())
        }
        _ => Err(CompilerError::Unsupported(
            "JOIN conditions must be conjunctions of equality comparisons".to_string(),
        )),
    }
}

fn parse_qualified_column(expr: &ExprAst) -> Option<QualifiedColumnAst> {
    let ExprAst::Identifier(identifier) = expr else {
        return None;
    };
    let (table_alias, column) = identifier.rsplit_once('.')?;
    if table_alias.is_empty() || column.is_empty() {
        return None;
    }

    Some(QualifiedColumnAst {
        table_alias: table_alias.to_string(),
        column: column.to_string(),
    })
}

fn parse_select_item(node: &protobuf::Node) -> Result<SelectItemAst, CompilerError> {
    let Some(PgNodeEnum::ResTarget(target)) = node.node.as_ref() else {
        return Err(CompilerError::Sql(
            "expected SELECT target item in parsed SQL".to_string(),
        ));
    };

    if !target.indirection.is_empty() {
        return Err(CompilerError::Unsupported(
            "SELECT target indirection is unsupported in this compiler subset".to_string(),
        ));
    }

    let value = target
        .val
        .as_ref()
        .ok_or_else(|| CompilerError::Sql("missing SELECT target value".to_string()))?;

    if let Some(qualifier) = parse_star_qualifier(value.as_ref()) {
        if !target.name.is_empty() {
            return Err(CompilerError::Unsupported(
                "star projection cannot have an alias in this compiler subset".to_string(),
            ));
        }
        return Ok(SelectItemAst::Star { qualifier });
    }

    let expr = parse_expr_node(value.as_ref())?;
    let alias = if target.name.is_empty() {
        None
    } else {
        Some(target.name.clone())
    };

    Ok(SelectItemAst::Expr { expr, alias })
}

fn parse_star_qualifier(node: &protobuf::Node) -> Option<Option<String>> {
    let Some(PgNodeEnum::ColumnRef(column_ref)) = node.node.as_ref() else {
        return None;
    };

    if column_ref.fields.is_empty() {
        return None;
    }

    let Some(last) = column_ref.fields.last() else {
        return None;
    };
    let Some(PgNodeEnum::AStar(_)) = last.node.as_ref() else {
        return None;
    };

    if !column_ref
        .fields
        .iter()
        .take(column_ref.fields.len().saturating_sub(1))
        .all(|field| matches!(field.node.as_ref(), Some(PgNodeEnum::String(_))))
    {
        return None;
    }

    let qualifier_parts = column_ref
        .fields
        .iter()
        .take(column_ref.fields.len().saturating_sub(1))
        .filter_map(|field| match field.node.as_ref() {
            Some(PgNodeEnum::String(value)) => Some(value.sval.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();

    if qualifier_parts.is_empty() {
        Some(None)
    } else {
        Some(Some(qualifier_parts.join(".")))
    }
}

fn parse_expr_node(node: &protobuf::Node) -> Result<ExprAst, CompilerError> {
    match node.node.as_ref() {
        Some(PgNodeEnum::ColumnRef(column_ref)) => parse_column_ref(column_ref),
        Some(PgNodeEnum::AConst(a_const)) => parse_a_const(a_const),
        Some(PgNodeEnum::FuncCall(func_call)) => {
            let name = parse_name_from_nodes(&func_call.funcname)?;
            let mut args = Vec::with_capacity(func_call.args.len());
            for arg in &func_call.args {
                args.push(parse_expr_node(arg)?);
            }
            Ok(ExprAst::Function { name, args })
        }
        Some(PgNodeEnum::AExpr(a_expr)) => parse_a_expr(a_expr),
        Some(PgNodeEnum::BoolExpr(bool_expr)) => parse_bool_expr(bool_expr),
        Some(PgNodeEnum::NullTest(null_test)) => parse_null_test(null_test),
        Some(PgNodeEnum::TypeCast(type_cast)) => parse_type_cast(type_cast),
        Some(PgNodeEnum::CaseExpr(case_expr)) => parse_case_expr(case_expr),
        Some(PgNodeEnum::SubLink(sub_link)) => parse_sub_link(sub_link),
        Some(PgNodeEnum::CoalesceExpr(coalesce_expr)) => {
            let mut args = Vec::with_capacity(coalesce_expr.args.len());
            for arg in &coalesce_expr.args {
                args.push(parse_expr_node(arg)?);
            }
            Ok(ExprAst::Function {
                name: "coalesce".to_string(),
                args,
            })
        }
        Some(PgNodeEnum::NullIfExpr(null_if_expr)) => {
            let left = null_if_expr
                .args
                .first()
                .ok_or_else(|| CompilerError::Sql("NULLIF missing left argument".to_string()))
                .and_then(parse_expr_node)?;
            let right = null_if_expr
                .args
                .get(1)
                .ok_or_else(|| CompilerError::Sql("NULLIF missing right argument".to_string()))
                .and_then(parse_expr_node)?;

            Ok(ExprAst::CaseWhen {
                operand: None,
                whens: vec![(
                    ExprAst::Binary {
                        left: Box::new(left.clone()),
                        op: "=".to_string(),
                        right: Box::new(right),
                    },
                    ExprAst::Null,
                )],
                else_expr: Some(Box::new(left)),
            })
        }
        _ => Err(CompilerError::Unsupported(format!(
            "unsupported SQL expression in compiler subset: {}",
            node_kind(node)
        ))),
    }
}

fn parse_column_ref(column_ref: &protobuf::ColumnRef) -> Result<ExprAst, CompilerError> {
    let mut parts = Vec::new();
    for field in &column_ref.fields {
        match field.node.as_ref() {
            Some(PgNodeEnum::String(value)) => parts.push(value.sval.clone()),
            Some(PgNodeEnum::AStar(_)) => {
                return Err(CompilerError::Unsupported(
                    "qualified star expressions are unsupported in this compiler subset"
                        .to_string(),
                ));
            }
            _ => {
                return Err(CompilerError::Unsupported(
                    "unsupported column reference in this compiler subset".to_string(),
                ));
            }
        }
    }

    if parts.is_empty() {
        return Err(CompilerError::Sql(
            "empty column reference in parsed SQL".to_string(),
        ));
    }

    Ok(ExprAst::Identifier(parts.join(".")))
}

fn parse_a_const(value: &protobuf::AConst) -> Result<ExprAst, CompilerError> {
    if value.isnull {
        return Ok(ExprAst::Null);
    }

    match value.val.as_ref() {
        Some(protobuf::a_const::Val::Ival(number)) => Ok(ExprAst::Int(number.ival.to_string())),
        Some(protobuf::a_const::Val::Fval(number)) => {
            let parsed = number.fval.parse::<f64>().map_err(|_| {
                CompilerError::Sql(format!("invalid floating-point literal: {}", number.fval))
            })?;
            Ok(ExprAst::Double(parsed))
        }
        Some(protobuf::a_const::Val::Boolval(value)) => Ok(ExprAst::Bool(value.boolval)),
        Some(protobuf::a_const::Val::Sval(value)) => Ok(ExprAst::String(value.sval.clone())),
        Some(protobuf::a_const::Val::Bsval(value)) => Ok(ExprAst::String(value.bsval.clone())),
        None => Ok(ExprAst::Null),
    }
}

fn parse_a_expr(a_expr: &protobuf::AExpr) -> Result<ExprAst, CompilerError> {
    let kind = protobuf::AExprKind::try_from(a_expr.kind).unwrap_or(protobuf::AExprKind::Undefined);
    let operator = parse_operator_name(&a_expr.name);

    match kind {
        protobuf::AExprKind::AexprOp => {
            let right = a_expr
                .rexpr
                .as_ref()
                .ok_or_else(|| {
                    CompilerError::Sql("operator expression missing right operand".to_string())
                })
                .and_then(|value| parse_expr_node(value.as_ref()))?;
            if let Some(left) = &a_expr.lexpr {
                let left = parse_expr_node(left.as_ref())?;
                let op = operator?;
                if op == "->" || op == "->>" {
                    Ok(ExprAst::Function {
                        name: op,
                        args: vec![left, right],
                    })
                } else {
                    Ok(ExprAst::Binary {
                        left: Box::new(left),
                        op,
                        right: Box::new(right),
                    })
                }
            } else {
                Ok(ExprAst::Unary {
                    op: operator?,
                    operand: Box::new(right),
                })
            }
        }
        protobuf::AExprKind::AexprIn => {
            let target = a_expr
                .lexpr
                .as_ref()
                .ok_or_else(|| CompilerError::Sql("IN expression missing left operand".to_string()))
                .and_then(|value| parse_expr_node(value.as_ref()))?;
            let right = a_expr.rexpr.as_ref().ok_or_else(|| {
                CompilerError::Sql("IN expression missing right side".to_string())
            })?;

            let values = parse_list_node(right.as_ref())?;
            Ok(ExprAst::ScalarIn {
                target: Box::new(target),
                values,
            })
        }
        protobuf::AExprKind::AexprBetween | protobuf::AExprKind::AexprNotBetween => {
            let value = a_expr
                .lexpr
                .as_ref()
                .ok_or_else(|| CompilerError::Sql("BETWEEN expression missing value".to_string()))
                .and_then(|value| parse_expr_node(value.as_ref()))?;
            let right = a_expr.rexpr.as_ref().ok_or_else(|| {
                CompilerError::Sql("BETWEEN expression missing bounds".to_string())
            })?;

            let mut bounds = parse_list_node(right.as_ref())?;
            if bounds.len() != 2 {
                return Err(CompilerError::Sql(
                    "BETWEEN requires exactly two bound expressions".to_string(),
                ));
            }
            let high = bounds.pop().expect("bounds length checked");
            let low = bounds.pop().expect("bounds length checked");

            let between = ExprAst::Between {
                value: Box::new(value),
                low: Box::new(low),
                high: Box::new(high),
            };
            if kind == protobuf::AExprKind::AexprNotBetween {
                Ok(ExprAst::Unary {
                    op: "not".to_string(),
                    operand: Box::new(between),
                })
            } else {
                Ok(between)
            }
        }
        protobuf::AExprKind::AexprLike
        | protobuf::AExprKind::AexprIlike
        | protobuf::AExprKind::AexprSimilar => {
            let left = a_expr
                .lexpr
                .as_ref()
                .ok_or_else(|| {
                    CompilerError::Sql("pattern expression missing left operand".to_string())
                })
                .and_then(|value| parse_expr_node(value.as_ref()))?;
            let right = a_expr
                .rexpr
                .as_ref()
                .ok_or_else(|| {
                    CompilerError::Sql("pattern expression missing right operand".to_string())
                })
                .and_then(|value| parse_expr_node(value.as_ref()))?;

            Ok(ExprAst::Binary {
                left: Box::new(left),
                op: operator?,
                right: Box::new(right),
            })
        }
        _ => Err(CompilerError::Unsupported(format!(
            "unsupported operator expression kind in this compiler subset: {kind:?}"
        ))),
    }
}

fn parse_bool_expr(bool_expr: &protobuf::BoolExpr) -> Result<ExprAst, CompilerError> {
    let kind = protobuf::BoolExprType::try_from(bool_expr.boolop)
        .unwrap_or(protobuf::BoolExprType::Undefined);
    let mut args = Vec::with_capacity(bool_expr.args.len());
    for arg in &bool_expr.args {
        args.push(parse_expr_node(arg)?);
    }
    if args.is_empty() {
        return Err(CompilerError::Sql(
            "boolean expression must have at least one argument".to_string(),
        ));
    }

    match kind {
        protobuf::BoolExprType::AndExpr | protobuf::BoolExprType::OrExpr => {
            let op = if kind == protobuf::BoolExprType::AndExpr {
                "and"
            } else {
                "or"
            };

            let mut iter = args.into_iter();
            let first = iter.next().ok_or_else(|| {
                CompilerError::Sql("boolean expression has no arguments".to_string())
            })?;
            Ok(iter.fold(first, |left, right| ExprAst::Binary {
                left: Box::new(left),
                op: op.to_string(),
                right: Box::new(right),
            }))
        }
        protobuf::BoolExprType::NotExpr => {
            if args.len() != 1 {
                return Err(CompilerError::Sql(
                    "NOT expression must have exactly one argument".to_string(),
                ));
            }
            Ok(ExprAst::Unary {
                op: "not".to_string(),
                operand: Box::new(args.into_iter().next().expect("length checked")),
            })
        }
        _ => Err(CompilerError::Unsupported(
            "unsupported boolean expression in this compiler subset".to_string(),
        )),
    }
}

fn parse_null_test(null_test: &protobuf::NullTest) -> Result<ExprAst, CompilerError> {
    let arg = null_test
        .arg
        .as_ref()
        .ok_or_else(|| CompilerError::Sql("NULL test missing argument".to_string()))
        .and_then(|value| parse_expr_node(value.as_ref()))?;
    let kind = protobuf::NullTestType::try_from(null_test.nulltesttype)
        .unwrap_or(protobuf::NullTestType::Undefined);

    let is_expr = ExprAst::Binary {
        left: Box::new(arg),
        op: "is".to_string(),
        right: Box::new(ExprAst::Null),
    };

    match kind {
        protobuf::NullTestType::IsNull => Ok(is_expr),
        protobuf::NullTestType::IsNotNull => Ok(ExprAst::Unary {
            op: "not".to_string(),
            operand: Box::new(is_expr),
        }),
        _ => Err(CompilerError::Unsupported(
            "unsupported NULL test in this compiler subset".to_string(),
        )),
    }
}

fn parse_type_cast(type_cast: &protobuf::TypeCast) -> Result<ExprAst, CompilerError> {
    let expr = type_cast
        .arg
        .as_ref()
        .ok_or_else(|| CompilerError::Sql("CAST missing value expression".to_string()))
        .and_then(|value| parse_expr_node(value.as_ref()))?;
    let type_name = type_cast
        .type_name
        .as_ref()
        .ok_or_else(|| CompilerError::Sql("CAST missing type name".to_string()))?;
    let cast_name = parse_name_from_nodes(&type_name.names)?;
    let cast_as = cast_name
        .split('.')
        .last()
        .unwrap_or(cast_name.as_str())
        .to_ascii_lowercase();

    Ok(ExprAst::Cast {
        expr: Box::new(expr),
        cast_as,
    })
}

fn parse_case_expr(case_expr: &protobuf::CaseExpr) -> Result<ExprAst, CompilerError> {
    let operand = case_expr
        .arg
        .as_ref()
        .map(|value| parse_expr_node(value.as_ref()).map(Box::new))
        .transpose()?;

    let mut whens = Vec::with_capacity(case_expr.args.len());
    for arg in &case_expr.args {
        let Some(PgNodeEnum::CaseWhen(case_when)) = arg.node.as_ref() else {
            return Err(CompilerError::Sql(
                "CASE expression contains a non-WHEN branch".to_string(),
            ));
        };
        let when_expr = case_when
            .expr
            .as_ref()
            .ok_or_else(|| CompilerError::Sql("CASE WHEN missing condition".to_string()))
            .and_then(|value| parse_expr_node(value.as_ref()))?;
        let then_expr = case_when
            .result
            .as_ref()
            .ok_or_else(|| CompilerError::Sql("CASE WHEN missing result".to_string()))
            .and_then(|value| parse_expr_node(value.as_ref()))?;
        whens.push((when_expr, then_expr));
    }

    let else_expr = case_expr
        .defresult
        .as_ref()
        .map(|value| parse_expr_node(value.as_ref()).map(Box::new))
        .transpose()?;

    Ok(ExprAst::CaseWhen {
        operand,
        whens,
        else_expr,
    })
}

fn parse_sub_link(sub_link: &protobuf::SubLink) -> Result<ExprAst, CompilerError> {
    let kind = protobuf::SubLinkType::try_from(sub_link.sub_link_type)
        .unwrap_or(protobuf::SubLinkType::Undefined);
    if kind != protobuf::SubLinkType::AnySublink {
        return Err(CompilerError::Unsupported(
            "only IN (SELECT ...) subqueries are supported in this compiler subset".to_string(),
        ));
    }

    let operator = if sub_link.oper_name.is_empty() {
        "=".to_string()
    } else {
        parse_operator_name(&sub_link.oper_name)?
    };
    if operator != "=" {
        return Err(CompilerError::Unsupported(
            "only IN (SELECT ...) equality subqueries are supported".to_string(),
        ));
    }

    let target = sub_link
        .testexpr
        .as_ref()
        .ok_or_else(|| CompilerError::Sql("subquery IN expression missing left operand".to_string()))
        .and_then(|value| parse_expr_node(value.as_ref()))?;

    let subselect = sub_link
        .subselect
        .as_ref()
        .ok_or_else(|| CompilerError::Sql("subquery IN expression missing SELECT".to_string()))?;
    let Some(PgNodeEnum::SelectStmt(select)) = subselect.node.as_ref() else {
        return Err(CompilerError::Unsupported(
            "only SELECT subqueries are supported in IN expressions".to_string(),
        ));
    };

    Ok(ExprAst::SubqueryIn {
        target: Box::new(target),
        subquery: Box::new(parse_select_stmt(select)?),
    })
}

fn parse_list_node(node: &protobuf::Node) -> Result<Vec<ExprAst>, CompilerError> {
    let Some(PgNodeEnum::List(list)) = node.node.as_ref() else {
        return Err(CompilerError::Unsupported(
            "expected expression list in this compiler subset".to_string(),
        ));
    };

    let mut values = Vec::with_capacity(list.items.len());
    for item in &list.items {
        values.push(parse_expr_node(item)?);
    }
    Ok(values)
}

fn parse_operator_name(nodes: &[protobuf::Node]) -> Result<String, CompilerError> {
    parse_name_from_nodes(nodes)
}

fn parse_name_from_nodes(nodes: &[protobuf::Node]) -> Result<String, CompilerError> {
    let mut parts = Vec::new();
    for node in nodes {
        match node.node.as_ref() {
            Some(PgNodeEnum::String(value)) => parts.push(value.sval.clone()),
            _ => {
                return Err(CompilerError::Sql(
                    "expected string node in identifier/operator name".to_string(),
                ));
            }
        }
    }

    if parts.is_empty() {
        return Err(CompilerError::Sql(
            "empty identifier/operator name in parsed SQL".to_string(),
        ));
    }

    Ok(parts.join("."))
}

fn node_kind(node: &protobuf::Node) -> &'static str {
    match node.node.as_ref() {
        Some(PgNodeEnum::ColumnRef(_)) => "column_ref",
        Some(PgNodeEnum::AConst(_)) => "a_const",
        Some(PgNodeEnum::FuncCall(_)) => "func_call",
        Some(PgNodeEnum::AExpr(_)) => "a_expr",
        Some(PgNodeEnum::BoolExpr(_)) => "bool_expr",
        Some(PgNodeEnum::SubLink(_)) => "sub_link",
        Some(PgNodeEnum::NullTest(_)) => "null_test",
        Some(PgNodeEnum::CaseExpr(_)) => "case_expr",
        Some(PgNodeEnum::TypeCast(_)) => "type_cast",
        Some(PgNodeEnum::CoalesceExpr(_)) => "coalesce_expr",
        Some(PgNodeEnum::NullIfExpr(_)) => "null_if_expr",
        Some(_) => "other",
        None => "empty",
    }
}

fn column_alias_from_expr(expr: &ExprAst) -> Option<String> {
    match expr {
        ExprAst::Identifier(value) => Some(value.split('.').last().unwrap_or(value).to_string()),
        _ => None,
    }
}

fn transform_expr(expr: &ExprAst) -> Result<SqlExpression, CompilerError> {
    match expr {
        ExprAst::Identifier(identifier) => Ok(SqlExpression::Data {
            source: ExternalSource::Column {
                column: identifier
                    .split('.')
                    .last()
                    .unwrap_or(identifier)
                    .to_string(),
            },
        }),
        ExprAst::String(value) => Ok(SqlExpression::LitString {
            value: value.clone(),
        }),
        ExprAst::Int(base10) => Ok(SqlExpression::LitInt {
            base10: base10.clone(),
        }),
        ExprAst::Double(value) => Ok(SqlExpression::LitDouble { value: *value }),
        ExprAst::Bool(value) => Ok(SqlExpression::LitInt {
            base10: if *value { "1" } else { "0" }.to_string(),
        }),
        ExprAst::Null => Ok(SqlExpression::LitNull),
        ExprAst::Unary { op, operand } => Ok(SqlExpression::Unary {
            operand: Box::new(transform_expr(operand)?),
            operator: op.clone(),
        }),
        ExprAst::Binary { left, op, right } => {
            if op == "!=" || op == "<>" {
                Ok(SqlExpression::Unary {
                    operator: "not".to_string(),
                    operand: Box::new(SqlExpression::Binary {
                        left: Box::new(transform_expr(left)?),
                        operator: "=".to_string(),
                        right: Box::new(transform_expr(right)?),
                    }),
                })
            } else {
                Ok(SqlExpression::Binary {
                    left: Box::new(transform_expr(left)?),
                    operator: op.to_ascii_lowercase(),
                    right: Box::new(transform_expr(right)?),
                })
            }
        }
        ExprAst::ScalarIn { target, values } => Ok(SqlExpression::ScalarIn {
            target: Box::new(transform_expr(target)?),
            in_values: values
                .iter()
                .map(transform_expr)
                .collect::<Result<Vec<_>, _>>()?,
        }),
        ExprAst::Between { value, low, high } => Ok(SqlExpression::Between {
            value: Box::new(transform_expr(value)?),
            low: Box::new(transform_expr(low)?),
            high: Box::new(transform_expr(high)?),
        }),
        ExprAst::Cast { expr, cast_as } => Ok(SqlExpression::Cast {
            operand: Box::new(transform_expr(expr)?),
            cast_as: cast_as.clone(),
        }),
        ExprAst::CaseWhen {
            operand,
            whens,
            else_expr,
        } => Ok(SqlExpression::CaseWhen {
            operand: operand
                .as_ref()
                .map(|value| transform_expr(value).map(Box::new))
                .transpose()?,
            whens: whens
                .iter()
                .map(|(when, then)| {
                    Ok(CaseWhenBranch {
                        when: transform_expr(when)?,
                        then: transform_expr(then)?,
                    })
                })
                .collect::<Result<Vec<_>, CompilerError>>()?,
            else_expr: else_expr
                .as_ref()
                .map(|value| transform_expr(value).map(Box::new))
                .transpose()?,
        }),
        ExprAst::Function { name, args } => transform_function(name, args),
        ExprAst::SubqueryIn { .. } => Err(CompilerError::Unsupported(
            "subqueries are unsupported in expression transform".to_string(),
        )),
    }
}

fn transform_function(name: &str, args: &[ExprAst]) -> Result<SqlExpression, CompilerError> {
    let lowered = name.to_ascii_lowercase();

    if lowered == "auth.user_id" {
        if !args.is_empty() {
            return Err(CompilerError::Sql(
                "auth.user_id() takes no arguments".to_string(),
            ));
        }

        return Ok(SqlExpression::Function {
            function: "->>".to_string(),
            parameters: vec![
                SqlExpression::Data {
                    source: ExternalSource::Request {
                        request: "auth".to_string(),
                    },
                },
                SqlExpression::LitString {
                    value: "$.sub".to_string(),
                },
            ],
        });
    }

    let parameter_source = match lowered.as_str() {
        "auth.parameter" => Some("auth"),
        "subscription.parameter" => Some("subscription"),
        "connection.parameter" => Some("connection"),
        _ => None,
    };

    if let Some(source) = parameter_source {
        if args.len() != 1 {
            return Err(CompilerError::Sql(format!("{name}() expects one argument")));
        }

        return Ok(SqlExpression::Function {
            function: "->>".to_string(),
            parameters: vec![
                SqlExpression::Data {
                    source: ExternalSource::Request {
                        request: source.to_string(),
                    },
                },
                transform_expr(&args[0])?,
            ],
        });
    }

    let object_source = match lowered.as_str() {
        "auth.parameters" => Some("auth"),
        "subscription.parameters" => Some("subscription"),
        "connection.parameters" => Some("connection"),
        _ => None,
    };

    if let Some(source) = object_source {
        if !args.is_empty() {
            return Err(CompilerError::Sql(format!("{name}() expects no arguments")));
        }

        return Ok(SqlExpression::Data {
            source: ExternalSource::Request {
                request: source.to_string(),
            },
        });
    }

    Ok(SqlExpression::Function {
        function: lowered,
        parameters: args
            .iter()
            .map(transform_expr)
            .collect::<Result<Vec<_>, _>>()?,
    })
}

fn compile_query_bindings(
    query: &SelectQueryAst,
    query_sql: &str,
    parameter_indexes: &mut Vec<SerializedParameterIndexLookupCreator>,
    parameter_index_signatures: &mut HashMap<String, usize>,
) -> Result<CompiledQueryBindings, CompilerError> {
    let output_alias = query.output_alias.clone();
    if let Some(where_expr) = &query.where_expr {
        if !expr_contains_subquery(where_expr) {
            let transformed = transform_expr(where_expr)?;
            if classify_expression(&transformed) == ReferenceKind::Row {
                return Ok(CompiledQueryBindings {
                    data_filters: vec![transformed],
                    request_filters: Vec::new(),
                    partition_by: Vec::new(),
                    source_instantiation: Vec::new(),
                    lookup_stages: Vec::new(),
                });
            }
        }
    }

    let mut data_filters = Vec::new();
    let mut request_filters = Vec::new();
    let mut partition_bindings: Vec<(PartitionKey, SerializedParameterValue)> = Vec::new();
    let mut lookup_builders: Vec<LookupStageBuilder> = Vec::new();
    let mut table_filters: HashMap<String, Vec<SqlExpression>> = HashMap::new();
    let mut request_seeds: Vec<(QualifiedColumnAst, SqlExpression)> = Vec::new();
    let mut join_equalities = query.join_equalities.clone();

    let mut conjuncts = Vec::new();
    if let Some(where_expr) = &query.where_expr {
        flatten_and_ast(where_expr, &mut conjuncts);
    }

    for conjunct in conjuncts {
        if let Some(eq) = extract_identifier_identifier_equality(&conjunct, query) {
            if eq.left.table_alias != eq.right.table_alias {
                join_equalities.push(eq);
                continue;
            }
        }

        if let Some((target, subquery)) = extract_subquery_in_binding(&conjunct, query) {
            let (mut chain, mut value) = compile_subquery_chain(&subquery, query_sql)?;
            let stage_offset = lookup_builders.len();
            offset_lookup_builders(&mut chain, stage_offset);
            offset_parameter_value(&mut value, stage_offset);
            lookup_builders.extend(chain);
            if target.table_alias == output_alias {
                partition_bindings.push((
                    PartitionKey {
                        expr: column_expression(&target.column),
                    },
                    value,
                ));
            } else {
                let (mut join_chain, partition_column, mut source_value) = build_join_lookup_chain(
                    query,
                    &join_equalities,
                    &output_alias,
                    &target,
                    value,
                    query_sql,
                )?;
                let join_offset = lookup_builders.len();
                offset_join_lookup_builders(&mut join_chain, join_offset);
                offset_parameter_value(&mut source_value, join_offset);
                lookup_builders.extend(join_chain);
                partition_bindings.push((
                    PartitionKey {
                        expr: column_expression(&partition_column),
                    },
                    source_value,
                ));
            }
            continue;
        }

        if let Some((column, request_expr)) = extract_identifier_request_equality(&conjunct, query)? {
            request_seeds.push((column, request_expr));
            continue;
        }

        let transformed = transform_expr(&conjunct)?;
        match classify_expression(&transformed) {
            ReferenceKind::Row => {
                let aliases = referenced_aliases(&conjunct, query);
                if aliases.is_empty() || aliases.iter().all(|alias| alias == &output_alias) {
                    data_filters.push(transformed);
                } else if aliases.len() == 1 {
                    table_filters
                        .entry(aliases[0].clone())
                        .or_default()
                        .push(transformed);
                } else {
                    return Err(CompilerError::Unsupported(format!(
                        "row filters across multiple tables are unsupported in this compiler subset: {query_sql}"
                    )));
                }
            }
            ReferenceKind::Request | ReferenceKind::Static => request_filters.push(transformed),
            ReferenceKind::Mixed => {
                return Err(CompilerError::Unsupported(format!(
                    "mixed row/request expression is unsupported in this compiler subset: {query_sql}"
                )));
            }
        }
    }

    for (seed_column, seed_expr) in request_seeds {
        if seed_column.table_alias == output_alias {
            partition_bindings.push((
                PartitionKey {
                    expr: column_expression(&seed_column.column),
                },
                SerializedParameterValue::Request { expr: seed_expr },
            ));
            continue;
        }

        let (mut chain, partition_column, mut source_value) = build_join_lookup_chain(
            query,
            &join_equalities,
            &output_alias,
            &seed_column,
            SerializedParameterValue::Request { expr: seed_expr },
            query_sql,
        )?;
        let stage_offset = lookup_builders.len();
        offset_lookup_builders(&mut chain, stage_offset);
        offset_parameter_value(&mut source_value, stage_offset);
        lookup_builders.extend(chain);

        partition_bindings.push((
            PartitionKey {
                expr: column_expression(&partition_column),
            },
            source_value,
        ));
    }

    partition_bindings.sort_by(|(left_partition, _), (right_partition, _)| {
        let left = serde_json::to_string(&left_partition.expr).unwrap_or_else(|_| String::new());
        let right = serde_json::to_string(&right_partition.expr).unwrap_or_else(|_| String::new());
        right.cmp(&left)
    });
    partition_bindings.dedup_by(|(left_partition, _), (right_partition, _)| {
        left_partition.expr == right_partition.expr
    });

    let mut lookup_stages = Vec::new();
    for builder in &mut lookup_builders {
        if let Some(filters) = table_filters.remove(&builder.table_alias) {
            builder.filters.extend(filters);
        }
    }
    for builder in lookup_builders {
        let lookup_index = register_parameter_index(
            &builder,
            parameter_indexes,
            parameter_index_signatures,
        )?;
        lookup_stages.push(vec![SerializedExpandingLookup::Parameter {
            lookup: lookup_index,
            instantiation: vec![builder.instantiation],
        }]);
    }

    let mut partition_by = Vec::new();
    let mut source_instantiation = Vec::new();
    for (partition, value) in partition_bindings {
        partition_by.push(partition);
        source_instantiation.push(value);
    }

    Ok(CompiledQueryBindings {
        data_filters,
        request_filters,
        partition_by,
        source_instantiation,
        lookup_stages,
    })
}

fn register_parameter_index(
    builder: &LookupStageBuilder,
    parameter_indexes: &mut Vec<SerializedParameterIndexLookupCreator>,
    parameter_index_signatures: &mut HashMap<String, usize>,
) -> Result<usize, CompilerError> {
    let table = SerializedTablePattern {
        connection: builder.schema.as_ref().map(|_| "default".to_string()),
        schema: builder.schema.clone(),
        table: builder.table.clone(),
    };
    let partition_by = vec![PartitionKey {
        expr: column_expression(&builder.partition_column),
    }];
    let output = vec![column_expression(&builder.output_column)];
    let signature = serde_json::to_string(&(table.clone(), partition_by.clone(), output.clone(), builder.filters.clone()))?;

    if let Some(index) = parameter_index_signatures.get(&signature) {
        return Ok(*index);
    }

    let index = parameter_indexes.len();
    let mut parameter_index = SerializedParameterIndexLookupCreator {
        table,
        hash: 0,
        lookup_scope: ParameterLookupScope {
            lookup_name: "lookup".to_string(),
            query_id: index.to_string(),
        },
        output,
        filters: builder.filters.clone(),
        partition_by,
    };
    parameter_index.hash = hash_json(&parameter_index)?;
    parameter_indexes.push(parameter_index);
    parameter_index_signatures.insert(signature, index);
    Ok(index)
}

fn compile_subquery_chain(
    subquery: &SelectQueryAst,
    query_sql: &str,
) -> Result<(Vec<LookupStageBuilder>, SerializedParameterValue), CompilerError> {
    let output_alias = subquery.output_alias.clone();
    let table = subquery
        .tables
        .iter()
        .find(|table| table.alias == output_alias)
        .ok_or_else(|| {
            CompilerError::Unsupported(format!(
                "subquery output alias '{}' is missing from FROM clause in query: {query_sql}",
                output_alias
            ))
        })?;

    if let Some(inner_query) = &table.derived_query {
        if subquery.where_expr.is_none() && subquery.join_equalities.is_empty() {
            let requested_alias = subquery
                .columns
                .first()
                .and_then(|column| match column {
                    SelectItemAst::Expr { expr, .. } => resolve_identifier(expr, subquery),
                    _ => None,
                })
                .map(|column| column.column)
                .ok_or_else(|| {
                    CompilerError::Unsupported(
                        "derived subquery must select a single named column in this compiler subset"
                            .to_string(),
                    )
                })?;
            let derived_column = table
                .derived_columns
                .as_ref()
                .and_then(|columns| {
                    columns.iter().find_map(|column| match column {
                        SelectItemAst::Expr { expr: _, alias }
                            if alias.as_deref() == Some(requested_alias.as_str()) =>
                        {
                            Some(column.clone())
                        }
                        _ => None,
                    })
                })
                .ok_or_else(|| {
                    CompilerError::Unsupported(format!(
                        "column '{requested_alias}' is not available on derived table '{}'",
                        table.alias
                    ))
                })?;

            let mut adjusted_inner = (*inner_query.clone()).clone();
            adjusted_inner.columns = vec![derived_column];
            return compile_subquery_chain(&adjusted_inner, query_sql);
        }
    }

    let output_column = subquery
        .columns
        .first()
        .and_then(|column| match column {
            SelectItemAst::Expr { expr, .. } => resolve_identifier(expr, subquery),
            _ => None,
        })
        .filter(|column| column.table_alias == table.alias)
        .map(|column| column.column)
        .ok_or_else(|| {
            CompilerError::Unsupported(
                "subquery must SELECT a single table column in this compiler subset".to_string(),
            )
        })?;

    let mut filters = Vec::new();
    let mut chain = Vec::new();
    let mut join_equalities = subquery.join_equalities.clone();
    let mut request_seeds: Vec<(QualifiedColumnAst, SqlExpression)> = Vec::new();
    let mut partition_column: Option<String> = None;
    let mut instantiation: Option<SerializedParameterValue> = None;
    let mut conjuncts = Vec::new();

    if let Some(where_expr) = &subquery.where_expr {
        flatten_and_ast(where_expr, &mut conjuncts);
    }

    for conjunct in conjuncts {
        if let Some(eq) = extract_identifier_identifier_equality(&conjunct, subquery) {
            if eq.left.table_alias != eq.right.table_alias {
                join_equalities.push(eq);
                continue;
            }
        }

        if let Some((target, nested_subquery)) = extract_subquery_in_binding(&conjunct, subquery) {
            if target.table_alias != output_alias {
                return Err(CompilerError::Unsupported(format!(
                    "subquery partition target must be on its output table in this compiler subset: {query_sql}"
                )));
            }
            let stage_offset = chain.len();
            let (mut nested_chain, mut nested_value) =
                compile_subquery_chain(&nested_subquery, query_sql)?;
            offset_lookup_builders(&mut nested_chain, stage_offset);
            offset_parameter_value(&mut nested_value, stage_offset);
            chain.extend(nested_chain);
            partition_column = Some(target.column);
            instantiation = Some(nested_value);
            continue;
        }

        if let Some((column, request_expr)) = extract_identifier_request_equality(&conjunct, subquery)? {
            request_seeds.push((column, request_expr));
            continue;
        }

        filters.push(transform_expr(&conjunct)?);
    }

    if partition_column.is_none() && instantiation.is_none() {
        if let Some((seed_column, seed_expr)) = request_seeds.into_iter().next() {
            if seed_column.table_alias == output_alias {
                partition_column = Some(seed_column.column);
                instantiation = Some(SerializedParameterValue::Request { expr: seed_expr });
            } else {
                let stage_offset = chain.len();
                let (mut join_chain, join_partition, mut join_value) = build_join_lookup_chain(
                    subquery,
                    &join_equalities,
                    &output_alias,
                    &seed_column,
                    SerializedParameterValue::Request { expr: seed_expr },
                    query_sql,
                )?;
                offset_join_lookup_builders(&mut join_chain, stage_offset);
                offset_parameter_value(&mut join_value, stage_offset);
                chain.extend(join_chain);
                partition_column = Some(join_partition);
                instantiation = Some(join_value);
            }
        }
    }

    let partition_column = partition_column.ok_or_else(|| {
        CompilerError::Unsupported(format!(
            "subquery must include an equality or IN condition for partitioning in this compiler subset: {query_sql}"
        ))
    })?;
    let instantiation = instantiation.ok_or_else(|| {
        CompilerError::Unsupported(format!(
            "subquery partition source could not be derived in this compiler subset: {query_sql}"
        ))
    })?;

    let stage_id = chain.len();
    chain.push(LookupStageBuilder {
        table_alias: table.alias.clone(),
        table: table.table.clone(),
        schema: table.schema.clone(),
        partition_column,
        output_column,
        instantiation,
        filters,
    });

    Ok((
        chain,
        SerializedParameterValue::Lookup {
            lookup: LookupReference {
                stage_id,
                id_in_stage: 0,
            },
            result_index: 0,
        },
    ))
}

fn build_join_lookup_chain(
    query: &SelectQueryAst,
    join_equalities: &[JoinEqualityAst],
    output_alias: &str,
    seed_column: &QualifiedColumnAst,
    seed_value: SerializedParameterValue,
    query_sql: &str,
) -> Result<(Vec<LookupStageBuilder>, String, SerializedParameterValue), CompilerError> {
    if seed_column.table_alias == output_alias {
        return Ok((Vec::new(), seed_column.column.clone(), seed_value));
    }

    let mut adjacency: HashMap<String, Vec<(String, String, String)>> = HashMap::new();
    for equality in join_equalities {
        adjacency
            .entry(equality.left.table_alias.clone())
            .or_default()
            .push((
                equality.right.table_alias.clone(),
                equality.left.column.clone(),
                equality.right.column.clone(),
            ));
        adjacency
            .entry(equality.right.table_alias.clone())
            .or_default()
            .push((
                equality.left.table_alias.clone(),
                equality.right.column.clone(),
                equality.left.column.clone(),
            ));
    }

    let mut queue = VecDeque::new();
    let mut prev: HashMap<String, (String, String, String)> = HashMap::new();
    queue.push_back(seed_column.table_alias.clone());
    prev.insert(
        seed_column.table_alias.clone(),
        ("".to_string(), "".to_string(), "".to_string()),
    );

    while let Some(current) = queue.pop_front() {
        if current == output_alias {
            break;
        }

        let neighbors = adjacency.get(&current).cloned().unwrap_or_default();
        for (next_table, current_col, next_col) in neighbors {
            if prev.contains_key(&next_table) {
                continue;
            }
            prev.insert(next_table.clone(), (current.clone(), current_col, next_col));
            queue.push_back(next_table);
        }
    }

    if !prev.contains_key(output_alias) {
        return Err(CompilerError::Unsupported(format!(
            "could not derive join path from '{}' to output table '{}' in query: {query_sql}",
            seed_column.table_alias, output_alias
        )));
    }

    let mut steps: Vec<(String, String, String, String)> = Vec::new();
    let mut current_table = output_alias.to_string();
    while current_table != seed_column.table_alias {
        let (previous_table, previous_column, current_column) = prev
            .get(&current_table)
            .cloned()
            .ok_or_else(|| {
                CompilerError::Unsupported(format!(
                    "broken join path while compiling query: {query_sql}"
                ))
            })?;
        steps.push((
            previous_table.clone(),
            current_table.clone(),
            previous_column,
            current_column,
        ));
        current_table = previous_table;
    }
    steps.reverse();

    let table_by_alias = query
        .tables
        .iter()
        .map(|table| (table.alias.clone(), table.clone()))
        .collect::<HashMap<_, _>>();

    let mut chain = Vec::new();
    let mut current_partition_column = seed_column.column.clone();
    let mut current_value = seed_value;
    for (from_table, to_table, from_column, to_column) in steps {
        let table = table_by_alias.get(&from_table).ok_or_else(|| {
            CompilerError::Unsupported(format!(
                "missing table alias '{}' in join chain for query: {query_sql}",
                from_table
            ))
        })?;
        let stage_id = chain.len();
        chain.push(LookupStageBuilder {
            table_alias: table.alias.clone(),
            table: table.table.clone(),
            schema: table.schema.clone(),
            partition_column: current_partition_column.clone(),
            output_column: from_column,
            instantiation: current_value,
            filters: Vec::new(),
        });

        current_value = SerializedParameterValue::Lookup {
            lookup: LookupReference {
                stage_id,
                id_in_stage: 0,
            },
            result_index: 0,
        };

        if to_table == output_alias {
            return Ok((chain, to_column, current_value));
        }

        current_partition_column = to_column;
    }

    Err(CompilerError::Unsupported(format!(
        "invalid join path generated for query: {query_sql}"
    )))
}

fn extract_subquery_in_binding(
    expr: &ExprAst,
    query: &SelectQueryAst,
) -> Option<(QualifiedColumnAst, SelectQueryAst)> {
    match expr {
        ExprAst::SubqueryIn { target, subquery } => {
            let target = resolve_identifier(target, query)?;
            Some((target, (*subquery.clone())))
        }
        _ => None,
    }
}

fn extract_identifier_request_equality(
    expr: &ExprAst,
    query: &SelectQueryAst,
) -> Result<Option<(QualifiedColumnAst, SqlExpression)>, CompilerError> {
    let ExprAst::Binary { left, op, right } = expr else {
        return Ok(None);
    };
    if op != "=" {
        return Ok(None);
    }

    if let Some(column) = resolve_identifier(left, query) {
        let request_expr = transform_expr(right)?;
        if classify_expression(&request_expr) == ReferenceKind::Request {
            return Ok(Some((column, request_expr)));
        }
    }
    if let Some(column) = resolve_identifier(right, query) {
        let request_expr = transform_expr(left)?;
        if classify_expression(&request_expr) == ReferenceKind::Request {
            return Ok(Some((column, request_expr)));
        }
    }

    Ok(None)
}

fn extract_identifier_identifier_equality(
    expr: &ExprAst,
    query: &SelectQueryAst,
) -> Option<JoinEqualityAst> {
    let ExprAst::Binary { left, op, right } = expr else {
        return None;
    };
    if op != "=" {
        return None;
    }
    let left = resolve_identifier(left, query)?;
    let right = resolve_identifier(right, query)?;
    Some(JoinEqualityAst { left, right })
}

fn resolve_identifier(expr: &ExprAst, query: &SelectQueryAst) -> Option<QualifiedColumnAst> {
    let ExprAst::Identifier(identifier) = expr else {
        return None;
    };

    if let Some((table_alias, column)) = identifier.rsplit_once('.') {
        return Some(QualifiedColumnAst {
            table_alias: table_alias.to_string(),
            column: column.to_string(),
        });
    }

    if query.tables.len() == 1 {
        return Some(QualifiedColumnAst {
            table_alias: query.tables[0].alias.clone(),
            column: identifier.clone(),
        });
    }

    None
}

fn referenced_aliases(expr: &ExprAst, query: &SelectQueryAst) -> Vec<String> {
    let mut aliases = Vec::new();
    collect_referenced_aliases(expr, query, &mut aliases);
    aliases.sort();
    aliases.dedup();
    aliases
}

fn collect_referenced_aliases(expr: &ExprAst, query: &SelectQueryAst, aliases: &mut Vec<String>) {
    if let Some(identifier) = resolve_identifier(expr, query) {
        aliases.push(identifier.table_alias);
        return;
    }

    match expr {
        ExprAst::Unary { operand, .. } => collect_referenced_aliases(operand, query, aliases),
        ExprAst::Binary { left, right, .. } => {
            collect_referenced_aliases(left, query, aliases);
            collect_referenced_aliases(right, query, aliases);
        }
        ExprAst::ScalarIn { target, values } => {
            collect_referenced_aliases(target, query, aliases);
            for value in values {
                collect_referenced_aliases(value, query, aliases);
            }
        }
        ExprAst::Between { value, low, high } => {
            collect_referenced_aliases(value, query, aliases);
            collect_referenced_aliases(low, query, aliases);
            collect_referenced_aliases(high, query, aliases);
        }
        ExprAst::Cast { expr, .. } => collect_referenced_aliases(expr, query, aliases),
        ExprAst::CaseWhen {
            operand,
            whens,
            else_expr,
        } => {
            if let Some(operand) = operand {
                collect_referenced_aliases(operand, query, aliases);
            }
            for (when, then) in whens {
                collect_referenced_aliases(when, query, aliases);
                collect_referenced_aliases(then, query, aliases);
            }
            if let Some(else_expr) = else_expr {
                collect_referenced_aliases(else_expr, query, aliases);
            }
        }
        ExprAst::Function { args, .. } => {
            for arg in args {
                collect_referenced_aliases(arg, query, aliases);
            }
        }
        ExprAst::SubqueryIn { target, subquery } => {
            collect_referenced_aliases(target, query, aliases);
            if let Some(where_expr) = &subquery.where_expr {
                collect_referenced_aliases(where_expr, subquery, aliases);
            }
        }
        _ => {}
    }
}

fn flatten_and_ast(expr: &ExprAst, out: &mut Vec<ExprAst>) {
    if let ExprAst::Binary { left, op, right } = expr {
        if op.eq_ignore_ascii_case("and") {
            flatten_and_ast(left, out);
            flatten_and_ast(right, out);
            return;
        }
    }

    out.push(expr.clone());
}

fn expr_contains_subquery(expr: &ExprAst) -> bool {
    match expr {
        ExprAst::SubqueryIn { .. } => true,
        ExprAst::Unary { operand, .. } => expr_contains_subquery(operand),
        ExprAst::Binary { left, right, .. } => {
            expr_contains_subquery(left) || expr_contains_subquery(right)
        }
        ExprAst::ScalarIn { target, values } => {
            expr_contains_subquery(target) || values.iter().any(expr_contains_subquery)
        }
        ExprAst::Between { value, low, high } => {
            expr_contains_subquery(value)
                || expr_contains_subquery(low)
                || expr_contains_subquery(high)
        }
        ExprAst::Cast { expr, .. } => expr_contains_subquery(expr),
        ExprAst::CaseWhen {
            operand,
            whens,
            else_expr,
        } => {
            operand
                .as_ref()
                .map(|value| expr_contains_subquery(value))
                .unwrap_or(false)
                || whens.iter().any(|(when, then)| {
                    expr_contains_subquery(when) || expr_contains_subquery(then)
                })
                || else_expr
                    .as_ref()
                    .map(|value| expr_contains_subquery(value))
                    .unwrap_or(false)
        }
        ExprAst::Function { args, .. } => args.iter().any(expr_contains_subquery),
        _ => false,
    }
}

fn column_expression(column: &str) -> SqlExpression {
    SqlExpression::Data {
        source: ExternalSource::Column {
            column: column.to_string(),
        },
    }
}

fn offset_lookup_builders(builders: &mut [LookupStageBuilder], offset: usize) {
    for builder in builders {
        offset_parameter_value(&mut builder.instantiation, offset);
    }
}

fn offset_join_lookup_builders(builders: &mut [LookupStageBuilder], offset: usize) {
    for builder in builders.iter_mut().skip(1) {
        offset_parameter_value(&mut builder.instantiation, offset);
    }
}

fn offset_parameter_value(value: &mut SerializedParameterValue, offset: usize) {
    match value {
        SerializedParameterValue::Request { .. } => {}
        SerializedParameterValue::Lookup { lookup, .. } => {
            lookup.stage_id += offset;
        }
        SerializedParameterValue::Intersection { values } => {
            for value in values {
                offset_parameter_value(value, offset);
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReferenceKind {
    Static,
    Row,
    Request,
    Mixed,
}

fn classify_expression(expr: &SqlExpression) -> ReferenceKind {
    match expr {
        SqlExpression::Data { source } => match source {
            ExternalSource::Column { .. } => ReferenceKind::Row,
            ExternalSource::Request { .. } => ReferenceKind::Request,
            ExternalSource::Other(_) => ReferenceKind::Mixed,
        },
        SqlExpression::LitNull
        | SqlExpression::LitDouble { .. }
        | SqlExpression::LitInt { .. }
        | SqlExpression::LitString { .. } => ReferenceKind::Static,
        SqlExpression::Unary { operand, .. } => classify_expression(operand),
        SqlExpression::Binary { left, right, .. } => {
            combine_kinds(classify_expression(left), classify_expression(right))
        }
        SqlExpression::Between { value, low, high } => combine_kinds(
            combine_kinds(classify_expression(value), classify_expression(low)),
            classify_expression(high),
        ),
        SqlExpression::ScalarIn { target, in_values } => {
            let mut combined = classify_expression(target);
            for value in in_values {
                combined = combine_kinds(combined, classify_expression(value));
            }
            combined
        }
        SqlExpression::CaseWhen {
            operand,
            whens,
            else_expr,
        } => {
            let mut combined = operand
                .as_ref()
                .map(|e| classify_expression(e))
                .unwrap_or(ReferenceKind::Static);
            for branch in whens {
                combined = combine_kinds(combined, classify_expression(&branch.when));
                combined = combine_kinds(combined, classify_expression(&branch.then));
            }
            if let Some(else_expr) = else_expr {
                combined = combine_kinds(combined, classify_expression(else_expr));
            }
            combined
        }
        SqlExpression::Cast { operand, .. } => classify_expression(operand),
        SqlExpression::Function { parameters, .. } => {
            let mut combined = ReferenceKind::Static;
            for parameter in parameters {
                combined = combine_kinds(combined, classify_expression(parameter));
            }
            combined
        }
    }
}

fn combine_kinds(a: ReferenceKind, b: ReferenceKind) -> ReferenceKind {
    use ReferenceKind::*;

    match (a, b) {
        (Mixed, _) | (_, Mixed) => Mixed,
        (Static, other) => other,
        (other, Static) => other,
        (Row, Row) => Row,
        (Request, Request) => Request,
        (Row, Request) | (Request, Row) => Mixed,
    }
}

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

    #[test]
    fn compiles_basic_query() {
        let yaml = r#"
config:
  edition: 2
  sync_config_compiler: true
streams:
  stream:
    query: SELECT * FROM users
"#;

        let compiled = compile_sync_plan(yaml, CompileOptions::default()).unwrap();
        assert_eq!(compiled.plan.streams.len(), 1);
        assert_eq!(compiled.plan.data_sources.len(), 1);
        assert_eq!(compiled.plan.buckets[0].unique_name, "stream|0");
        assert!(compiled.plan.parameter_indexes.is_empty());
    }

    #[test]
    fn extracts_request_partition_from_where_clause() {
        let yaml = r#"
config:
  edition: 2
  sync_config_compiler: true
streams:
  stream:
    query: SELECT * FROM users WHERE value = subscription.parameter('p')
"#;

        let compiled = compile_sync_plan(yaml, CompileOptions::default()).unwrap();
        let source = &compiled.plan.data_sources[0];
        assert_eq!(source.partition_by.len(), 1);
        assert!(source.filters.is_empty());

        let querier = &compiled.plan.streams[0].queriers[0];
        assert_eq!(querier.source_instantiation.len(), 1);
        assert!(querier.request_filters.is_empty());
    }

    #[test]
    fn reuses_bucket_for_multiple_queries_in_stream() {
        let yaml = r#"
config:
  edition: 2
  sync_config_compiler: true
streams:
  stream:
    queries:
      - SELECT * FROM users
      - SELECT * FROM comments
"#;

        let compiled = compile_sync_plan(yaml, CompileOptions::default()).unwrap();
        assert_eq!(compiled.plan.buckets.len(), 1);
        assert_eq!(compiled.plan.buckets[0].sources.len(), 2);
        assert_eq!(compiled.plan.streams[0].queriers.len(), 1);
        assert_eq!(compiled.plan.streams[0].queriers[0].bucket, 0);
    }
}
