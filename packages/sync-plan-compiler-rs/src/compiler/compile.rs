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
