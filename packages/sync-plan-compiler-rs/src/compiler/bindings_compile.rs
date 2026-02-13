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
