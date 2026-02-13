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
