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
