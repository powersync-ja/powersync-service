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
