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
