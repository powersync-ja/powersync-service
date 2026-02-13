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
