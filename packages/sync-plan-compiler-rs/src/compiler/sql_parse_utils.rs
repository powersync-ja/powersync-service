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
