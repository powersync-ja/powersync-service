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
