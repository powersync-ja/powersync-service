use crate::evaluator::{EvalContext, EvaluatorError, EvaluatorResult};
use crate::model::{CaseWhenBranch, ExternalSource, SqlExpression};
use crate::value::{
    cast_as_f64, cast_as_i64, cast_as_text, cast_numeric, compare_sql_values, sqlite_bool,
    sqlite_not, NumericValue,
};
use serde_json::{Number, Value};
use std::cmp::Ordering;

pub fn evaluate_expression(
    expr: &SqlExpression,
    context: &EvalContext<'_>,
) -> EvaluatorResult<Value> {
    match expr {
        SqlExpression::Data { source } => match source {
            ExternalSource::Column { column } => Ok(context
                .row
                .and_then(|row| row.get(column).cloned())
                .unwrap_or(Value::Null)),
            ExternalSource::Request { request } => Ok(context.request_source(request)),
            ExternalSource::Other(v) => Ok(v.clone()),
        },
        SqlExpression::Unary { operand, operator } => {
            let value = evaluate_expression(operand, context)?;
            match operator.as_str() {
                "not" | "NOT" => Ok(sqlite_not(&value)),
                "+" => Ok(value),
                _ => Err(EvaluatorError::UnsupportedExpression(format!(
                    "unsupported unary operator: {operator}"
                ))),
            }
        }
        SqlExpression::Binary {
            left,
            operator,
            right,
        } => {
            let left_value = evaluate_expression(left, context)?;
            let right_value = evaluate_expression(right, context)?;
            evaluate_binary(operator, &left_value, &right_value)
        }
        SqlExpression::Between { value, low, high } => {
            let value = evaluate_expression(value, context)?;
            let low = evaluate_expression(low, context)?;
            let high = evaluate_expression(high, context)?;

            let geq = evaluate_binary(">=", &value, &low)?;
            let leq = evaluate_binary("<=", &value, &high)?;

            if geq.is_null() || leq.is_null() {
                Ok(Value::Null)
            } else if sqlite_bool(&geq) && sqlite_bool(&leq) {
                Ok(Value::Number(Number::from(1)))
            } else {
                Ok(Value::Number(Number::from(0)))
            }
        }
        SqlExpression::ScalarIn { target, in_values } => {
            let target = evaluate_expression(target, context)?;
            if target.is_null() {
                return Ok(Value::Null);
            }

            let mut has_null = false;
            for candidate in in_values {
                let candidate = evaluate_expression(candidate, context)?;
                if candidate.is_null() {
                    has_null = true;
                    continue;
                }

                if compare_sql_values(&target, &candidate) == Some(Ordering::Equal) {
                    return Ok(Value::Number(Number::from(1)));
                }
            }

            if has_null {
                Ok(Value::Null)
            } else {
                Ok(Value::Number(Number::from(0)))
            }
        }
        SqlExpression::CaseWhen {
            operand,
            whens,
            else_expr,
        } => {
            if let Some(operand) = operand {
                let operand_value = evaluate_expression(operand, context)?;
                evaluate_case_operand(whens, else_expr.as_deref(), &operand_value, context)
            } else {
                evaluate_case_condition(whens, else_expr.as_deref(), context)
            }
        }
        SqlExpression::Cast { operand, cast_as } => {
            let value = evaluate_expression(operand, context)?;
            evaluate_cast(value, cast_as)
        }
        SqlExpression::Function {
            function,
            parameters,
        } => {
            let mut args = Vec::with_capacity(parameters.len());
            for parameter in parameters {
                args.push(evaluate_expression(parameter, context)?);
            }
            evaluate_function(function, &args)
        }
        SqlExpression::LitNull => Ok(Value::Null),
        SqlExpression::LitDouble { value } => {
            let number = Number::from_f64(*value).ok_or_else(|| {
                EvaluatorError::InvalidLiteral(format!("invalid floating point literal: {value}"))
            })?;
            Ok(Value::Number(number))
        }
        SqlExpression::LitInt { base10 } => {
            let parsed = base10.parse::<i64>().map_err(|_| {
                EvaluatorError::InvalidLiteral(format!("invalid integer literal: {base10}"))
            })?;
            Ok(Value::Number(Number::from(parsed)))
        }
        SqlExpression::LitString { value } => Ok(Value::String(value.clone())),
    }
}

fn evaluate_case_operand(
    whens: &[CaseWhenBranch],
    else_expr: Option<&SqlExpression>,
    operand_value: &Value,
    context: &EvalContext<'_>,
) -> EvaluatorResult<Value> {
    for branch in whens {
        let when_value = evaluate_expression(&branch.when, context)?;
        if compare_sql_values(operand_value, &when_value) == Some(Ordering::Equal) {
            return evaluate_expression(&branch.then, context);
        }
    }

    if let Some(else_expr) = else_expr {
        evaluate_expression(else_expr, context)
    } else {
        Ok(Value::Null)
    }
}

fn evaluate_case_condition(
    whens: &[CaseWhenBranch],
    else_expr: Option<&SqlExpression>,
    context: &EvalContext<'_>,
) -> EvaluatorResult<Value> {
    for branch in whens {
        let when_value = evaluate_expression(&branch.when, context)?;
        if sqlite_bool(&when_value) {
            return evaluate_expression(&branch.then, context);
        }
    }

    if let Some(else_expr) = else_expr {
        evaluate_expression(else_expr, context)
    } else {
        Ok(Value::Null)
    }
}

fn evaluate_cast(value: Value, cast_as: &str) -> EvaluatorResult<Value> {
    match cast_as.to_ascii_lowercase().as_str() {
        "text" => Ok(cast_as_text(&value)
            .map(Value::String)
            .unwrap_or(Value::Null)),
        "integer" => {
            let value = cast_as_f64(&value).map(|v| v as i64);
            Ok(value
                .map(Number::from)
                .map(Value::Number)
                .unwrap_or(Value::Null))
        }
        "real" | "numeric" => {
            let value = cast_as_f64(&value).and_then(Number::from_f64);
            Ok(value.map(Value::Number).unwrap_or(Value::Null))
        }
        "blob" => Ok(Value::Null),
        other => Err(EvaluatorError::UnsupportedExpression(format!(
            "unsupported cast target: {other}"
        ))),
    }
}

fn evaluate_function(function: &str, args: &[Value]) -> EvaluatorResult<Value> {
    match function {
        "->>" => extract_json_path(args, true),
        "->" => extract_json_path(args, false),
        "length" => {
            if args.len() != 1 {
                return Err(EvaluatorError::UnsupportedExpression(
                    "length() expects one argument".to_string(),
                ));
            }

            if let Some(value) = cast_as_text(&args[0]) {
                Ok(Value::Number(Number::from(value.len() as i64)))
            } else {
                Ok(Value::Null)
            }
        }
        "lower" => {
            let value = args.get(0).and_then(cast_as_text);
            Ok(value
                .map(|v| Value::String(v.to_lowercase()))
                .unwrap_or(Value::Null))
        }
        "upper" => {
            let value = args.get(0).and_then(cast_as_text);
            Ok(value
                .map(|v| Value::String(v.to_uppercase()))
                .unwrap_or(Value::Null))
        }
        "ifnull" => {
            if args.len() != 2 {
                return Err(EvaluatorError::UnsupportedExpression(
                    "ifnull() expects two arguments".to_string(),
                ));
            }
            if args[0].is_null() {
                Ok(args[1].clone())
            } else {
                Ok(args[0].clone())
            }
        }
        "coalesce" => {
            for value in args {
                if !value.is_null() {
                    return Ok(value.clone());
                }
            }
            Ok(Value::Null)
        }
        "substr" | "substring" => substr(args),
        unsupported => Err(EvaluatorError::UnsupportedExpression(format!(
            "unsupported function: {unsupported}"
        ))),
    }
}

fn substr(args: &[Value]) -> EvaluatorResult<Value> {
    if args.len() < 2 || args.len() > 3 {
        return Err(EvaluatorError::UnsupportedExpression(
            "substr()/substring() expects two or three arguments".to_string(),
        ));
    }

    let text = match cast_as_text(&args[0]) {
        Some(value) => value,
        None => return Ok(Value::Null),
    };

    let start = match cast_as_f64(&args[1]) {
        Some(value) => value as i64,
        None => return Ok(Value::Null),
    };

    let length = if args.len() == 3 {
        cast_as_f64(&args[2]).map(|v| v as i64)
    } else {
        None
    };

    let start_index = if start > 0 { (start - 1) as usize } else { 0 };
    let mut chars = text.chars().skip(start_index);

    if let Some(length) = length {
        if length < 0 {
            return Ok(Value::String(String::new()));
        }
        let content: String = chars.by_ref().take(length as usize).collect();
        Ok(Value::String(content))
    } else {
        Ok(Value::String(chars.collect()))
    }
}

fn extract_json_path(args: &[Value], scalar: bool) -> EvaluatorResult<Value> {
    if args.len() != 2 {
        return Err(EvaluatorError::UnsupportedExpression(
            "json extraction expects two arguments".to_string(),
        ));
    }

    let source = &args[0];
    let path = match args[1].as_str() {
        Some(value) => value,
        None => return Ok(Value::Null),
    };

    let mut source_value = source.clone();
    if matches!(source_value, Value::String(_)) {
        if let Some(text) = source_value.as_str() {
            if let Ok(parsed) = serde_json::from_str::<Value>(text) {
                source_value = parsed;
            }
        }
    }

    let extracted = extract_json_path_inner(&source_value, path);
    if scalar {
        Ok(to_sqlite_scalar(extracted))
    } else {
        Ok(extracted.cloned().unwrap_or(Value::Null))
    }
}

fn extract_json_path_inner<'a>(source: &'a Value, path: &str) -> Option<&'a Value> {
    if let Some(rest) = path.strip_prefix("$.") {
        let mut current = source;
        for segment in rest.split('.') {
            current = current.get(segment)?;
        }
        Some(current)
    } else {
        source.get(path)
    }
}

fn to_sqlite_scalar(value: Option<&Value>) -> Value {
    match value {
        None => Value::Null,
        Some(Value::Null) => Value::Null,
        Some(Value::Bool(v)) => Value::Number(Number::from(if *v { 1 } else { 0 })),
        Some(Value::String(v)) => Value::String(v.clone()),
        Some(Value::Number(v)) => Value::Number(v.clone()),
        Some(Value::Array(_) | Value::Object(_)) => Value::String(value.unwrap().to_string()),
    }
}

fn evaluate_binary(operator: &str, left: &Value, right: &Value) -> EvaluatorResult<Value> {
    let op = operator.to_ascii_lowercase();

    match op.as_str() {
        "or" => Ok(Value::Number(Number::from(
            (sqlite_bool(left) || sqlite_bool(right)) as i64,
        ))),
        "and" => Ok(Value::Number(Number::from(
            (sqlite_bool(left) && sqlite_bool(right)) as i64,
        ))),
        "=" => Ok(compare_result(
            compare_sql_values(left, right),
            Ordering::Equal,
        )),
        "!=" | "<>" => Ok(compare_ordering(compare_sql_values(left, right), |o| {
            o != Ordering::Equal
        })),
        "is" => Ok(Value::Number(Number::from((left == right) as i64))),
        "is not" => Ok(Value::Number(Number::from((left != right) as i64))),
        "<" => Ok(compare_result(
            compare_sql_values(left, right),
            Ordering::Less,
        )),
        "<=" => Ok(compare_ordering(compare_sql_values(left, right), |o| {
            o != Ordering::Greater
        })),
        ">" => Ok(compare_result(
            compare_sql_values(left, right),
            Ordering::Greater,
        )),
        ">=" => Ok(compare_ordering(compare_sql_values(left, right), |o| {
            o != Ordering::Less
        })),
        "+" => numeric_add(left, right),
        "-" => numeric_sub(left, right),
        "*" => numeric_mul(left, right),
        "/" => numeric_div(left, right),
        "%" => numeric_mod(left, right),
        "||" => {
            let left = cast_as_text(left);
            let right = cast_as_text(right);

            if let (Some(left), Some(right)) = (left, right) {
                Ok(Value::String(format!("{left}{right}")))
            } else {
                Ok(Value::Null)
            }
        }
        _ => Err(EvaluatorError::UnsupportedExpression(format!(
            "unsupported binary operator: {operator}"
        ))),
    }
}

fn numeric_add(left: &Value, right: &Value) -> EvaluatorResult<Value> {
    numeric_binary(left, right, i64::checked_add, |a, b| a + b)
}

fn numeric_sub(left: &Value, right: &Value) -> EvaluatorResult<Value> {
    numeric_binary(left, right, i64::checked_sub, |a, b| a - b)
}

fn numeric_mul(left: &Value, right: &Value) -> EvaluatorResult<Value> {
    numeric_binary(left, right, i64::checked_mul, |a, b| a * b)
}

fn numeric_div(left: &Value, right: &Value) -> EvaluatorResult<Value> {
    match (cast_numeric(left), cast_numeric(right)) {
        (Some(NumericValue::Int(_)), Some(NumericValue::Int(0))) => Ok(Value::Null),
        (Some(NumericValue::Int(a)), Some(NumericValue::Int(b))) => Ok(Value::Number(Number::from(a / b))),
        _ => {
            let left = match cast_as_f64(left) {
                Some(value) => value,
                None => return Ok(Value::Null),
            };
            let right = match cast_as_f64(right) {
                Some(value) => value,
                None => return Ok(Value::Null),
            };
            if right == 0.0 {
                return Ok(Value::Null);
            }
            number_from_f64(left / right)
        }
    }
}

fn numeric_mod(left: &Value, right: &Value) -> EvaluatorResult<Value> {
    let left_int = match cast_as_i64(left) {
        Some(value) => value,
        None => return Ok(Value::Null),
    };
    let right_int = match cast_as_i64(right) {
        Some(value) => value,
        None => return Ok(Value::Null),
    };

    if right_int == 0 {
        return Ok(Value::Null);
    }

    let result = left_int % right_int;
    match (cast_numeric(left), cast_numeric(right)) {
        (Some(NumericValue::Int(_)), Some(NumericValue::Int(_))) => {
            Ok(Value::Number(Number::from(result)))
        }
        _ => number_from_f64(result as f64),
    }
}

fn numeric_binary(
    left: &Value,
    right: &Value,
    int_op: fn(i64, i64) -> Option<i64>,
    float_op: fn(f64, f64) -> f64,
) -> EvaluatorResult<Value> {
    match (cast_numeric(left), cast_numeric(right)) {
        (Some(NumericValue::Int(a)), Some(NumericValue::Int(b))) => {
            if let Some(value) = int_op(a, b) {
                return Ok(Value::Number(Number::from(value)));
            }
            number_from_f64(float_op(a as f64, b as f64))
        }
        (Some(_), Some(_)) => {
            let left = match cast_as_f64(left) {
                Some(value) => value,
                None => return Ok(Value::Null),
            };
            let right = match cast_as_f64(right) {
                Some(value) => value,
                None => return Ok(Value::Null),
            };
            number_from_f64(float_op(left, right))
        }
        _ => Ok(Value::Null),
    }
}

fn number_from_f64(value: f64) -> EvaluatorResult<Value> {
    if value.is_finite()
        && value.fract() == 0.0
        && value >= i64::MIN as f64
        && value <= i64::MAX as f64
    {
        Ok(Value::Number(Number::from(value as i64)))
    } else {
        let number = Number::from_f64(value).ok_or_else(|| {
            EvaluatorError::UnsupportedExpression("non-finite number".to_string())
        })?;
        Ok(Value::Number(number))
    }
}

fn compare_result(comparison: Option<Ordering>, expected: Ordering) -> Value {
    match comparison {
        None => Value::Null,
        Some(value) => Value::Number(Number::from((value == expected) as i64)),
    }
}

fn compare_ordering(comparison: Option<Ordering>, predicate: fn(Ordering) -> bool) -> Value {
    match comparison {
        None => Value::Null,
        Some(value) => Value::Number(Number::from(predicate(value) as i64)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::RequestParameters;

    fn eval(expr: &SqlExpression) -> Value {
        evaluate_expression(
            expr,
            &EvalContext {
                row: None,
                request: &RequestParameters::default(),
            },
        )
        .unwrap()
    }

    fn int(value: i64) -> SqlExpression {
        SqlExpression::LitInt {
            base10: value.to_string(),
        }
    }

    fn double(value: f64) -> SqlExpression {
        SqlExpression::LitDouble { value }
    }

    #[test]
    fn integer_division_uses_integer_semantics() {
        let expr = SqlExpression::Binary {
            left: Box::new(int(7)),
            operator: "/".to_string(),
            right: Box::new(int(2)),
        };

        assert_eq!(eval(&expr), Value::Number(Number::from(3)));
    }

    #[test]
    fn real_division_keeps_fractional_result() {
        let expr = SqlExpression::Binary {
            left: Box::new(double(7.0)),
            operator: "/".to_string(),
            right: Box::new(int(2)),
        };

        assert_eq!(eval(&expr), Value::Number(Number::from_f64(3.5).unwrap()));
    }

    #[test]
    fn modulo_with_integer_inputs_stays_integer() {
        let expr = SqlExpression::Binary {
            left: Box::new(int(7)),
            operator: "%".to_string(),
            right: Box::new(int(3)),
        };

        assert_eq!(eval(&expr), Value::Number(Number::from(1)));
    }

    #[test]
    fn division_by_zero_returns_null() {
        let expr = SqlExpression::Binary {
            left: Box::new(int(7)),
            operator: "/".to_string(),
            right: Box::new(int(0)),
        };

        assert_eq!(eval(&expr), Value::Null);
    }

    #[test]
    fn text_and_numeric_equality_follow_sqlite_type_order() {
        let expr = SqlExpression::Binary {
            left: Box::new(SqlExpression::LitString {
                value: "1".to_string(),
            }),
            operator: "=".to_string(),
            right: Box::new(int(1)),
        };

        assert_eq!(eval(&expr), Value::Number(Number::from(0)));
    }

    #[test]
    fn not_equal_operator_is_supported() {
        let expr = SqlExpression::Binary {
            left: Box::new(SqlExpression::LitString {
                value: "1".to_string(),
            }),
            operator: "!=".to_string(),
            right: Box::new(int(1)),
        };

        assert_eq!(eval(&expr), Value::Number(Number::from(1)));
    }
}
