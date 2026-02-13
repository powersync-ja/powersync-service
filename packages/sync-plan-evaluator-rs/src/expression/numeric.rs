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
