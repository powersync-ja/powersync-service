use serde_json::{Map, Number, Value};
use std::cmp::Ordering;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum NumericValue {
    Int(i64),
    Float(f64),
}

pub fn sqlite_bool(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(v) => *v,
        Value::Number(v) => {
            if let Some(i) = v.as_i64() {
                i != 0
            } else if let Some(u) = v.as_u64() {
                u != 0
            } else if let Some(f) = v.as_f64() {
                f != 0.0
            } else {
                false
            }
        }
        Value::String(v) => v.parse::<i64>().map(|i| i != 0).unwrap_or(false),
        Value::Array(_) | Value::Object(_) => false,
    }
}

pub fn sqlite_not(value: &Value) -> Value {
    if sqlite_bool(value) {
        Value::Number(Number::from(0))
    } else {
        Value::Number(Number::from(1))
    }
}

pub fn compare_sql_values(left: &Value, right: &Value) -> Option<Ordering> {
    match (left, right) {
        (Value::Null, _) | (_, Value::Null) => None,
        _ => {
            let left_rank = sqlite_type_rank(left);
            let right_rank = sqlite_type_rank(right);

            if left_rank != right_rank {
                return Some(left_rank.cmp(&right_rank));
            }

            match left_rank {
                1 => compare_numeric(cast_numeric(left)?, cast_numeric(right)?),
                2 => Some(left.as_str()?.cmp(right.as_str()?)),
                // Blob/blob comparisons are currently unsupported in JS parity code as well.
                3 => None,
                _ => None,
            }
        }
    }
}

pub fn normalize_parameter_value(value: &Value) -> Option<Value> {
    match value {
        Value::Null => None,
        Value::Bool(v) => Some(Value::Number(Number::from(if *v { 1 } else { 0 }))),
        Value::String(v) => Some(Value::String(v.clone())),
        Value::Number(v) => {
            if let Some(i) = v.as_i64() {
                Some(Value::Number(Number::from(i)))
            } else if let Some(u) = v.as_u64() {
                if let Ok(i) = i64::try_from(u) {
                    Some(Value::Number(Number::from(i)))
                } else {
                    let f = u as f64;
                    Number::from_f64(f).map(Value::Number)
                }
            } else {
                Number::from_f64(v.as_f64()?).map(Value::Number)
            }
        }
        _ => None,
    }
}

pub fn cast_as_text(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::Bool(v) => Some(if *v { "1".to_string() } else { "0".to_string() }),
        Value::String(v) => Some(v.clone()),
        Value::Number(v) => Some(v.to_string()),
        Value::Array(_) | Value::Object(_) => Some(value.to_string()),
    }
}

pub fn cast_as_f64(value: &Value) -> Option<f64> {
    numeric_value(value)
}

pub fn numeric_value(value: &Value) -> Option<f64> {
    match cast_numeric(value)? {
        NumericValue::Int(value) => Some(value as f64),
        NumericValue::Float(value) => Some(value),
    }
}

pub fn cast_numeric(value: &Value) -> Option<NumericValue> {
    match value {
        Value::Number(v) => number_to_numeric(v),
        Value::Bool(v) => Some(NumericValue::Int(if *v { 1 } else { 0 })),
        Value::String(v) => parse_numeric_string(v),
        _ => None,
    }
}

pub fn cast_as_i64(value: &Value) -> Option<i64> {
    match cast_numeric(value)? {
        NumericValue::Int(value) => Some(value),
        NumericValue::Float(value) => {
            if !value.is_finite() || value < i64::MIN as f64 || value > i64::MAX as f64 {
                None
            } else {
                Some(value.trunc() as i64)
            }
        }
    }
}

fn number_to_numeric(value: &Number) -> Option<NumericValue> {
    if let Some(i) = value.as_i64() {
        Some(NumericValue::Int(i))
    } else if let Some(u) = value.as_u64() {
        if let Ok(i) = i64::try_from(u) {
            Some(NumericValue::Int(i))
        } else {
            Some(NumericValue::Float(u as f64))
        }
    } else {
        value.as_f64().map(NumericValue::Float)
    }
}

fn parse_numeric_string(value: &str) -> Option<NumericValue> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }

    if let Ok(i) = trimmed.parse::<i64>() {
        return Some(NumericValue::Int(i));
    }

    trimmed.parse::<f64>().ok().map(NumericValue::Float)
}

fn compare_numeric(left: NumericValue, right: NumericValue) -> Option<Ordering> {
    match (left, right) {
        (NumericValue::Int(a), NumericValue::Int(b)) => Some(a.cmp(&b)),
        (NumericValue::Int(a), NumericValue::Float(b)) => (a as f64).partial_cmp(&b),
        (NumericValue::Float(a), NumericValue::Int(b)) => a.partial_cmp(&(b as f64)),
        (NumericValue::Float(a), NumericValue::Float(b)) => a.partial_cmp(&b),
    }
}

fn sqlite_type_rank(value: &Value) -> u8 {
    match value {
        Value::Null => 0,
        Value::Number(_) | Value::Bool(_) => 1,
        Value::String(_) => 2,
        Value::Array(_) | Value::Object(_) => 3,
    }
}

pub fn filter_json_row(source: &Map<String, Value>) -> Map<String, Value> {
    let mut out = Map::new();
    for (k, v) in source {
        if matches!(v, Value::Null | Value::String(_) | Value::Number(_)) {
            out.insert(k.clone(), v.clone());
        }
    }
    out
}

pub fn id_from_data(source: &Map<String, Value>) -> String {
    match source.get("id") {
        Some(Value::String(v)) => v.clone(),
        Some(other) => cast_as_text(other).unwrap_or_default(),
        None => String::new(),
    }
}

pub fn serialize_value_array(values: &[Value]) -> String {
    let mut out = String::new();
    out.push('[');

    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }

        out.push_str(&serialize_json_scalar(value));
    }

    out.push(']');
    out
}

fn serialize_json_scalar(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(v) => {
            if *v {
                "1".to_string()
            } else {
                "0".to_string()
            }
        }
        Value::Number(v) => {
            if let Some(i) = v.as_i64() {
                i.to_string()
            } else if let Some(u) = v.as_u64() {
                if let Ok(i) = i64::try_from(u) {
                    i.to_string()
                } else {
                    (u as f64).to_string()
                }
            } else if let Some(f) = v.as_f64() {
                if f.is_finite()
                    && (f.fract() == 0.0)
                    && f >= i64::MIN as f64
                    && f <= i64::MAX as f64
                {
                    (f as i64).to_string()
                } else {
                    let mut text = f.to_string();
                    if text.ends_with(".0") {
                        text.truncate(text.len() - 2);
                    }
                    text
                }
            } else {
                "0".to_string()
            }
        }
        Value::String(v) => serde_json::to_string(v).unwrap_or_else(|_| "\"\"".to_string()),
        Value::Array(_) | Value::Object(_) => {
            serde_json::to_string(value).unwrap_or_else(|_| "null".to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cmp::Ordering;

    #[test]
    fn serializes_integerish_floats_like_integers() {
        let values = vec![
            Number::from(1).into(),
            Number::from_f64(1.0).unwrap().into(),
            Number::from_f64(1.5).unwrap().into(),
            Value::String("x".to_string()),
        ];

        assert_eq!(serialize_value_array(&values), r#"[1,1,1.5,"x"]"#);
    }

    #[test]
    fn compares_large_integers_without_f64_rounding() {
        let a = Value::Number(Number::from(9_007_199_254_740_993_i64));
        let b = Value::Number(Number::from(9_007_199_254_740_992_i64));

        assert_eq!(compare_sql_values(&a, &b), Some(Ordering::Greater));
    }

    #[test]
    fn compares_text_and_numeric_by_sqlite_type_order() {
        assert_eq!(
            compare_sql_values(&Value::String("1".to_string()), &Value::Number(Number::from(1))),
            Some(Ordering::Greater)
        );
        assert_eq!(
            compare_sql_values(&Value::Number(Number::from(1)), &Value::String("1".to_string())),
            Some(Ordering::Less)
        );
    }
}
