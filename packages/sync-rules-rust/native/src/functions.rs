//! PowerSync overrides of SQLite builtins. No JavaScript callbacks are registered.
use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, Utc};
use geozero::{wkb::Ewkb, ToJson, ToWkt};
use rusqlite::{
    functions::{Context, FunctionFlags},
    types::Value,
    Connection,
};

fn error(message: impl Into<String>) -> rusqlite::Error {
    rusqlite::Error::UserFunctionError(std::io::Error::other(message.into()).into())
}

pub fn js_number(value: f64) -> String {
    ryu_js::Buffer::new().format(value).to_owned()
}

pub fn text(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::Integer(v) => Some(v.to_string()),
        Value::Real(v) => Some(js_number(*v)),
        Value::Text(v) => Some(v.clone()),
        Value::Blob(v) => Some(String::from_utf8_lossy(v).into_owned()),
    }
}

fn arg(context: &Context<'_>, index: usize) -> rusqlite::Result<Value> {
    if index >= context.len() {
        Ok(Value::Null)
    } else {
        context.get(index)
    }
}

pub fn register(db: &Connection) -> rusqlite::Result<()> {
    let flags = FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC;
    for name in ["upper", "lower"] {
        db.create_scalar_function(name, -1, flags, move |ctx| {
            Ok(text(&arg(ctx, 0)?).map(|v| {
                if name == "upper" {
                    v.to_uppercase()
                } else {
                    v.to_lowercase()
                }
            }))
        })?;
    }
    for name in ["unixepoch", "datetime"] {
        db.create_scalar_function(name, -1, flags, move |ctx| date(ctx, name == "unixepoch"))?;
    }
    db.create_scalar_function("ps_json_contains", 2, flags, |ctx| {
        contains(arg(ctx, 0)?, arg(ctx, 1)?)
    })?;
    for name in ["st_asgeojson", "st_astext", "st_x", "st_y"] {
        db.create_scalar_function(name, -1, flags, move |ctx| geometry(arg(ctx, 0)?, name))?;
    }
    Ok(())
}

fn contains(needle: Value, haystack: Value) -> rusqlite::Result<Value> {
    if needle == Value::Null || haystack == Value::Null {
        return Ok(Value::Null);
    }
    let Value::Text(json) = haystack else {
        return Err(error("IN is only supported on JSON arrays"));
    };
    let value: serde_json::Value = serde_json::from_str(&json).map_err(|e| error(e.to_string()))?;
    let serde_json::Value::Array(values) = value else {
        return Err(error(json));
    };
    let found = values.iter().any(|value| match value {
        serde_json::Value::Null => false,
        serde_json::Value::Bool(v) => numeric_equal(&needle, if *v { 1 } else { 0 }, None),
        serde_json::Value::Number(v) => {
            if let Some(i) = v.as_i64() {
                numeric_equal(&needle, i, None)
            } else if let Some(f) = v.as_f64() {
                numeric_equal(&needle, 0, Some(f))
            } else {
                false
            }
        }
        serde_json::Value::String(v) => matches!(&needle, Value::Text(s) if s == v),
        v => matches!(&needle, Value::Text(s) if s == &v.to_string()),
    });
    Ok(Value::Integer(i64::from(found)))
}

fn numeric_equal(needle: &Value, integer: i64, real: Option<f64>) -> bool {
    match (needle, real) {
        (Value::Integer(v), None) => *v == integer,
        (Value::Real(v), Some(f)) => *v == f,
        (Value::Real(_), None) | (Value::Integer(_), Some(_)) => {
            // Never round a large integer to f64 before comparing it with an integer-valued double.
            let f = match (needle, real) {
                (Value::Real(v), None) => *v,
                (_, Some(v)) => v,
                _ => unreachable!(),
            };
            let i = match needle {
                Value::Integer(v) => *v,
                _ => integer,
            };
            f.is_finite()
                && f.fract() == 0.0
                && f >= i64::MIN as f64
                && f < -(i64::MIN as f64)
                && f as i64 == i
        }
        _ => false,
    }
}

fn date(ctx: &Context<'_>, epoch_output: bool) -> rusqlite::Result<Value> {
    let input = arg(ctx, 0)?;
    let modifier = arg(ctx, 1)?;
    let last = arg(ctx, 2)?;
    let (unix, subsec) = match &modifier {
        Value::Null => (false, false),
        Value::Text(s) if s == "unixepoch" => match &last {
            Value::Null => (true, false),
            Value::Text(s) if s == "subsec" || s == "subsecond" => (true, true),
            _ => return Ok(Value::Null),
        },
        Value::Text(s) if s == "subsec" || s == "subsecond" => (false, true),
        _ => return Ok(Value::Null),
    };
    let Some(ms) = date_millis(&input, unix) else {
        return Ok(Value::Null);
    };
    if epoch_output {
        return Ok(if subsec {
            Value::Real(ms as f64 / 1000.0)
        } else {
            Value::Integer(ms.div_euclid(1000))
        });
    }
    let Some(dt) = DateTime::<Utc>::from_timestamp_millis(ms) else {
        return Ok(Value::Null);
    };
    // Match Date.toISOString's extended positive years and datetime's negative-year normalization.
    let year = match dt.year() {
        y if y < 0 => y.to_string(),
        y if y >= 10000 => format!("+{y:06}"),
        y => format!("{y:04}"),
    };
    let tail = if subsec {
        "%m-%d %H:%M:%S%.3f"
    } else {
        "%m-%d %H:%M:%S"
    };
    let formatted = format!("{year}-{}", dt.format(tail));
    Ok(Value::Text(formatted))
}

fn date_millis(input: &Value, unix: bool) -> Option<i64> {
    let numeric = |v: f64, unix: bool| {
        let ms = if unix {
            v * 1000.0
        } else {
            (v - 2440587.5) * 86400000.0
        };
        (ms.is_finite() && ms.abs() <= 8640000000000000.0).then_some(ms.trunc() as i64)
    };
    match input {
        Value::Integer(v) => numeric(*v as f64, unix),
        Value::Real(v) => numeric(*v, unix),
        Value::Text(s) => {
            if !s.is_empty()
                && s.bytes().all(|b| b.is_ascii_digit() || b == b'.')
                && s.bytes().filter(|b| *b == b'.').count() <= 1
            {
                return numeric(s.parse().ok()?, false);
            }
            let normalized = s.replace(' ', "T").replace('z', "Z");
            if let Ok(d) = DateTime::parse_from_rfc3339(&normalized) {
                return Some(d.timestamp_millis());
            }
            let without_z = normalized.trim_end_matches('Z');
            for format in ["%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%dT%H:%M"] {
                if let Ok(d) = NaiveDateTime::parse_from_str(without_z, format) {
                    return Some(d.and_utc().timestamp_millis());
                }
            }
            NaiveDate::parse_from_str(without_z, "%Y-%m-%d")
                .ok()?
                .and_hms_opt(0, 0, 0)
                .map(|d| d.and_utc().timestamp_millis())
        }
        _ => None,
    }
}

fn geometry(input: Value, name: &str) -> rusqlite::Result<Value> {
    let bytes = match input {
        Value::Null | Value::Integer(_) | Value::Real(_) => return Ok(Value::Null),
        Value::Blob(v) => v,
        Value::Text(v) => {
            // Buffer.from(hex) stops at the first invalid pair and ignores an odd final digit.
            v.as_bytes()
                .chunks_exact(2)
                .map_while(|p| {
                    let pair = std::str::from_utf8(p).ok()?;
                    u8::from_str_radix(pair, 16).ok()
                })
                .collect()
        }
    };
    let geom = Ewkb(&bytes);
    if name == "st_astext" {
        return geom
            .to_wkt()
            .map(|s| Value::Text(normalize_wkt(&s)))
            .map_err(|e| error(e.to_string()));
    }
    let json = geom.to_json().map_err(|e| error(e.to_string()))?;
    let parsed: serde_json::Value =
        serde_json::from_str(&json).map_err(|e| error(e.to_string()))?;
    if name == "st_asgeojson" {
        return Ok(Value::Text(geo_json(&parsed)));
    }
    if parsed["type"] != "Point" {
        return Ok(Value::Null);
    }
    Ok(parsed["coordinates"][if name == "st_x" { 0 } else { 1 }]
        .as_f64()
        .map(Value::Real)
        .unwrap_or(Value::Null))
}

fn normalize_wkt(wkt: &str) -> String {
    let mut result = String::with_capacity(wkt.len());
    let mut chars = wkt.chars().peekable();
    while let Some(c) = chars.next() {
        if c.is_ascii_digit() || c == '-' || c == '+' || c == '.' {
            let mut number = String::from(c);
            while chars
                .peek()
                .is_some_and(|c| c.is_ascii_digit() || matches!(c, '.' | '-' | '+' | 'e' | 'E'))
            {
                number.push(chars.next().unwrap());
            }
            match number.parse::<f64>() {
                Ok(v) => result.push_str(&js_number(v)),
                Err(_) => result.push_str(&number),
            }
        } else {
            result.push(c);
        }
    }
    result
}

fn geo_json(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::Number(n) => {
            let f = n.as_f64().unwrap_or(f64::NAN);
            let s = js_number(f);
            if f.is_finite() && !s.contains(['.', 'e']) {
                format!("{s}.0")
            } else {
                s
            }
        }
        serde_json::Value::Array(a) => {
            format!("[{}]", a.iter().map(geo_json).collect::<Vec<_>>().join(","))
        }
        serde_json::Value::Object(o) => {
            // wkx emits type first, followed by coordinates/geometries.
            let mut keys: Vec<_> = o.keys().collect();
            keys.sort_by_key(|key| if *key == "type" { 0 } else { 1 });
            format!(
                "{{{}}}",
                keys.into_iter()
                    .map(|k| format!("{}:{}", serde_json::to_string(k).unwrap(), geo_json(&o[k])))
                    .collect::<Vec<_>>()
                    .join(",")
            )
        }
        _ => v.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn integer_real_equality_preserves_precision() {
        assert!(numeric_equal(&Value::Integer(1), 0, Some(1.0)));
        assert!(!numeric_equal(
            &Value::Integer(9007199254740993),
            0,
            Some(9007199254740992.0)
        ));
        assert!(!numeric_equal(
            &Value::Integer(i64::MAX),
            0,
            Some(9223372036854775808.0)
        ));
    }
    #[test]
    fn deterministic_dates() {
        assert_eq!(date_millis(&Value::Text("now".into()), false), None);
        assert_eq!(
            date_millis(&Value::Text("1970-01-01".into()), false),
            Some(0)
        );
        assert_eq!(
            date_millis(&Value::Text("2440587.5".into()), false),
            Some(0)
        );
        assert_eq!(date_millis(&Value::Real(-0.0019), true), Some(-1));
    }
}
