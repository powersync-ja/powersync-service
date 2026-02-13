use bson::raw::{RawArray, RawBsonRef, RawDocument, RawRegexRef};
use bson::{spec::BinarySubtype, DateTime, Decimal128, Timestamp};
use serde_json::{Map, Number, Value};
use std::fmt::Write as _;
use thiserror::Error;

const BYTES_MARKER_KEY: &str = "__bytes";
const DEPTH_LIMIT: usize = 20;

#[derive(Debug, Clone, PartialEq)]
pub enum FlatValue {
    Null,
    String(String),
    Number(f64),
    BigInt(i64),
    Bytes(Vec<u8>),
}

pub type FlatRecord = Vec<(String, FlatValue)>;

#[derive(Debug, Error)]
pub enum ConverterError {
    #[error("failed to decode bson: {0}")]
    BsonDecode(#[from] bson::raw::Error),
    #[error("failed to serialize nested json: {0}")]
    JsonSerialize(#[from] serde_json::Error),
    #[error("unsupported non-finite number")]
    NonFiniteNumber,
    #[error("unsupported datetime value")]
    InvalidDateTime,
    #[error("json nested object depth exceeds the limit of {DEPTH_LIMIT}")]
    DepthLimit,
}

pub type ConverterResult<T> = Result<T, ConverterError>;

pub fn construct_after_record_json(bson_bytes: &[u8]) -> ConverterResult<String> {
    let record = construct_after_record_entries(bson_bytes)?;
    Ok(serde_json::to_string(&flat_record_to_json_object(record))?)
}

pub fn construct_after_record_entries(bson_bytes: &[u8]) -> ConverterResult<FlatRecord> {
    let document = RawDocument::from_bytes(bson_bytes)?;

    let mut out = FlatRecord::new();
    for element in document {
        let (key, value) = element?;
        out.push((key.to_string(), convert_top_level_raw_value(value)?));
    }
    Ok(out)
}

pub(crate) fn convert_top_level_raw_value(value: RawBsonRef<'_>) -> ConverterResult<FlatValue> {
    match value {
        RawBsonRef::Null | RawBsonRef::Undefined => Ok(FlatValue::Null),
        RawBsonRef::String(v) => Ok(FlatValue::String(v.to_string())),
        RawBsonRef::Boolean(v) => Ok(FlatValue::BigInt(if v { 1 } else { 0 })),
        RawBsonRef::Int32(v) => Ok(FlatValue::BigInt(v as i64)),
        RawBsonRef::Int64(v) => Ok(FlatValue::BigInt(v)),
        RawBsonRef::Double(v) => number_or_bigint(v),
        RawBsonRef::ObjectId(v) => Ok(FlatValue::String(v.to_hex())),
        RawBsonRef::DateTime(v) => Ok(FlatValue::String(to_legacy_datetime_string(v)?)),
        RawBsonRef::Binary(v) => top_level_binary(v.subtype, v.bytes),
        RawBsonRef::Decimal128(v) => Ok(FlatValue::String(decimal128_to_string(&v))),
        RawBsonRef::RegularExpression(v) => regex_to_top_level_string(v),
        RawBsonRef::MaxKey | RawBsonRef::MinKey => Ok(FlatValue::Null),
        RawBsonRef::Array(values) => nested_array_to_json_text(values),
        RawBsonRef::Document(values) => nested_document_to_json_text(values),
        RawBsonRef::Timestamp(ts) => nested_timestamp_to_json_text(ts),
        _ => Ok(FlatValue::Null),
    }
}

fn flat_record_to_json_object(record: FlatRecord) -> Map<String, Value> {
    let mut out = Map::with_capacity(record.len());

    for (key, value) in record {
        let mapped = match value {
            FlatValue::Null => Value::Null,
            FlatValue::String(v) => Value::String(v),
            FlatValue::Number(v) => Number::from_f64(v)
                .map(Value::Number)
                .unwrap_or(Value::Null),
            FlatValue::BigInt(v) => Value::Number(Number::from(v)),
            FlatValue::Bytes(v) => bytes_marker(v),
        };
        out.insert(key, mapped);
    }

    out
}

fn regex_to_top_level_string(value: RawRegexRef<'_>) -> ConverterResult<FlatValue> {
    let mut out = String::new();
    write_regex_json(value, &mut out);
    Ok(FlatValue::String(out))
}

fn top_level_binary(subtype: BinarySubtype, bytes: &[u8]) -> ConverterResult<FlatValue> {
    if is_uuid_subtype(subtype) {
        Ok(FlatValue::String(uuid_or_hex(bytes)))
    } else {
        Ok(FlatValue::Bytes(bytes.to_vec()))
    }
}

fn nested_document_to_json_text(document: &RawDocument) -> ConverterResult<FlatValue> {
    let mut out = String::with_capacity(document.as_bytes().len().saturating_mul(2));
    write_nested_document(document, 1, &mut out)?;
    Ok(FlatValue::String(out))
}

fn nested_array_to_json_text(values: &RawArray) -> ConverterResult<FlatValue> {
    let mut out = String::with_capacity(values.as_bytes().len().saturating_mul(2));
    write_nested_array(values, 1, &mut out)?;
    Ok(FlatValue::String(out))
}

fn nested_timestamp_to_json_text(value: Timestamp) -> ConverterResult<FlatValue> {
    let mut out = String::with_capacity(32);
    write_timestamp_json(value, &mut out);
    Ok(FlatValue::String(out))
}

fn write_nested_document(
    document: &RawDocument,
    depth: usize,
    out: &mut String,
) -> ConverterResult<()> {
    out.push('{');
    let mut first = true;

    for element in document {
        let (key, value) = element?;
        let field_start = out.len();
        if !first {
            out.push(',');
        }
        push_json_string(out, key);
        out.push(':');

        if write_nested_value(value, depth, out)? {
            first = false;
        } else {
            out.truncate(field_start);
        }
    }

    out.push('}');
    Ok(())
}

fn write_nested_array(values: &RawArray, depth: usize, out: &mut String) -> ConverterResult<()> {
    out.push('[');
    let mut first = true;

    for value in values {
        if !first {
            out.push(',');
        }
        first = false;

        let mapped = value?;
        if !write_nested_value(mapped, depth, out)? {
            out.push_str("null");
        }
    }

    out.push(']');
    Ok(())
}

fn write_nested_value(
    value: RawBsonRef<'_>,
    depth: usize,
    out: &mut String,
) -> ConverterResult<bool> {
    if depth > DEPTH_LIMIT {
        return Err(ConverterError::DepthLimit);
    }

    match value {
        RawBsonRef::Null => {
            out.push_str("null");
            Ok(true)
        }
        RawBsonRef::Undefined => Ok(false),
        RawBsonRef::String(v) => {
            push_json_string(out, v);
            Ok(true)
        }
        RawBsonRef::Boolean(v) => {
            write_i64(out, if v { 1 } else { 0 });
            Ok(true)
        }
        RawBsonRef::Int32(v) => {
            write_i64(out, v as i64);
            Ok(true)
        }
        RawBsonRef::Int64(v) => {
            write_i64(out, v);
            Ok(true)
        }
        RawBsonRef::Double(v) => {
            write_number_or_integer(v, out)?;
            Ok(true)
        }
        RawBsonRef::ObjectId(v) => {
            let text = v.to_hex();
            push_json_string(out, &text);
            Ok(true)
        }
        RawBsonRef::DateTime(v) => {
            let text = to_legacy_datetime_string(v)?;
            push_json_string(out, &text);
            Ok(true)
        }
        RawBsonRef::Binary(_) => Ok(false),
        RawBsonRef::Decimal128(v) => {
            let text = decimal128_to_string(&v);
            push_json_string(out, &text);
            Ok(true)
        }
        RawBsonRef::RegularExpression(v) => {
            write_regex_json(v, out);
            Ok(true)
        }
        RawBsonRef::MaxKey | RawBsonRef::MinKey => {
            out.push_str("null");
            Ok(true)
        }
        RawBsonRef::Array(values) => {
            write_nested_array(values, depth + 1, out)?;
            Ok(true)
        }
        RawBsonRef::Document(values) => {
            write_nested_document(values, depth + 1, out)?;
            Ok(true)
        }
        RawBsonRef::Timestamp(ts) => {
            write_timestamp_json(ts, out);
            Ok(true)
        }
        _ => Ok(false),
    }
}

fn write_regex_json(value: RawRegexRef<'_>, out: &mut String) {
    out.push('{');
    push_json_string(out, "pattern");
    out.push(':');
    push_json_string(out, value.pattern);
    out.push(',');
    push_json_string(out, "options");
    out.push(':');
    let flags = regex_options_to_js_flags(value.options);
    push_json_string(out, &flags);
    out.push('}');
}

fn write_timestamp_json(value: Timestamp, out: &mut String) {
    out.push('{');
    push_json_string(out, "t");
    out.push(':');
    write_i64(out, value.time as i64);
    out.push(',');
    push_json_string(out, "i");
    out.push(':');
    write_i64(out, value.increment as i64);
    out.push('}');
}

fn push_json_string(out: &mut String, value: &str) {
    out.push('"');
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0C}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c <= '\u{1F}' => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            _ => out.push(ch),
        }
    }
    out.push('"');
}

fn write_i64(out: &mut String, value: i64) {
    let _ = write!(out, "{value}");
}

fn write_number_or_integer(value: f64, out: &mut String) -> ConverterResult<()> {
    if !value.is_finite() {
        return Err(ConverterError::NonFiniteNumber);
    }

    if value.fract() == 0.0 && value >= i64::MIN as f64 && value <= i64::MAX as f64 {
        write_i64(out, value as i64);
    } else {
        let _ = write!(out, "{value}");
    }

    Ok(())
}

fn decimal128_to_string(value: &Decimal128) -> String {
    value.to_string()
}

fn number_or_bigint(value: f64) -> ConverterResult<FlatValue> {
    if !value.is_finite() {
        return Err(ConverterError::NonFiniteNumber);
    }

    if value.fract() == 0.0 && value >= i64::MIN as f64 && value <= i64::MAX as f64 {
        Ok(FlatValue::BigInt(value as i64))
    } else {
        Ok(FlatValue::Number(value))
    }
}

fn to_legacy_datetime_string(value: DateTime) -> ConverterResult<String> {
    let millis = value.timestamp_millis();
    let Some(dt) = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(millis) else {
        return Err(ConverterError::InvalidDateTime);
    };
    let iso = dt.to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    Ok(iso.replacen('T', " ", 1))
}

fn is_uuid_subtype(subtype: BinarySubtype) -> bool {
    matches!(subtype, BinarySubtype::Uuid | BinarySubtype::UuidOld)
}

fn bytes_marker(bytes: Vec<u8>) -> Value {
    let mut object = Map::new();
    let values = bytes
        .into_iter()
        .map(|value| Value::Number(Number::from(value as i64)))
        .collect::<Vec<_>>();
    object.insert(BYTES_MARKER_KEY.to_string(), Value::Array(values));
    Value::Object(object)
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for value in bytes {
        out.push(HEX[(value >> 4) as usize] as char);
        out.push(HEX[(value & 0x0f) as usize] as char);
    }
    out
}

fn uuid_or_hex(bytes: &[u8]) -> String {
    if bytes.len() != 16 {
        return hex_lower(bytes);
    }

    format!(
        "{}-{}-{}-{}-{}",
        hex_lower(&bytes[0..4]),
        hex_lower(&bytes[4..6]),
        hex_lower(&bytes[6..8]),
        hex_lower(&bytes[8..10]),
        hex_lower(&bytes[10..16])
    )
}

fn regex_options_to_js_flags(options: &str) -> String {
    let mut out = String::new();

    // In this stack's BSON encoding, `s` carries JS global flag `g`.
    if options.contains('s') {
        out.push('g');
    }
    if options.contains('i') {
        out.push('i');
    }
    if options.contains('m') {
        out.push('m');
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::{doc, Binary, Bson, Regex};

    #[test]
    fn converts_core_types() {
        let doc = doc! {
            "name": "n",
            "int32": 42_i32,
            "int64": 43_i64,
            "bool": true,
            "float": 1.5_f64,
            "float_i": 2.0_f64,
            "null": Bson::Null,
            "date": bson::DateTime::from_millis(1_738_886_400_123),
            "re": Bson::RegularExpression(Regex { pattern: "a+".to_string(), options: "i".to_string() })
        };

        let bytes = bson::to_vec(&doc).unwrap();
        let converted = construct_after_record_entries(&bytes).unwrap();

        assert_eq!(
            converted,
            vec![
                ("name".to_string(), FlatValue::String("n".to_string())),
                ("int32".to_string(), FlatValue::BigInt(42)),
                ("int64".to_string(), FlatValue::BigInt(43)),
                ("bool".to_string(), FlatValue::BigInt(1)),
                ("float".to_string(), FlatValue::Number(1.5)),
                ("float_i".to_string(), FlatValue::BigInt(2)),
                ("null".to_string(), FlatValue::Null),
                (
                    "date".to_string(),
                    FlatValue::String("2025-02-07 00:00:00.123Z".to_string())
                ),
                (
                    "re".to_string(),
                    FlatValue::String(r#"{"pattern":"a+","options":"i"}"#.to_string())
                )
            ]
        );
    }

    #[test]
    fn converts_nested_structures_to_json_text() {
        let doc = doc! {
            "nested": {
                "a": 1_i32,
                "drop_me": Bson::Binary(Binary { subtype: BinarySubtype::Generic, bytes: vec![1, 2, 3] }),
                "arr": [1_i32, Bson::Undefined, 3_i32]
            }
        };
        let bytes = bson::to_vec(&doc).unwrap();
        let converted = construct_after_record_entries(&bytes).unwrap();

        assert_eq!(
            converted,
            vec![(
                "nested".to_string(),
                FlatValue::String(r#"{"a":1,"arr":[1,null,3]}"#.to_string())
            )]
        );
    }

    #[test]
    fn converts_top_level_binary_and_uuid() {
        let uuid = Binary {
            subtype: BinarySubtype::Uuid,
            bytes: vec![
                0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd,
                0xee, 0xff,
            ],
        };
        let binary = Binary {
            subtype: BinarySubtype::Generic,
            bytes: vec![1, 2, 3],
        };

        let doc = doc! {
            "uuid": Bson::Binary(uuid),
            "binary": Bson::Binary(binary)
        };
        let bytes = bson::to_vec(&doc).unwrap();
        let converted = construct_after_record_entries(&bytes).unwrap();

        assert_eq!(
            converted,
            vec![
                (
                    "uuid".to_string(),
                    FlatValue::String("00112233-4455-6677-8899-aabbccddeeff".to_string())
                ),
                ("binary".to_string(), FlatValue::Bytes(vec![1, 2, 3]))
            ]
        );
    }

    #[test]
    fn maps_bson_regex_options_to_js_flags() {
        assert_eq!(regex_options_to_js_flags(""), "");
        assert_eq!(regex_options_to_js_flags("i"), "i");
        assert_eq!(regex_options_to_js_flags("m"), "m");
        assert_eq!(regex_options_to_js_flags("s"), "g");
        assert_eq!(regex_options_to_js_flags("is"), "gi");
        assert_eq!(regex_options_to_js_flags("ms"), "gm");
        assert_eq!(regex_options_to_js_flags("ims"), "gim");
    }

    #[test]
    fn escapes_nested_json_strings() {
        let doc = doc! {
            "nested": {
                "quote": "he said \"hi\"",
                "newline": "a\nb"
            }
        };

        let bytes = bson::to_vec(&doc).unwrap();
        let converted = construct_after_record_entries(&bytes).unwrap();

        assert_eq!(
            converted,
            vec![(
                "nested".to_string(),
                FlatValue::String(r#"{"quote":"he said \"hi\"","newline":"a\nb"}"#.to_string())
            )]
        );
    }
}
