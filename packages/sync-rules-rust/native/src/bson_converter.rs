//! Adapted from packages/mongo-after-record-rs/src/converter.rs at 5773fd47b (rust-sync-plans).
//! Uses the BSON crate for parsing; renders PowerSync SQLite values, not Extended JSON.
use bson::raw::{RawArray, RawBsonRef, RawDocument, RawRegexRef};
use bson::{spec::BinarySubtype, DateTime, Decimal128, Timestamp};
use chrono::{Datelike, Timelike};
use rusqlite::types::Value as FlatValue;
use thiserror::Error;

const DEPTH_LIMIT: usize = 20;
pub type FlatRecord = Vec<(String, FlatValue)>;

#[derive(Clone, Copy)]
pub enum DateRenderMode {
    LegacyMilliseconds,
    IsoMilliseconds,
    IsoSeconds,
}
impl TryFrom<u32> for DateRenderMode {
    type Error = String;
    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::LegacyMilliseconds),
            1 => Ok(Self::IsoMilliseconds),
            2 => Ok(Self::IsoSeconds),
            _ => Err("Invalid MongoDB date render mode".into()),
        }
    }
}

#[derive(Debug, Error)]
pub enum ConverterError {
    #[error("deprecated BSON DBPointer is not supported")]
    UnsupportedDbPointer,
    #[error("failed to decode bson: {0}")]
    BsonDecode(#[from] bson::raw::Error),
    #[error("failed to serialize nested json: {0}")]
    JsonSerialize(#[from] serde_json::Error),
    #[error("BSON integer exceeds SQLite signed 64-bit range")]
    IntegerRange,
    #[error("unsupported datetime value")]
    InvalidDateTime,
    #[error("json nested object depth exceeds the limit of {DEPTH_LIMIT}")]
    DepthLimit,
}

pub type ConverterResult<T> = Result<T, ConverterError>;

pub fn construct_after_record_entries(
    bson_bytes: &[u8],
    mode: DateRenderMode,
) -> ConverterResult<FlatRecord> {
    let document = RawDocument::from_bytes(bson_bytes)?;

    let mut out = FlatRecord::new();
    for element in document {
        let (key, value) = element?;
        out.push((key.to_string(), convert_top_level_raw_value(value, mode)?));
    }
    Ok(out)
}

pub(crate) fn convert_top_level_raw_value(
    value: RawBsonRef<'_>,
    mode: DateRenderMode,
) -> ConverterResult<FlatValue> {
    match value {
        RawBsonRef::Null | RawBsonRef::Undefined => Ok(FlatValue::Null),
        RawBsonRef::String(v) => Ok(FlatValue::Text(v.to_string())),
        RawBsonRef::Boolean(v) => Ok(FlatValue::Integer(if v { 1 } else { 0 })),
        RawBsonRef::Int32(v) => Ok(FlatValue::Integer(v as i64)),
        RawBsonRef::Int64(v) => Ok(FlatValue::Integer(v)),
        RawBsonRef::Double(v) => number_or_bigint(v),
        RawBsonRef::ObjectId(v) => Ok(FlatValue::Text(v.to_hex())),
        RawBsonRef::DateTime(v) => Ok(FlatValue::Text(datetime_string(v, mode)?)),
        RawBsonRef::Binary(v) => top_level_binary(v.subtype, v.bytes),
        RawBsonRef::Decimal128(v) => Ok(FlatValue::Text(decimal128_to_string(&v))),
        RawBsonRef::RegularExpression(v) => regex_to_top_level_string(v),
        RawBsonRef::MaxKey | RawBsonRef::MinKey => Ok(FlatValue::Null),
        RawBsonRef::Array(values) => nested_array_to_json_text(values, mode),
        RawBsonRef::Document(values) => nested_document_to_json_text(values, mode),
        RawBsonRef::Timestamp(ts) => Ok(FlatValue::Integer(
            i64::try_from(timestamp_integer(ts)).map_err(|_| ConverterError::IntegerRange)?,
        )),
        RawBsonRef::Symbol(v) => Ok(FlatValue::Text(v.to_string())),
        value => {
            let mut out = String::new();
            if write_nested_value(value, 0, &mut out, mode)? {
                Ok(FlatValue::Text(out))
            } else {
                Ok(FlatValue::Null)
            }
        }
    }
}

fn regex_to_top_level_string(value: RawRegexRef<'_>) -> ConverterResult<FlatValue> {
    let mut out = String::new();
    write_regex_json(value, &mut out);
    Ok(FlatValue::Text(out))
}

fn top_level_binary(subtype: BinarySubtype, bytes: &[u8]) -> ConverterResult<FlatValue> {
    if subtype == BinarySubtype::Uuid && bytes.len() == 16 {
        Ok(FlatValue::Text(uuid_or_hex(bytes)))
    } else {
        Ok(FlatValue::Blob(bytes.to_vec()))
    }
}

fn nested_document_to_json_text(
    document: &RawDocument,
    mode: DateRenderMode,
) -> ConverterResult<FlatValue> {
    let mut out = String::with_capacity(document.as_bytes().len().saturating_mul(2));
    write_nested_document(document, 0, &mut out, mode)?;
    Ok(FlatValue::Text(out))
}

fn nested_array_to_json_text(
    values: &RawArray,
    mode: DateRenderMode,
) -> ConverterResult<FlatValue> {
    let mut out = String::with_capacity(values.as_bytes().len().saturating_mul(2));
    write_nested_array(values, 0, &mut out, mode)?;
    Ok(FlatValue::Text(out))
}

fn write_nested_document(
    document: &RawDocument,
    depth: usize,
    out: &mut String,
    mode: DateRenderMode,
) -> ConverterResult<()> {
    if depth > DEPTH_LIMIT {
        return Err(ConverterError::DepthLimit);
    }
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

        if write_nested_value(value, depth, out, mode)? {
            first = false;
        } else {
            out.truncate(field_start);
        }
    }

    out.push('}');
    Ok(())
}

fn write_nested_array(
    values: &RawArray,
    depth: usize,
    out: &mut String,
    mode: DateRenderMode,
) -> ConverterResult<()> {
    if depth > DEPTH_LIMIT {
        return Err(ConverterError::DepthLimit);
    }
    out.push('[');
    let mut first = true;

    for value in values {
        if !first {
            out.push(',');
        }
        first = false;

        let mapped = value?;
        if !write_nested_value(mapped, depth, out, mode)? {
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
    mode: DateRenderMode,
) -> ConverterResult<bool> {
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
            if v.is_finite() {
                out.push_str(&crate::functions::js_number(v));
            } else {
                out.push_str("null");
            }
            Ok(true)
        }
        RawBsonRef::ObjectId(v) => {
            let text = v.to_hex();
            push_json_string(out, &text);
            Ok(true)
        }
        RawBsonRef::DateTime(v) => {
            let text = datetime_string(v, mode)?;
            push_json_string(out, &text);
            Ok(true)
        }
        RawBsonRef::Binary(v) => {
            if v.subtype == BinarySubtype::Uuid && v.bytes.len() == 16 {
                push_json_string(out, &uuid_or_hex(v.bytes));
                Ok(true)
            } else {
                Ok(false)
            }
        }
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
            write_nested_array(values, depth + 1, out, mode)?;
            Ok(true)
        }
        RawBsonRef::Document(values) => {
            write_nested_document(values, depth + 1, out, mode)?;
            Ok(true)
        }
        RawBsonRef::Timestamp(ts) => {
            out.push_str(&timestamp_integer(ts).to_string());
            Ok(true)
        }
        RawBsonRef::Symbol(v) => {
            push_json_string(out, v);
            Ok(true)
        }
        RawBsonRef::JavaScriptCode(v) => {
            out.push_str("{\"code\":");
            push_json_string(out, v);
            out.push_str(",\"scope\":null}");
            Ok(true)
        }
        RawBsonRef::JavaScriptCodeWithScope(v) => {
            out.push_str("{\"code\":");
            push_json_string(out, v.code);
            out.push_str(",\"scope\":");
            write_nested_document(v.scope, depth + 1, out, mode)?;
            out.push('}');
            Ok(true)
        }
        // The BSON crate deliberately keeps DBPointer fields private. Reject this
        // deprecated type rather than silently generating the wrong payload.
        RawBsonRef::DbPointer(_) => Err(ConverterError::UnsupportedDbPointer),
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
    push_json_string(out, value.options);
    out.push('}');
}

fn timestamp_integer(value: Timestamp) -> u64 {
    (u64::from(value.time) << 32) | u64::from(value.increment)
}

fn push_json_string(out: &mut String, value: &str) {
    out.push_str(&serde_json::to_string(value).expect("String serialization is infallible"));
}

fn write_i64(out: &mut String, value: i64) {
    let mut buffer = itoa::Buffer::new();
    out.push_str(buffer.format(value));
}

fn decimal128_to_string(value: &Decimal128) -> String {
    value.to_string()
}

fn number_or_bigint(value: f64) -> ConverterResult<FlatValue> {
    if value.is_finite() && value.fract() == 0.0 {
        // i64::MAX rounds up as f64. The upper bound must therefore be exclusive.
        if value >= i64::MIN as f64 && value < -(i64::MIN as f64) {
            Ok(FlatValue::Integer(value as i64))
        } else {
            Err(ConverterError::IntegerRange)
        }
    } else {
        Ok(FlatValue::Real(value))
    }
}

fn datetime_string(value: DateTime, mode: DateRenderMode) -> ConverterResult<String> {
    let dt = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(value.timestamp_millis())
        .ok_or(ConverterError::InvalidDateTime)?;
    // Match JavaScript ISO years for the range supported by chrono.
    let year = dt.year();
    let year = if (0..=9999).contains(&year) {
        format!("{year:04}")
    } else {
        format!("{year:+07}")
    };
    let sep = if matches!(mode, DateRenderMode::LegacyMilliseconds) {
        ' '
    } else {
        'T'
    };
    let mut text = format!(
        "{year}-{:02}-{:02}{sep}{:02}:{:02}:{:02}",
        dt.month(),
        dt.day(),
        dt.hour(),
        dt.minute(),
        dt.second()
    );
    if !matches!(mode, DateRenderMode::IsoSeconds) {
        text.push_str(&format!(".{:03}", dt.timestamp_subsec_millis()));
    }
    text.push('Z');
    Ok(text)
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
        let converted =
            construct_after_record_entries(&bytes, DateRenderMode::LegacyMilliseconds).unwrap();

        assert_eq!(
            converted,
            vec![
                ("name".to_string(), FlatValue::Text("n".to_string())),
                ("int32".to_string(), FlatValue::Integer(42)),
                ("int64".to_string(), FlatValue::Integer(43)),
                ("bool".to_string(), FlatValue::Integer(1)),
                ("float".to_string(), FlatValue::Real(1.5)),
                ("float_i".to_string(), FlatValue::Integer(2)),
                ("null".to_string(), FlatValue::Null),
                (
                    "date".to_string(),
                    FlatValue::Text("2025-02-07 00:00:00.123Z".to_string())
                ),
                (
                    "re".to_string(),
                    FlatValue::Text(r#"{"pattern":"a+","options":"i"}"#.to_string())
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
        let converted =
            construct_after_record_entries(&bytes, DateRenderMode::LegacyMilliseconds).unwrap();

        assert_eq!(
            converted,
            vec![(
                "nested".to_string(),
                FlatValue::Text(r#"{"a":1,"arr":[1,null,3]}"#.to_string())
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
        let converted =
            construct_after_record_entries(&bytes, DateRenderMode::LegacyMilliseconds).unwrap();

        assert_eq!(
            converted,
            vec![
                (
                    "uuid".to_string(),
                    FlatValue::Text("00112233-4455-6677-8899-aabbccddeeff".to_string())
                ),
                ("binary".to_string(), FlatValue::Blob(vec![1, 2, 3]))
            ]
        );
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
        let converted =
            construct_after_record_entries(&bytes, DateRenderMode::LegacyMilliseconds).unwrap();

        assert_eq!(
            converted,
            vec![(
                "nested".to_string(),
                FlatValue::Text(r#"{"quote":"he said \"hi\"","newline":"a\nb"}"#.to_string())
            )]
        );
    }
}
