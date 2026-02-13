use bson::{spec::BinarySubtype, Bson, DateTime, Decimal128, Document, Regex, Timestamp};
use serde_json::{Map, Number, Value};
use std::io::Cursor;
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
    BsonDecode(#[from] bson::de::Error),
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
    let mut cursor = Cursor::new(bson_bytes);
    let document = Document::from_reader(&mut cursor)?;

    let mut out = FlatRecord::with_capacity(document.len());
    for (key, value) in document {
        out.push((key, convert_top_level_value(value)?));
    }
    Ok(out)
}

pub(crate) fn convert_top_level_value(value: Bson) -> ConverterResult<FlatValue> {
    match value {
        Bson::Null | Bson::Undefined => Ok(FlatValue::Null),
        Bson::String(v) => Ok(FlatValue::String(v)),
        Bson::Boolean(v) => Ok(FlatValue::BigInt(if v { 1 } else { 0 })),
        Bson::Int32(v) => Ok(FlatValue::BigInt(v as i64)),
        Bson::Int64(v) => Ok(FlatValue::BigInt(v)),
        Bson::Double(v) => number_or_bigint(v),
        Bson::ObjectId(v) => Ok(FlatValue::String(v.to_hex())),
        Bson::DateTime(v) => Ok(FlatValue::String(to_legacy_datetime_string(v)?)),
        Bson::Binary(v) => top_level_binary(v),
        Bson::Decimal128(v) => Ok(FlatValue::String(decimal128_to_string(&v))),
        Bson::RegularExpression(v) => regex_to_top_level_string(v),
        Bson::MinKey | Bson::MaxKey => Ok(FlatValue::Null),
        Bson::Array(values) => nested_to_json_text(filter_nested_array(values, 1)?),
        Bson::Document(values) => nested_to_json_text(filter_nested_document(values, 1)?),
        Bson::Timestamp(ts) => nested_to_json_text(timestamp_to_json(ts)),
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

fn nested_to_json_text(value: Value) -> ConverterResult<FlatValue> {
    Ok(FlatValue::String(serde_json::to_string(&value)?))
}

fn regex_to_top_level_string(value: Regex) -> ConverterResult<FlatValue> {
    let mut obj = Map::new();
    obj.insert("pattern".to_string(), Value::String(value.pattern));
    obj.insert(
        "options".to_string(),
        Value::String(regex_options_to_js_flags(value.options.as_str())),
    );
    nested_to_json_text(Value::Object(obj))
}

fn top_level_binary(value: bson::Binary) -> ConverterResult<FlatValue> {
    if is_uuid_subtype(value.subtype) {
        Ok(FlatValue::String(uuid_or_hex(&value.bytes)))
    } else {
        Ok(FlatValue::Bytes(value.bytes))
    }
}

fn filter_nested_value(value: Bson, depth: usize) -> ConverterResult<Option<Value>> {
    if depth > DEPTH_LIMIT {
        return Err(ConverterError::DepthLimit);
    }

    match value {
        Bson::Null => Ok(Some(Value::Null)),
        Bson::Undefined => Ok(None),
        Bson::String(v) => Ok(Some(Value::String(v))),
        Bson::Boolean(v) => Ok(Some(int_value(if v { 1 } else { 0 }))),
        Bson::Int32(v) => Ok(Some(int_value(v as i64))),
        Bson::Int64(v) => Ok(Some(int_value(v))),
        Bson::Double(v) => Ok(Some(number_json_or_integer(v)?)),
        Bson::ObjectId(v) => Ok(Some(Value::String(v.to_hex()))),
        Bson::DateTime(v) => Ok(Some(Value::String(to_legacy_datetime_string(v)?))),
        Bson::Binary(_) => Ok(None),
        Bson::Decimal128(v) => Ok(Some(Value::String(decimal128_to_string(&v)))),
        Bson::RegularExpression(v) => {
            let mut obj = Map::new();
            obj.insert("pattern".to_string(), Value::String(v.pattern));
            obj.insert(
                "options".to_string(),
                Value::String(regex_options_to_js_flags(v.options.as_str())),
            );
            Ok(Some(Value::Object(obj)))
        }
        Bson::MinKey | Bson::MaxKey => Ok(Some(Value::Null)),
        Bson::Array(values) => Ok(Some(filter_nested_array(values, depth + 1)?)),
        Bson::Document(values) => Ok(Some(filter_nested_document(values, depth + 1)?)),
        Bson::Timestamp(ts) => Ok(Some(timestamp_to_json(ts))),
        _ => Ok(None),
    }
}

fn filter_nested_document(document: Document, depth: usize) -> ConverterResult<Value> {
    let mut out = Map::new();
    for (key, value) in document {
        if let Some(mapped) = filter_nested_value(value, depth)? {
            out.insert(key, mapped);
        }
    }
    Ok(Value::Object(out))
}

fn filter_nested_array(values: Vec<Bson>, depth: usize) -> ConverterResult<Value> {
    let mut out = Vec::with_capacity(values.len());
    for value in values {
        if let Some(mapped) = filter_nested_value(value, depth)? {
            out.push(mapped);
        } else {
            out.push(Value::Null);
        }
    }
    Ok(Value::Array(out))
}

fn timestamp_to_json(value: Timestamp) -> Value {
    let mut obj = Map::new();
    obj.insert("t".to_string(), int_value(value.time as i64));
    obj.insert("i".to_string(), int_value(value.increment as i64));
    Value::Object(obj)
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

fn number_json_or_integer(value: f64) -> ConverterResult<Value> {
    if !value.is_finite() {
        return Err(ConverterError::NonFiniteNumber);
    }

    if value.fract() == 0.0 && value >= i64::MIN as f64 && value <= i64::MAX as f64 {
        Ok(int_value(value as i64))
    } else {
        let number = Number::from_f64(value).ok_or(ConverterError::NonFiniteNumber)?;
        Ok(Value::Number(number))
    }
}

fn int_value(value: i64) -> Value {
    Value::Number(Number::from(value))
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
    use bson::{doc, Binary};

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
}
