use std::{
    cell::Cell,
    fmt::{self, Display, Write},
    mem,
};

use bson::{RawBinaryRef, RawBsonRef, RawDbPointerRef, error::Error, spec::BinarySubtype};
use bumpalo::{Bump, collections::Vec};
use serde::{
    Serialize, Serializer,
    ser::{SerializeMap, SerializeSeq},
};
use serde_json::value::RawValue;
use thiserror::Error;

use crate::sync_plan::{CompatibilityContext, SqliteValue, TimeValuePrecision, evaluation::Frame};

/// An error decoding a BSON source row.
#[derive(Error, Debug)]
pub enum BsonSourceError {
    #[error("Invalid BSON: {0}")]
    Bson(#[from] Error),
    #[error("Could not format inner element as JSON: {0}")]
    WritingJson(#[from] serde_json::Error),
    /// A DBPointer whose serde representation didn't have the expected structure.
    #[error("Could not read BSON DBPointer")]
    InvalidDbPointer,
    /// A date outside of the range representable by JavaScript dates.
    ///
    /// We reject these for compatibility with `bufferToSqlite.ts`.
    #[error("BSON date out of supported range: {0}")]
    DateOutOfRange(i64),
    #[error("json nested object depth exceeds the limit of {NESTED_DEPTH_LIMIT}")]
    NestingTooDeep,
}

/// How to render BSON dates, the Rust equivalent of `DateRenderMode` in `bufferToSqlite.ts`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DateRenderMode {
    /// `2024-01-02 03:04:05.678Z`
    LegacyMilliseconds,
    /// `2024-01-02T03:04:05.678Z`
    IsoMilliseconds,
    /// `2024-01-02T03:04:05Z`
    IsoSeconds,
}

impl From<&CompatibilityContext> for DateRenderMode {
    fn from(value: &CompatibilityContext) -> Self {
        if !value.timestamps_iso8601 {
            return Self::LegacyMilliseconds;
        }

        match value.max_time_value_precision {
            Some(TimeValuePrecision::Seconds) => Self::IsoSeconds,
            // MongoDB only supports millisecond precision, so higher precisions render milliseconds.
            _ => Self::IsoMilliseconds,
        }
    }
}

/// Rust port of `DirectSourceRowConverter`, using the bson crate.
#[derive(Clone, Copy)]
pub struct SqliteValueFromBson<'a> {
    arena: &'a Bump,
    date_mode: DateRenderMode,
    fixed_booleans: bool,
}

impl<'a> From<&Frame<'a>> for SqliteValueFromBson<'a> {
    fn from(value: &Frame<'a>) -> Self {
        Self::new(value.arena, value.compatibility)
    }
}

impl<'a> SqliteValueFromBson<'a> {
    pub fn new(arena: &'a Bump, options: &CompatibilityContext) -> Self {
        Self {
            arena,
            date_mode: options.into(),
            fixed_booleans: options.fixed_booleans_in_json,
        }
    }

    pub fn parse_top_level_value(
        &self,
        value: RawBsonRef<'a>,
    ) -> Result<SqliteValue<'a>, BsonSourceError> {
        Ok(match value {
            RawBsonRef::Double(d) => {
                // This matches bufferToSqlite: integral doubles are widened to integers. The
                // JavaScript implementation also wraps non-safe double to (larger than i64)
                // integers, we preserve those as doubles to match SQLite numeric types.
                if d.fract() == 0.0 && (i64::MIN as f64..i64::MAX as f64).contains(&d) {
                    SqliteValue::Integer(d as i64)
                } else {
                    SqliteValue::Float(d)
                }
            }
            RawBsonRef::Boolean(b) => b.into(),
            RawBsonRef::Int32(v) => SqliteValue::Integer(v as i64),
            RawBsonRef::Int64(v) => SqliteValue::Integer(v),
            RawBsonRef::String(v) | RawBsonRef::Symbol(v) => SqliteValue::Text(v),
            RawBsonRef::Null | RawBsonRef::Undefined | RawBsonRef::MaxKey | RawBsonRef::MinKey => {
                SqliteValue::Null {
                    is_undefined: false,
                }
            }
            RawBsonRef::ObjectId(oid) => SqliteValue::Text(self.display(Hex(&oid.bytes()))),
            RawBsonRef::Timestamp(ts) => {
                SqliteValue::Integer(((ts.time as i64) << 32) | ts.increment as i64)
            }
            RawBsonRef::DateTime(date) => {
                SqliteValue::Text(self.display(self.date(date.timestamp_millis())?))
            }
            RawBsonRef::Binary(binary) => {
                // Only subtype 4 UUIDs are surfaced as strings, other subtypes stay as raw bytes.
                match Uuid::from_binary(&binary) {
                    Some(uuid) => SqliteValue::Text(self.display(uuid)),
                    None => SqliteValue::Blob(binary.bytes),
                }
            }

            RawBsonRef::Decimal128(d) => SqliteValue::Text(self.display(d)),

            RawBsonRef::Array(_)
            | RawBsonRef::Document(_)
            | RawBsonRef::RegularExpression(_)
            | RawBsonRef::JavaScriptCode(_)
            | RawBsonRef::DbPointer(_) => SqliteValue::Text(self.nested_json(value, 0)?),
            RawBsonRef::JavaScriptCodeWithScope(_) => {
                SqliteValue::Text(self.nested_json(value, 1)?)
            }
        })
    }

    /// Formats a value into a string allocated in the arena.
    fn display(&self, value: impl Display) -> &'a str {
        let mut out = bumpalo::collections::String::new_in(self.arena);
        write!(out, "{value}").expect("writing to a String is infallible");
        out.into_bump_str()
    }

    /// Validates a date for formatting like `appendDateTimeToWriter` in `bufferToSqlite.ts`.
    fn date(&self, millis: i64) -> Result<FormattedDate, BsonSourceError> {
        const MILLIS_PER_DAY: i64 = 86_400_000;
        /// The maximum absolute time value of JavaScript dates.
        const MAX_JS_DATE_MILLIS: i64 = 8_640_000_000_000_000;

        if !(-MAX_JS_DATE_MILLIS..=MAX_JS_DATE_MILLIS).contains(&millis) {
            return Err(BsonSourceError::DateOutOfRange(millis));
        }

        let days = millis.div_euclid(MILLIS_PER_DAY);
        let time_of_day = millis.rem_euclid(MILLIS_PER_DAY);
        let (year, month, day) = civil_from_days(days);

        Ok(FormattedDate {
            year,
            month,
            day,
            time_of_day,
            mode: self.date_mode,
        })
    }

    /// Serializes a nested document or array into a JSON string, like
    /// `serializeNestedObjectToJson` and `serializeNestedArrayToJson` in `bufferToSqlite.ts`.
    fn nested_json(&self, value: RawBsonRef<'a>, depth: usize) -> Result<&'a str, BsonSourceError> {
        let context = NestedContext {
            options: self,
            error: Cell::new(None),
        };
        let mut serializer = serde_json::Serializer::new(Vec::new_in(self.arena));

        let result = SerializeNestedValue {
            value,
            context: &context,
            depth,
        }
        .serialize(&mut serializer);
        if let Err(e) = result {
            // Prefer the original error over the opaque serde_json error wrapping it.
            return Err(context
                .error
                .take()
                .unwrap_or(BsonSourceError::WritingJson(e)));
        }

        let bytes = serializer.into_inner().into_bump_slice();
        Ok(unsafe {
            // Safety: serde_json writes valid utf-8
            str::from_utf8_unchecked(bytes)
        })
    }
}

/// Matches `NESTED_DEPTH_LIMIT` in `bufferToSqlite.ts`.
const NESTED_DEPTH_LIMIT: usize = 20;

/// Shared state while serializing a nested BSON value to JSON.
struct NestedContext<'s, 'a> {
    options: &'s SqliteValueFromBson<'a>,
    /// Serde errors are opaque, so we store the [BsonSourceError] that caused serialization to
    /// fail here. This allows callers to distinguish e.g. unsupported values.
    error: Cell<Option<BsonSourceError>>,
}

impl NestedContext<'_, '_> {
    fn fail<E: serde::ser::Error>(&self, error: impl Into<BsonSourceError>) -> E {
        let error = error.into();
        let serde_error = E::custom(&error);
        self.error.set(Some(error));
        serde_error
    }
}

/// Port of `serializeNestedElementValue` from `bufferToSqlite.ts`.
struct SerializeNestedValue<'c, 's, 'a> {
    value: RawBsonRef<'a>,
    context: &'c NestedContext<'s, 'a>,
    depth: usize,
}

impl<'c, 's, 'a> SerializeNestedValue<'c, 's, 'a> {
    /// Whether the value is represented in JSON.
    ///
    /// Undefined values are omitted from objects and written as `null` in arrays, like
    /// `defined: false` in `bufferToSqlite.ts`.
    fn is_defined(value: &RawBsonRef) -> bool {
        match value {
            RawBsonRef::Undefined => false,
            // Nested binary values are omitted from JSON unless they are subtype 4 UUIDs.
            RawBsonRef::Binary(binary) => Uuid::from_binary(binary).is_some(),
            _ => true,
        }
    }

    fn child(&self, value: RawBsonRef<'a>) -> Self {
        Self {
            value,
            context: self.context,
            depth: self.depth + 1,
        }
    }

    fn check_depth<E: serde::ser::Error>(&self) -> Result<(), E> {
        if self.depth > NESTED_DEPTH_LIMIT {
            Err(self.context.fail(BsonSourceError::NestingTooDeep))
        } else {
            Ok(())
        }
    }
}

impl Serialize for SerializeNestedValue<'_, '_, '_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let options = self.context.options;

        match self.value {
            RawBsonRef::Double(d) => {
                // Format doubles like JavaScript's Number.toString(), non-finite values are null.
                if d.is_finite() {
                    let mut buffer = ryu_js::Buffer::new();
                    let formatted = buffer.format_finite(d);
                    let raw = unsafe {
                        // Safety: RawValue has the same representation as str, and ryu_js writes
                        // valid JSON numbers.
                        mem::transmute::<&str, &RawValue>(formatted)
                    };
                    raw.serialize(serializer)
                } else {
                    serializer.serialize_none()
                }
            }
            RawBsonRef::String(s) | RawBsonRef::Symbol(s) => serializer.serialize_str(s),
            RawBsonRef::Boolean(b) => {
                if options.fixed_booleans {
                    serializer.serialize_bool(b)
                } else {
                    serializer.serialize_i32(if b { 1 } else { 0 })
                }
            }
            // Undefined is only reachable here inside arrays, where it's written as null.
            RawBsonRef::Null | RawBsonRef::Undefined | RawBsonRef::MinKey | RawBsonRef::MaxKey => {
                serializer.serialize_none()
            }
            RawBsonRef::Int32(i) => serializer.serialize_i32(i),
            RawBsonRef::Int64(i) => serializer.serialize_i64(i),
            RawBsonRef::Timestamp(ts) => {
                // timestampToBigInt in JavaScript treats this as an unsigned 64-bit integer.
                serializer.serialize_u64(((ts.time as u64) << 32) | ts.increment as u64)
            }
            RawBsonRef::ObjectId(oid) => serializer.collect_str(&Hex(&oid.bytes())),
            RawBsonRef::DateTime(date) => {
                let formatted = options
                    .date(date.timestamp_millis())
                    .map_err(|e| self.context.fail::<S::Error>(e))?;
                serializer.collect_str(&formatted)
            }
            RawBsonRef::Binary(binary) => match Uuid::from_binary(&binary) {
                Some(uuid) => serializer.collect_str(&uuid),
                // Only reachable inside arrays, where values that aren't defined become null.
                None => serializer.serialize_none(),
            },
            RawBsonRef::Array(array) => {
                self.check_depth()?;

                let mut seq = serializer.serialize_seq(None)?;
                for element in array {
                    let value = element.map_err(|e| self.context.fail::<S::Error>(e))?;
                    seq.serialize_element(&self.child(value))?;
                }
                seq.end()
            }
            RawBsonRef::Document(doc) => {
                self.check_depth()?;

                let mut map = serializer.serialize_map(None)?;
                for entry in doc {
                    let (key, value) = entry.map_err(|e| self.context.fail::<S::Error>(e))?;
                    if Self::is_defined(&value) {
                        map.serialize_entry(key.as_str(), &self.child(value))?;
                    }
                }
                map.end()
            }

            RawBsonRef::Decimal128(d) => serializer.collect_str(&d),
            // Like writeRegexJson, options are preserved verbatim.
            RawBsonRef::RegularExpression(regex) => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("pattern", regex.pattern.as_str())?;
                map.serialize_entry("options", regex.options.as_str())?;
                map.end()
            }
            RawBsonRef::JavaScriptCode(code) => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("code", code)?;
                map.serialize_entry("scope", &())?;
                map.end()
            }
            RawBsonRef::JavaScriptCodeWithScope(code) => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("code", code.code)?;
                // Unlike other nested documents, the scope uses the same depth as the code value.
                map.serialize_entry(
                    "scope",
                    &Self {
                        value: RawBsonRef::Document(code.scope),
                        context: self.context,
                        depth: self.depth,
                    },
                )?;
                map.end()
            }
            RawBsonRef::DbPointer(pointer) => {
                let (namespace, oid) =
                    db_pointer_parts(&pointer).map_err(|e| self.context.fail::<S::Error>(e))?;
                // Like writeDbPointerJson, the namespace is split into database and collection
                // at the first dot.
                let (db, collection) = match namespace.split_once('.') {
                    Some((db, collection)) => (Some(db), collection),
                    None => (None, namespace.as_str()),
                };

                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("collection", collection)?;
                map.serialize_entry("oid", &oid)?;
                if let Some(db) = db {
                    map.serialize_entry("db", db)?;
                }
                map.serialize_entry("fields", &EmptyObject)?;
                map.end()
            }
        }
    }
}

/// Serializes as `{}`.
struct EmptyObject;

impl Serialize for EmptyObject {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_map(Some(0))?.end()
    }
}

/// Extracts the namespace and the hex-encoded object id of a DBPointer.
///
/// The bson crate doesn't expose these fields, so we read them from its extended JSON serde
/// representation (`{"$dbPointer": {"$ref": namespace, "$id": {"$oid": hex}}}`). This is slow,
/// but DBPointers are deprecated and rare.
fn db_pointer_parts(pointer: &RawDbPointerRef) -> Result<(String, String), BsonSourceError> {
    let mut value = serde_json::to_value(pointer)?;
    let body = value
        .get_mut("$dbPointer")
        .ok_or(BsonSourceError::InvalidDbPointer)?;

    let take_string = |value: Option<&mut serde_json::Value>| match value.map(mem::take) {
        Some(serde_json::Value::String(s)) => Ok(s),
        _ => Err(BsonSourceError::InvalidDbPointer),
    };

    let namespace = take_string(body.get_mut("$ref"))?;
    let oid = take_string(body.get_mut("$id").and_then(|id| id.get_mut("$oid")))?;
    Ok((namespace, oid))
}

/// Lower-case hex representation of bytes.
struct Hex<'a>(&'a [u8]);

impl Display for Hex<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        for byte in self.0 {
            f.write_char(HEX[(byte >> 4) as usize] as char)?;
            f.write_char(HEX[(byte & 0xf) as usize] as char)?;
        }
        Ok(())
    }
}

/// Formats a UUID like `uuidToString` in `bufferToSqlite.ts`.
struct Uuid<'a>(&'a [u8; 16]);

impl<'a> Uuid<'a> {
    /// Returns the UUID for subtype 4 binaries of the right length.
    ///
    /// The bson crate already strips the nested length of the legacy subtype 2.
    fn from_binary(binary: &RawBinaryRef<'a>) -> Option<Self> {
        if binary.subtype == BinarySubtype::Uuid {
            binary.bytes.try_into().ok().map(Self)
        } else {
            None
        }
    }
}

impl Display for Uuid<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, range) in [0..4, 4..6, 6..8, 8..10, 10..16].into_iter().enumerate() {
            if i != 0 {
                f.write_char('-')?;
            }
            Hex(&self.0[range]).fmt(f)?;
        }
        Ok(())
    }
}

/// A date in the supported range, formatted like `appendDateTimeToWriter` in `bufferToSqlite.ts`.
struct FormattedDate {
    year: i64,
    month: u32,
    day: u32,
    time_of_day: i64,
    mode: DateRenderMode,
}

impl Display for FormattedDate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let millisecond = self.time_of_day % 1000;
        let mut time = self.time_of_day / 1000;
        let second = time % 60;
        time /= 60;
        let minute = time % 60;
        let hour = time / 60;

        let separator = if self.mode == DateRenderMode::LegacyMilliseconds {
            ' '
        } else {
            'T'
        };
        if (0..=9999).contains(&self.year) {
            write!(f, "{:04}", self.year)?;
        } else {
            // Extended years, like Date.toISOString() in JavaScript.
            let sign = if self.year < 0 { '-' } else { '+' };
            write!(f, "{sign}{:06}", self.year.unsigned_abs())?;
        }
        write!(
            f,
            "-{:02}-{:02}{separator}{hour:02}:{minute:02}:{second:02}",
            self.month, self.day
        )?;
        if self.mode != DateRenderMode::IsoSeconds {
            write!(f, ".{millisecond:03}")?;
        }
        f.write_char('Z')
    }
}

/// Converts days since the Unix epoch into a `(year, month, day)` triple in the proleptic
/// Gregorian calendar.
///
/// This is the `civil_from_days` algorithm from <https://howardhinnant.github.io/date_algorithms.html>.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097); // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m, d)
}
