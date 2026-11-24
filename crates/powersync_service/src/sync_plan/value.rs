use std::{cmp::Ordering, fmt::Display, mem};

use crate::sync_plan::operators::{
    bool::{SQLITE_FALSE, SQLITE_TRUE},
    cast,
};
use serde::Serialize;
use serde_json::value::RawValue;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SqliteValue<'a> {
    Null {
        /// Whether this `null` value is treated similarly to `undefined` in JavaScript.
        ///
        /// For a stream like `SELECT id, foo FROM tbl`, the JavaScript implementation omits the
        /// `foo` column when it's missing in the source record. We do the same here, so we need to
        /// be able to tell regular null values apart from this state.
        ///
        /// This value is never emitted by functions or decoders (even when decoding
        /// [bson::spec::ElementType::Undefined]).
        is_undefined: bool,
    },
    Float(f64),
    Integer(i64),
    Blob(&'a [u8]),
    Text(&'a str),
}

impl<'a> SqliteValue<'a> {
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null { is_undefined: _ })
    }

    pub const fn null() -> Self {
        Self::Null {
            is_undefined: false,
        }
    }

    /// Whether this value can be represented in JSON.
    ///
    /// Blobs can't, which is why they're omitted from rows synced to clients.
    pub fn is_json_compatible(&self) -> bool {
        !matches!(self, Self::Blob(_) | Self::Null { is_undefined: true })
    }

    /// Turns this value into a [Parameter] if it's a valid value for parameters.
    ///
    /// We don't support binary parameters, and `NULL` isn't a valid parameter because it doesn't
    /// compare to itself.
    pub fn as_parameter(&self) -> Option<Parameter<'a>> {
        match self {
            SqliteValue::Null { is_undefined: _ } | SqliteValue::Blob(_) => None,
            _ => Some(Parameter(*self)),
        }
    }

    /// Whether this value is true when used as a condition, following SQLite semantics.
    ///
    /// `NULL` is false, numbers are true if they're non-zero. Texts and blobs are converted to a
    /// number by parsing their longest numeric prefix (so `'1abc'` is true and `'abc'` is false).
    pub fn is_truthy(&self) -> bool {
        match self {
            Self::Null { is_undefined: _ } => false,
            Self::Integer(i) => *i != 0,
            Self::Float(f) => *f != 0.0,
            Self::Text(text) => cast::numeric_prefix(text.as_bytes()) != 0.0,
            Self::Blob(blob) => cast::numeric_prefix(blob) != 0.0,
        }
    }

    /// Compares values following `compare` as described in <https://www.sqlite.org/datatype3.html#comparisons>:
    ///
    ///  1. Values of different storage classes are ordered as `NULL` < numbers < text < blobs.
    ///  2. Integers and reals are compared numerically (and exactly, also for integers that can't
    ///     be represented as a double).
    ///  3. Texts and blobs are compared by bytes, which doesn't match JavaScript.
    pub fn compare(&self, other: &Self) -> Ordering {
        use SqliteValue::*;

        match (*self, *other) {
            (Integer(a), Integer(b)) => a.cmp(&b),
            (Float(a), Float(b)) => compare_floats(a, b),
            (Integer(a), Float(b)) => compare_integer_float(a, b),
            (Float(a), Integer(b)) => compare_integer_float(b, a).reverse(),
            (Text(a), Text(b)) => a.cmp(b),
            (Blob(a), Blob(b)) => a.cmp(b),
            _ => self.storage_class_order().cmp(&other.storage_class_order()),
        }
    }

    /// The order of storage classes when comparing values of different classes.
    fn storage_class_order(&self) -> u8 {
        match self {
            SqliteValue::Null { is_undefined: _ } => 0,
            SqliteValue::Integer(_) | SqliteValue::Float(_) => 1,
            SqliteValue::Text(_) => 2,
            SqliteValue::Blob(_) => 3,
        }
    }
}

/// Compares doubles, treating `NaN` similar to `NULL` (as equal to itself and smaller than all other
/// values).
fn compare_floats(a: f64, b: f64) -> Ordering {
    a.partial_cmp(&b)
        .unwrap_or_else(|| a.is_nan().cmp(&b.is_nan()).reverse())
}

/// Compares an integer and a double exactly, without rounding the integer to a double.
fn compare_integer_float(int: i64, float: f64) -> Ordering {
    // 2^63, the smallest double greater than i64::MAX. -2^63 is i64::MIN.
    const TWO_POW_63: f64 = 9_223_372_036_854_775_808.0;

    if float.is_nan() {
        Ordering::Greater
    } else if float >= TWO_POW_63 {
        Ordering::Less
    } else if float < -TWO_POW_63 {
        Ordering::Greater
    } else {
        // The truncated double fits into an i64 exactly.
        let truncated = float.trunc();
        int.cmp(&(truncated as i64)).then_with(|| {
            // Same integral part, so the fractional part of the double decides.
            0.0f64.partial_cmp(&(float - truncated)).expect("not NaN")
        })
    }
}

impl From<bool> for SqliteValue<'static> {
    fn from(value: bool) -> Self {
        if value { SQLITE_TRUE } else { SQLITE_FALSE }
    }
}

impl Display for SqliteValue<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SqliteValue::Null { is_undefined: _ } => write!(f, "null"),
            SqliteValue::Float(num) => write!(f, "{num}"),
            SqliteValue::Integer(i) => write!(f, "{i}"),
            SqliteValue::Blob(items) => write!(f, "blob ({} bytes)", items.len()),
            SqliteValue::Text(s) => write!(f, "{s}"),
        }
    }
}

impl Serialize for SqliteValue<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            SqliteValue::Null { is_undefined: _ } => serializer.serialize_unit(),
            SqliteValue::Float(f) => serializer.serialize_f64(*f),
            SqliteValue::Integer(i) => serializer.serialize_i64(*i),
            SqliteValue::Blob(items) => serializer.serialize_bytes(items),
            SqliteValue::Text(str) => serializer.serialize_str(str),
        }
    }
}

/// A SQLite value that's also a valid bucket parameter (not null and not a blob).
#[derive(Clone, Copy, PartialEq)]
#[repr(transparent)]
pub struct Parameter<'a>(SqliteValue<'a>);

impl<'a> AsRef<SqliteValue<'a>> for Parameter<'a> {
    fn as_ref(&self) -> &SqliteValue<'a> {
        &self.0
    }
}

impl Serialize for Parameter<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self.0 {
            SqliteValue::Float(f) => {
                // Format doubles in a way that's compatible JSONBucketNameSerialize, which in
                // turn uses JSON.stringify for numbers. It's important that Float(0.0) and
                // Integer(0) get the same representation.
                if !f.is_finite() {
                    // NaN and Infinity serialize to null
                    serializer.serialize_none()
                } else if serializer.is_human_readable() {
                    // Formatting to JSON, we serialize the number ourself.
                    let mut buffer = ryu_js::Buffer::new();
                    let formatted = buffer.format_finite(f);
                    let as_json = unsafe {
                        // Safety: RawValue has the same representation as str. Given that we
                        // know the buffer holds a valid JSON value, we can cast references.
                        mem::transmute::<&str, &RawValue>(formatted)
                    };

                    as_json.serialize(serializer)
                } else {
                    // Serializing to BSON (e.g. for parameter lookups).
                    if f.fract() == 0.0 && (i64::MIN as f64..i64::MAX as f64).contains(&f) {
                        serializer.serialize_i64(f as i64)
                    } else {
                        serializer.serialize_f64(f)
                    }
                }
            }
            SqliteValue::Integer(i) => serializer.serialize_i64(i),
            SqliteValue::Text(str) => serializer.serialize_str(str),
            SqliteValue::Null { is_undefined: _ } | SqliteValue::Blob(_) => unreachable!(),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::sync_plan::value::Parameter;

    use super::SqliteValue;

    #[test]
    fn truthy_like_sqlite() {
        // Aslo see the test in sync-rules/test/src/sql_operators.test.ts, this just covers cases
        // where we disagree with JS.
        assert!(SqliteValue::Text("0.1").is_truthy());
        assert!(SqliteValue::Text(".5").is_truthy());
        assert!(SqliteValue::Blob(b"12").is_truthy());
    }

    fn parameter(source: SqliteValue) -> String {
        serde_json::to_string(&Parameter(source)).unwrap()
    }

    #[test]
    fn parameter_serialize() {
        assert_eq!(parameter(SqliteValue::Text("a")), "\"a\"");

        assert_eq!(parameter(SqliteValue::Integer(123)), "123");
        assert_eq!(parameter(SqliteValue::Float(123.0)), "123");
        assert_eq!(parameter(SqliteValue::Float(123.1)), "123.1");
        assert_eq!(parameter(SqliteValue::Float(-0.0)), "0");
        assert_eq!(parameter(SqliteValue::Float(1e20)), "100000000000000000000");
        assert_eq!(parameter(SqliteValue::Float(1e21)), "1e+21"); // matches JS

        // This matches what JavaScript does, arguably it shouldn't be a parameter at all.
        assert_eq!(parameter(SqliteValue::Float(f64::INFINITY)), "null");
        assert_eq!(parameter(SqliteValue::Float(f64::NAN)), "null");
    }
}
