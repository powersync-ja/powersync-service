use std::{fmt::Display, mem};

use serde::{Serialize, ser::SerializeSeq};
use serde_json::value::RawValue;

#[derive(Default, Debug, Clone, Copy, PartialEq)]
pub enum SqliteValue<'a> {
    #[default]
    Null,
    Float(f64),
    Integer(i64),
    Blob(&'a [u8]),
    Text(&'a str),
}

impl SqliteValue<'_> {
    /// Whether this value can be represented in JSON.
    ///
    /// Blobs can't, which is why they're omitted from rows synced to clients.
    pub fn is_json_compatible(&self) -> bool {
        !matches!(self, Self::Blob(_))
    }

    /// Whether this is a valid bucket parameter.
    ///
    /// We don't support binary parameters, and `NULL` isn't a valid parameter because it doesn't
    /// compare to itself.
    pub fn is_valid_parameter(&self) -> bool {
        !matches!(self, SqliteValue::Null | SqliteValue::Blob(_))
    }

    /// Whether this value is true when used as a condition, following SQLite semantics.
    ///
    /// `NULL` is false, numbers are true if they're non-zero. Texts and blobs are converted to a
    /// number by parsing their longest numeric prefix (so `'1abc'` is true and `'abc'` is false).
    pub fn is_truthy(&self) -> bool {
        match self {
            Self::Null => false,
            Self::Integer(i) => *i != 0,
            Self::Float(f) => *f != 0.0,
            Self::Text(text) => numeric_prefix(text.as_bytes()) != 0.0,
            Self::Blob(blob) => numeric_prefix(blob) != 0.0,
        }
    }
}

impl Display for SqliteValue<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SqliteValue::Null => write!(f, "null"),
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
            SqliteValue::Null => serializer.serialize_unit(),
            SqliteValue::Float(f) => serializer.serialize_f64(*f),
            SqliteValue::Integer(i) => serializer.serialize_i64(*i),
            SqliteValue::Blob(items) => serializer.serialize_bytes(items),
            SqliteValue::Text(str) => serializer.serialize_str(str),
        }
    }
}

/// A collection of [SqliteValue]s that are also valid bucket parameters.
#[repr(transparent)]
pub struct Parameters<'s, 'a>(pub &'s [SqliteValue<'a>]);

impl<'a> AsRef<[SqliteValue<'a>]> for Parameters<'_, 'a> {
    fn as_ref(&self) -> &[SqliteValue<'a>] {
        self.0
    }
}

impl Serialize for Parameters<'_, '_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(self.0.len()))?;

        for value in self.0 {
            match value {
                SqliteValue::Float(f) => {
                    // Format doubles in a way that's compatible JSONBucketNameSerialize, which in
                    // turn uses JSON.stringify for numbers. It's important that Float(0.0) and
                    // Integer(0) get the same representation.
                    if !f.is_finite() {
                        // NaN and Infinity serialize to null
                        seq.serialize_element(&None::<f64>)
                    } else {
                        let mut buffer = ryu_js::Buffer::new();
                        let formatted = buffer.format_finite(*f);
                        let as_json = unsafe {
                            // Safety: RawValue has the same representation as str. Given that we
                            // know the buffer holds a valid JSON value, we can cast references.
                            mem::transmute::<&str, &RawValue>(formatted)
                        };

                        seq.serialize_element(as_json)
                    }
                }
                SqliteValue::Integer(i) => seq.serialize_element(i),
                SqliteValue::Text(str) => seq.serialize_element(str),
                SqliteValue::Null | SqliteValue::Blob(_) => unreachable!(),
            }?;
        }

        seq.end()
    }
}

/// Parses the longest prefix of `bytes` (after leading whitespace) that is a valid number, or
/// returns `0.0` if there is none.
fn numeric_prefix(bytes: &[u8]) -> f64 {
    let start = bytes
        .iter()
        .position(|b| !b.is_ascii_whitespace())
        .unwrap_or(bytes.len());
    let bytes = &bytes[start..];

    let count_digits = |from: usize| {
        bytes[from..]
            .iter()
            .take_while(|b| b.is_ascii_digit())
            .count()
    };

    let mut end = 0;
    if matches!(bytes.first(), Some(b'+' | b'-')) {
        end += 1;
    }

    let integer_digits = count_digits(end);
    end += integer_digits;
    let mut has_digits = integer_digits > 0;

    if bytes.get(end) == Some(&b'.') {
        let fraction_digits = count_digits(end + 1);
        if fraction_digits > 0 || has_digits {
            end += 1 + fraction_digits;
            has_digits = true;
        }
    }

    if !has_digits {
        return 0.0;
    }

    if matches!(bytes.get(end), Some(b'e' | b'E')) {
        let mut exponent_end = end + 1;
        if matches!(bytes.get(exponent_end), Some(b'+' | b'-')) {
            exponent_end += 1;
        }

        let exponent_digits = count_digits(exponent_end);
        if exponent_digits > 0 {
            end = exponent_end + exponent_digits;
        }
    }

    // The prefix only consists of ASCII characters, so it's valid UTF-8.
    std::str::from_utf8(&bytes[..end])
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0.0)
}

#[cfg(test)]
mod test {
    use crate::sync_plan::Parameters;

    use super::SqliteValue;

    #[test]
    fn truthy() {
        assert!(!SqliteValue::Null.is_truthy());
        assert!(SqliteValue::Integer(-1).is_truthy());
        assert!(!SqliteValue::Integer(0).is_truthy());
        assert!(SqliteValue::Float(0.5).is_truthy());
        assert!(!SqliteValue::Float(0.0).is_truthy());

        for truthy in ["1", " 1abc", "-2", "0.1", ".5", "1e3", "5.", "+3"] {
            assert!(SqliteValue::Text(truthy).is_truthy(), "{truthy}");
        }
        for falsy in ["", "abc", "0", "0.0", "-", ".", "e5", "0e9", " "] {
            assert!(!SqliteValue::Text(falsy).is_truthy(), "{falsy}");
        }

        assert!(SqliteValue::Blob(b"12").is_truthy());
        assert!(!SqliteValue::Blob(b"\xff").is_truthy());
    }

    fn parameter(source: SqliteValue) -> String {
        let value: &[SqliteValue] = &[source];
        let params = Parameters(value);

        serde_json::to_string(&params).unwrap()
    }

    #[test]
    fn parameter_serialize() {
        assert_eq!(parameter(SqliteValue::Text("a")), "[\"a\"]");

        assert_eq!(parameter(SqliteValue::Integer(123)), "[123]");
        assert_eq!(parameter(SqliteValue::Float(123.0)), "[123]");
        assert_eq!(parameter(SqliteValue::Float(123.1)), "[123.1]");
        assert_eq!(parameter(SqliteValue::Float(-0.0)), "[0]");
        assert_eq!(
            parameter(SqliteValue::Float(1e20)),
            "[100000000000000000000]"
        );
        assert_eq!(parameter(SqliteValue::Float(1e21)), "[1e+21]"); // matches JS

        // This matches what JavaScript does, arguably it shouldn't be a parameter at all.
        assert_eq!(parameter(SqliteValue::Float(f64::INFINITY)), "[null]");
        assert_eq!(parameter(SqliteValue::Float(f64::NAN)), "[null]");
    }
}
