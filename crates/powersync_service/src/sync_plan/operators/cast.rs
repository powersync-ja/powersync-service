//! A port of `cast.ts`, converting values between SQLite types.
//!
//! Functions here follow the TypeScript implementation (which in turn uses JavaScript semantics
//! in some places, such as formatting numbers as text) rather than SQLite. Values that need to be
//! allocated are stored in the arena for the row being evaluated.

use bumpalo::Bump;

use crate::sync_plan::{SqliteValue, expression::CastType};

/// Extracts and normalizes the ID column from a row (`idFromData` in TypeScript).
///
/// Rows without an `id` column (or with a `NULL` id) get an empty id. That results in a single
/// arbitrary row of the table being synced, which is better than not being able to sync anything.
pub fn id_from_data<'a>(id: SqliteValue<'a>, arena: &'a Bump) -> &'a str {
    cast_as_text(id, arena).unwrap_or("")
}

/// Converts a value to text (`castAsText` in TypeScript), returning [None] for `NULL`.
///
/// Numbers are formatted like JavaScript's `toString()`, so `1.0` is `"1"` and `1e21` is
/// `"1e+21"`. Blobs are decoded as UTF-8, replacing invalid sequences like a `TextDecoder`.
pub fn cast_as_text<'a>(value: SqliteValue<'a>, arena: &'a Bump) -> Option<&'a str> {
    Some(match value {
        SqliteValue::Null { is_undefined: _ } => return None,
        SqliteValue::Text(text) => text,
        SqliteValue::Integer(i) => bumpalo::format!(in arena, "{}", i).into_bump_str(),
        SqliteValue::Float(f) => {
            if f.is_nan() {
                "NaN"
            } else if f.is_infinite() {
                if f > 0.0 { "Infinity" } else { "-Infinity" }
            } else {
                arena.alloc_str(ryu_js::Buffer::new().format_finite(f))
            }
        }
        SqliteValue::Blob(blob) => match String::from_utf8_lossy(blob) {
            std::borrow::Cow::Borrowed(text) => text,
            std::borrow::Cow::Owned(text) => arena.alloc_str(&text),
        },
    })
}

/// Converts a value to an integer (`CAST(value AS INTEGER)` in TypeScript), returning [None] for
/// `NULL`.
pub fn cast_as_integer(value: SqliteValue<'_>) -> Option<i64> {
    Some(match value {
        SqliteValue::Null { is_undefined: _ } => return None,
        SqliteValue::Integer(i) => i,
        SqliteValue::Float(f) => {
            if f.is_nan() {
                return None;
            } else {
                // TypeScript throws here, this converts to i64::MAX or i64::MIN
                f.floor() as i64
            }
        }
        SqliteValue::Text(text) => integer_prefix(text),
        SqliteValue::Blob(blob) => integer_prefix(&String::from_utf8_lossy(blob)),
    })
}

/// Parses an integer prefix of `text` (`parseBigInt` in TypeScript), saturating on overflow.
fn integer_prefix(text: &str) -> i64 {
    let text = text.trim_start_matches(char::is_whitespace);
    let (negative, unsigned) = match text.as_bytes().first() {
        Some(b'-') => (true, &text[1..]),
        Some(b'+') => (false, &text[1..]),
        _ => (false, text),
    };

    let mut value = 0i64;
    for digit in unsigned.bytes().take_while(u8::is_ascii_digit) {
        let digit = i64::from(digit - b'0');
        // Accumulating negative values directly allows parsing i64::MIN.
        value = value.saturating_mul(10);
        value = if negative {
            value.saturating_sub(digit)
        } else {
            value.saturating_add(digit)
        };
    }

    value
}

/// Converts a value to a real (`CAST(value AS REAL)` in TypeScript), returning [None] for `NULL`.
///
/// Text is parsed like JavaScript's `parseFloat`: The longest prefix that is a decimal number (or
/// `Infinity`) after leading whitespace, or `0.0` if there is none. Blobs are decoded as UTF-8 and
/// parsed the same way. Integers are rounded to the nearest real.
pub fn cast_as_real(value: SqliteValue<'_>) -> Option<f64> {
    Some(match value {
        SqliteValue::Null { is_undefined: _ } => return None,
        SqliteValue::Float(f) => f,
        SqliteValue::Integer(i) => i as f64,
        SqliteValue::Text(text) => parse_float(text).unwrap_or(0.0),
        SqliteValue::Blob(blob) => parse_float(&String::from_utf8_lossy(blob)).unwrap_or(0.0),
    })
}

/// Converts a value to a number (`CAST(value AS NUMERIC)` in TypeScript).
///
/// Numbers are returned as-is. Text is parsed by taking the longest prefix that is a decimal
/// number after leading whitespace: It's an integer if it has neither a fractional part nor an
/// exponent, and a real otherwise. Text without such prefix is cast to the integer `0`. Blobs are
/// decoded as UTF-8 and parsed the same way.
///
/// Unlike TypeScript (which uses arbitrary-precision integers), integers outside of the range of a
/// 64-bit integer are returned as reals, like SQLite does.
pub fn cast_as_numeric(value: SqliteValue<'_>) -> Option<Numeric> {
    Some(match value {
        SqliteValue::Null { is_undefined: _ } => return None,
        SqliteValue::Integer(i) => Numeric::Integer(i),
        SqliteValue::Float(f) => Numeric::Float(f),
        SqliteValue::Text(text) => parse_numeric(text),
        SqliteValue::Blob(blob) => parse_numeric(&String::from_utf8_lossy(blob)),
    })
}

#[derive(Debug, Clone, Copy)]
pub enum Numeric {
    Integer(i64),
    Float(f64),
}

impl Numeric {
    pub fn as_real(self) -> f64 {
        match self {
            Numeric::Integer(i) => i as f64,
            Numeric::Float(f) => f,
        }
    }
}

impl From<Numeric> for SqliteValue<'static> {
    fn from(value: Numeric) -> Self {
        match value {
            Numeric::Integer(e) => SqliteValue::Integer(e),
            Numeric::Float(e) => SqliteValue::Float(e),
        }
    }
}

/// Converts a value to a blob (`castAsBlob` in TypeScript), returning [None] for `NULL`.
///
/// Values other than blobs are converted to text with [cast_as_text] and then encoded as UTF-8.
pub fn cast_as_blob<'a>(value: SqliteValue<'a>, arena: &'a Bump) -> Option<&'a [u8]> {
    match value {
        SqliteValue::Blob(blob) => Some(blob),
        other => cast_as_text(other, arena).map(str::as_bytes),
    }
}

/// Evaluates `CAST(value AS to)` (`cast` in TypeScript).
///
/// `NULL` is cast to `NULL` for all types.
pub fn cast<'a>(value: SqliteValue<'a>, to: CastType, arena: &'a Bump) -> SqliteValue<'a> {
    match to {
        CastType::Text => cast_as_text(value, arena).map_or(SqliteValue::null(), SqliteValue::Text),
        CastType::Numeric => cast_as_numeric(value).map_or(SqliteValue::null(), |n| n.into()),
        CastType::Real => cast_as_real(value).map_or(SqliteValue::null(), SqliteValue::Float),
        CastType::Integer => {
            cast_as_integer(value).map_or(SqliteValue::null(), SqliteValue::Integer)
        }
        CastType::Blob => cast_as_blob(value, arena).map_or(SqliteValue::null(), SqliteValue::Blob),
    }
}

/// Parses the longest prefix of `bytes` (after leading whitespace) that is a valid number, or
/// returns `0.0` if there is none.
///
/// This follows SQLite's rules for interpreting text as a number, e.g. to evaluate conditions.
pub fn numeric_prefix(bytes: &[u8]) -> f64 {
    let start = bytes
        .iter()
        .position(|b| !b.is_ascii_whitespace())
        .unwrap_or(bytes.len());
    let bytes = &bytes[start..];

    let (len, _) = decimal_prefix(bytes);
    parse_ascii(&bytes[..len]).unwrap_or(0.0)
}

/// Parses text like JavaScript's `parseFloat`, returning [None] where that returns `NaN`.
fn parse_float(text: &str) -> Option<f64> {
    let text = text.trim_start_matches(char::is_whitespace);

    let (sign, unsigned) = match text.as_bytes().first() {
        Some(b'-') => (-1.0, &text[1..]),
        Some(b'+') => (1.0, &text[1..]),
        _ => (1.0, text),
    };
    if unsigned.starts_with("Infinity") {
        return Some(sign * f64::INFINITY);
    }

    let (len, _) = decimal_prefix(text.as_bytes());
    parse_ascii(&text.as_bytes()[..len])
}

/// Parses text for `CAST(text AS NUMERIC)` (`parseNumeric` in TypeScript).
fn parse_numeric(text: &str) -> Numeric {
    let text = text.trim_start_matches(char::is_whitespace);
    let (len, is_integer) = decimal_prefix(text.as_bytes());
    let prefix = &text.as_bytes()[..len];

    if len == 0 {
        return Numeric::Integer(0);
    }

    if is_integer {
        // Parsing fails if the integer doesn't fit into an i64, in which case we fall back to a
        // real.
        if let Some(int) = std::str::from_utf8(prefix)
            .ok()
            .and_then(|s| s.parse().ok())
        {
            return Numeric::Integer(int);
        }
    }

    Numeric::Float(parse_ascii(prefix).unwrap_or(0.0))
}

/// Finds the longest prefix of `bytes` matching `[+-]?([0-9]+(\.[0-9]*)?|\.[0-9]+)([eE][+-]?[0-9]+)?`.
///
/// Returns the length of the prefix (`0` if there is none), and whether the number is an integer
/// (meaning that it has neither a decimal point nor an exponent).
fn decimal_prefix(bytes: &[u8]) -> (usize, bool) {
    let count_digits = |from: usize| {
        bytes
            .get(from..)
            .unwrap_or_default()
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
    let mut is_integer = true;

    if bytes.get(end) == Some(&b'.') {
        let fraction_digits = count_digits(end + 1);
        if fraction_digits > 0 || has_digits {
            end += 1 + fraction_digits;
            has_digits = true;
            is_integer = false;
        }
    }

    if !has_digits {
        return (0, true);
    }

    if matches!(bytes.get(end), Some(b'e' | b'E')) {
        let mut exponent_end = end + 1;
        if matches!(bytes.get(exponent_end), Some(b'+' | b'-')) {
            exponent_end += 1;
        }

        let exponent_digits = count_digits(exponent_end);
        if exponent_digits > 0 {
            end = exponent_end + exponent_digits;
            is_integer = false;
        }
    }

    (end, is_integer)
}

/// Parses a prefix found by [decimal_prefix] as a double.
fn parse_ascii(number: &[u8]) -> Option<f64> {
    // The prefix only consists of ASCII characters, so it's valid UTF-8.
    std::str::from_utf8(number).ok()?.parse().ok()
}
