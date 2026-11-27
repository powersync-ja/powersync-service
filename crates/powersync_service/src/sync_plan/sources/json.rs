use bumpalo::Bump;
use serde::de::{Deserialize, DeserializeSeed, Visitor};
use serde_json::value::RawValue;

use crate::sync_plan::{SqliteValue, evaluation::Frame};

pub struct SqliteValueFromJson<'a> {
    arena: &'a Bump,
    /// Mirroring the behavior of `jsonValueToSqlite` in `@powersync/sync-rules`, this controls
    /// whether a `null` JSON value is turned into [SqliteValue::Null] or `'null'`.
    fixed_json_extract: bool,
}

impl<'a> From<&Frame<'a>> for SqliteValueFromJson<'a> {
    fn from(value: &Frame<'a>) -> Self {
        Self {
            arena: value.arena,
            fixed_json_extract: value.compatibility.fixed_json_extract,
        }
    }
}

impl<'a> SqliteValueFromJson<'a> {
    /// A converter for top-level values of a source row.
    ///
    /// Unlike values from `json_extract` or `json_each`, `null` values of the source row are always
    /// [SqliteValue::Null], regardless of the `fixed_json_extract` option.
    pub fn for_source_row(frame: &Frame<'a>) -> Self {
        Self {
            arena: frame.arena,
            fixed_json_extract: true,
        }
    }

    /// Converts a string known to hold a valid JSON value to a SQLite value.
    ///
    /// This avoids allocating where possible, but allocations are required for JSON strings with
    /// escape sequences.
    pub fn map_validated_json(self, json: &'a RawValue) -> SqliteValue<'a> {
        match ScalarSqliteValueOrFail(self).deserialize(json) {
            Ok(value) => value,
            Err(_) => {
                // We know we started with a valid JSON source, so the only way for this to fail is
                // if the value describes an object or array. Instead of decoding and encoding
                // again, report the value directly.
                SqliteValue::Text(json.get())
            }
        }
    }

    fn null_value(&self) -> SqliteValue<'a> {
        if self.fixed_json_extract {
            SqliteValue::Null {
                is_undefined: false,
            }
        } else {
            SqliteValue::Text("null")
        }
    }
}

/// Decodes a scalar SQLite from JSON, or fails.
pub struct ScalarSqliteValueOrFail<'a>(pub SqliteValueFromJson<'a>);

impl<'a> DeserializeSeed<'a> for ScalarSqliteValueOrFail<'a> {
    type Value = SqliteValue<'a>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'a>,
    {
        deserializer.deserialize_any(self.0)
    }
}

/// Decodes a scalar SQLite value from JSON.
///
/// Arrays and objects are reported as unparsed JSON text.
pub struct ScalarSqliteValueOrText<'a>(pub SqliteValueFromJson<'a>);

impl<'a> DeserializeSeed<'a> for ScalarSqliteValueOrText<'a> {
    type Value = SqliteValue<'a>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'a>,
    {
        let validated: &'a RawValue = Deserialize::deserialize(deserializer)?;
        Ok(self.0.map_validated_json(validated))
    }
}

impl<'a> Visitor<'a> for SqliteValueFromJson<'a> {
    type Value = SqliteValue<'a>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "any JSON value")
    }

    fn visit_borrowed_str<E>(self, v: &'a str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(SqliteValue::Text(v))
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        // Copy the string into the arena
        let allocated = self.arena.alloc_str(v);
        Ok(SqliteValue::Text(allocated))
    }

    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(SqliteValue::Integer(v))
    }

    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(if let Ok(i64) = i64::try_from(v) {
            SqliteValue::Integer(i64)
        } else {
            SqliteValue::Float(v as f64)
        })
    }

    fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(SqliteValue::Float(v))
    }

    fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(SqliteValue::Integer(if v { 1 } else { 0 }))
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(self.null_value())
    }

    fn visit_none<E>(self) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(self.null_value())
    }
}

#[cfg(test)]
mod test {
    use bumpalo::Bump;
    use serde::de::DeserializeSeed;
    use serde_json::value::RawValue;

    use super::{ScalarSqliteValueOrFail, ScalarSqliteValueOrText, SqliteValueFromJson};
    use crate::sync_plan::SqliteValue;

    fn source(arena: &Bump, fixed_json_extract: bool) -> SqliteValueFromJson<'_> {
        SqliteValueFromJson {
            arena,
            fixed_json_extract,
        }
    }

    fn parse<'a, S: DeserializeSeed<'a>>(
        seed: S,
        json: &'a str,
    ) -> Result<S::Value, serde_json::Error> {
        let mut deserializer = serde_json::Deserializer::from_str(json);
        let value = seed.deserialize(&mut deserializer)?;
        deserializer.end()?;
        Ok(value)
    }

    #[test]
    fn scalars() {
        use SqliteValue::*;

        let arena = Bump::new();
        let scalar = |json| parse(ScalarSqliteValueOrFail(source(&arena, true)), json).unwrap();

        assert_eq!(scalar("42"), Integer(42));
        assert_eq!(scalar("-42"), Integer(-42));
        assert_eq!(scalar("9223372036854775807"), Integer(i64::MAX));
        assert_eq!(scalar("-9223372036854775808"), Integer(i64::MIN));
        assert_eq!(
            scalar("18446744073709551615"),
            Float(18446744073709551615.0)
        );
        assert_eq!(scalar("2.5"), Float(2.5));
        assert_eq!(scalar("1e300"), Float(1e300));
        assert_eq!(scalar("true"), Integer(1));
        assert_eq!(scalar("false"), Integer(0));
        assert_eq!(
            scalar("null"),
            Null {
                is_undefined: false
            }
        );
        assert_eq!(scalar(r#""str""#), Text("str"));
        assert_eq!(scalar(r#""""#), Text(""));
        assert_eq!(scalar(r#""a\n\"b\"ä""#), Text("a\n\"b\"ä"));
    }

    #[test]
    fn strings_without_escapes_are_borrowed() {
        let arena = Bump::new();
        let json = r#""borrowed""#;
        let SqliteValue::Text(text) =
            parse(ScalarSqliteValueOrFail(source(&arena, true)), json).unwrap()
        else {
            panic!("expected text");
        };

        assert_eq!(text, "borrowed");
        assert!(json.as_bytes().as_ptr_range().contains(&text.as_ptr()));
        assert_eq!(arena.allocated_bytes(), 0);
    }

    #[test]
    fn legacy_null() {
        let arena = Bump::new();
        assert_eq!(
            parse(ScalarSqliteValueOrFail(source(&arena, false)), "null").unwrap(),
            SqliteValue::Text("null")
        );
        assert_eq!(
            parse(ScalarSqliteValueOrText(source(&arena, false)), "null").unwrap(),
            SqliteValue::Text("null")
        );
    }

    #[test]
    fn nested_values_fail() {
        let arena = Bump::new();

        for json in ["[]", "[1, 2]", "{}", r#"{"a": 1}"#, r#"[{"a": [null]}]"#] {
            assert!(
                parse(ScalarSqliteValueOrFail(source(&arena, true)), json).is_err(),
                "{json}"
            );
        }
    }

    #[test]
    fn nested_values_as_text() {
        use SqliteValue::*;

        let arena = Bump::new();
        let parse_or_text =
            |json| parse(ScalarSqliteValueOrText(source(&arena, true)), json).unwrap();

        assert_eq!(parse_or_text("42"), Integer(42));
        assert_eq!(parse_or_text(r#""a\nb""#), Text("a\nb"));
        assert_eq!(
            parse_or_text("null"),
            Null {
                is_undefined: false
            }
        );

        for json in [
            "[]",
            "{}",
            "[1, 2.0, 1e300]",
            r#"{"z": [1, "\u0001\"\n"], "a": {} }"#,
            r#"[ null , true ]"#,
        ] {
            // Nested values are reported as-is, without normalizing whitespace or escapes.
            assert_eq!(parse_or_text(json), Text(json));
        }
    }

    #[test]
    fn from_validated_json() {
        use SqliteValue::*;

        let arena = Bump::new();
        let validated = |json| {
            let raw: &RawValue = serde_json::from_str(json).unwrap();
            source(&arena, true).map_validated_json(raw)
        };

        assert_eq!(validated("1.5"), Float(1.5));
        assert_eq!(validated(r#""\"quoted\"""#), Text("\"quoted\""));
        assert_eq!(validated(r#"{"a": [1]}"#), Text(r#"{"a": [1]}"#));
    }
}
