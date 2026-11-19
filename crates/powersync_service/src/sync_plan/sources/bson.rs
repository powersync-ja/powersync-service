use bson::{RawBsonRef, error::Error};
use bumpalo::Bump;

use crate::sync_plan::{CompatibilityContext, SqliteValue, evaluator::Frame};

/// Rust port of `DirectSourceRowConverter`, using the bson crate.
#[derive(Clone, Copy)]
pub struct SqliteValueFromBson<'a> {
    _arena: &'a Bump,
    _options: &'a CompatibilityContext,
}

impl<'a> From<&Frame<'a>> for SqliteValueFromBson<'a> {
    fn from(value: &Frame<'a>) -> Self {
        Self {
            _arena: value.arena(),
            _options: value.compatibility(),
        }
    }
}

impl<'a> SqliteValueFromBson<'a> {
    pub fn parse_top_level_value(&self, value: RawBsonRef<'a>) -> Result<SqliteValue<'a>, Error> {
        Ok(match value {
            RawBsonRef::Double(d) => {
                // i64::MIN is exactly representable, i64::MAX as f64 rounds up to 2^63.
                if d.fract() == 0.0 && (i64::MIN as f64..i64::MAX as f64).contains(&d) {
                    // Match the default path: integral doubles are widened to bigint. Values out
                    // of range for an i64 stay floats instead of saturating.
                    SqliteValue::Integer(d as i64)
                } else {
                    SqliteValue::Float(d)
                }
            }
            RawBsonRef::Boolean(b) => SqliteValue::Integer(if b { 1 } else { 0 }),
            RawBsonRef::Int32(v) => SqliteValue::Integer(v as i64),
            RawBsonRef::Int64(v) => SqliteValue::Integer(v),
            RawBsonRef::String(v) => SqliteValue::Text(v),
            RawBsonRef::Null | RawBsonRef::Undefined | RawBsonRef::MaxKey | RawBsonRef::MinKey => {
                SqliteValue::Null
            }

            // TODO: Support these before using the Rust evaluator anywhere, the current BSON parser
            // is just a proof of concept.
            RawBsonRef::Array(_) => todo!(),
            RawBsonRef::Document(_) => todo!(),
            RawBsonRef::RegularExpression(_) => todo!(),
            RawBsonRef::JavaScriptCode(_) => todo!(),
            RawBsonRef::JavaScriptCodeWithScope(_) => todo!(),
            RawBsonRef::Timestamp(_) => todo!(),
            RawBsonRef::Binary(_) => todo!(),
            RawBsonRef::ObjectId(_) => todo!(),
            RawBsonRef::DateTime(_) => todo!(),
            RawBsonRef::Symbol(_) => todo!(),
            RawBsonRef::Decimal128(_) => todo!(),
            RawBsonRef::DbPointer(_) => todo!(),
        })
    }
}

#[cfg(test)]
mod test {
    use bson::RawBsonRef;
    use bumpalo::Bump;

    use super::SqliteValueFromBson;
    use crate::sync_plan::{CompatibilityContext, SqliteValue};

    #[test]
    fn doubles() {
        use SqliteValue::*;

        let arena = Bump::new();
        let options = CompatibilityContext::default();
        let source = SqliteValueFromBson {
            _arena: &arena,
            _options: &options,
        };
        let parse = |d| source.parse_top_level_value(RawBsonRef::Double(d)).unwrap();

        assert_eq!(parse(1.5), Float(1.5));
        assert_eq!(parse(-3.0), Integer(-3));
        assert_eq!(parse(-9223372036854775808.0), Integer(i64::MIN));
        // Out of range for i64, these must not saturate.
        assert_eq!(parse(9223372036854775808.0), Float(9223372036854775808.0));
        assert_eq!(parse(1e20), Float(1e20));
        assert_eq!(parse(-1e20), Float(-1e20));
        assert_eq!(parse(f64::INFINITY), Float(f64::INFINITY));
    }
}
