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
                if d.fract() == 0.0 {
                    // Match the default path: integral doubles are widened to bigint.
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
