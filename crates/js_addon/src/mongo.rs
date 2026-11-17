use napi::{
    Env, Error, Result,
    bindgen_prelude::{BigInt, BufferSlice, JsObjectValue, Null, Object, ToNapiValue, Uint8Array},
    check_status, sys,
};
use napi_derive::napi;
use powersync_service::mongo::{self, ConversionOptions, DateRenderMode, SqliteValue};

/// Experimental direct BSON converter. Replica identities are returned as a small
/// BSON document for decoding with the same Node BSON library as other converters.
#[napi]
pub struct MongoSourceRowConverter {
    options: ConversionOptions,
}

#[napi(object)]
pub struct ConvertedMongoRow<'a> {
    pub row: Object<'a>,
    pub id_document: BufferSlice<'a>,
}

#[napi]
impl MongoSourceRowConverter {
    #[napi(constructor)]
    pub fn new(date_render_mode: u32, fixed_booleans_in_json: bool) -> Result<Self> {
        let date_render_mode = match date_render_mode {
            0 => DateRenderMode::LegacyMilliseconds,
            1 => DateRenderMode::IsoMilliseconds,
            2 => DateRenderMode::IsoSeconds,
            _ => return Err(Error::from_reason("Invalid date render mode")),
        };
        Ok(Self {
            options: ConversionOptions {
                date_render_mode,
                fixed_booleans_in_json,
            },
        })
    }

    #[napi]
    pub fn raw_to_sqlite_row<'a>(
        &self,
        env: &'a Env,
        source: BufferSlice<'_>,
    ) -> Result<ConvertedMongoRow<'a>> {
        // BufferSlice borrows for this synchronous call without creating and
        // releasing the persistent reference needed by the async Buffer type.
        let converted =
            mongo::raw_to_sqlite_row(&source, self.options).map_err(Error::from_reason)?;
        let mut row = Object::new(env)?;
        for (key, value) in converted.row {
            match value {
                SqliteValue::Null => row.set_named_property(&key, Null)?,
                SqliteValue::Integer(value) => {
                    row.set_named_property(&key, Integer::Signed(value))?
                }
                SqliteValue::Timestamp(value) => {
                    row.set_named_property(&key, Integer::Unsigned(value))?
                }
                // Integral BSON doubles are widened to BigInt, including doubles
                // outside the i64 range. Decode their exact IEEE-754 integer value.
                SqliteValue::Double(value) if value.is_finite() && value.fract() == 0.0 => {
                    row.set_named_property(&key, integer_double(value))?
                }
                SqliteValue::Double(value) => row.set_named_property(&key, value)?,
                SqliteValue::Text(value) => {
                    // Avoid UTF-8 decoding at the N-API boundary for ASCII: the
                    // Latin-1 entrypoint can copy it directly. For Unicode, decode in
                    // Rust and pass UTF-16, avoiding V8's slower UTF-8 decoder.
                    let value = if value.is_ascii() {
                        env.create_string_latin1(value.as_bytes())?
                    } else {
                        env.create_string_utf16(value.encode_utf16().collect::<Vec<_>>())?
                    };
                    row.set_named_property(&key, value)?;
                }
                SqliteValue::Blob(value) => {
                    row.set_named_property(&key, Uint8Array::new(value.to_vec()))?
                }
            }
        }
        Ok(ConvertedMongoRow {
            row,
            // This document is small; a Node-owned copy avoids the external
            // buffer allocation and finalizer used when returning a Rust Vec.
            id_document: BufferSlice::copy_from(env, converted.id_document)?,
        })
    }
}

enum Integer {
    Signed(i64),
    Unsigned(u64),
}

impl ToNapiValue for Integer {
    unsafe fn to_napi_value(env: sys::napi_env, value: Self) -> Result<sys::napi_value> {
        // BigInt::from allocates a Vec of words. BSON integers fit in a scalar,
        // so use the N-API scalar constructors without that allocation.
        let mut result = std::ptr::null_mut();
        check_status!(unsafe {
            match value {
                Self::Signed(value) => sys::napi_create_bigint_int64(env, value, &mut result),
                Self::Unsigned(value) => sys::napi_create_bigint_uint64(env, value, &mut result),
            }
        })?;
        Ok(result)
    }
}

fn integer_double(value: f64) -> BigInt {
    if value == 0.0 {
        return BigInt::from(0i64);
    }
    let bits = value.to_bits();
    let exponent = ((bits >> 52) & 0x7ff) as i32 - 1023;
    let mantissa = (bits & ((1u64 << 52) - 1)) | (1u64 << 52);
    if exponent <= 52 {
        return BigInt {
            sign_bit: value < 0.0,
            words: vec![mantissa >> (52 - exponent)],
        };
    }
    let shift = (exponent - 52) as usize;
    let mut words = vec![0; shift / 64 + 2];
    words[shift / 64] = mantissa << (shift % 64);
    if !shift.is_multiple_of(64) {
        words[shift / 64 + 1] = mantissa >> (64 - shift % 64);
    }
    BigInt {
        sign_bit: value < 0.0,
        words,
    }
}
