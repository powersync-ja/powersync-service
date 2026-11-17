use napi::{
    Env, Error, Result,
    bindgen_prelude::{BigInt, Buffer, Null, Object, Uint8Array},
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
    pub id_document: Buffer,
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
        source: Buffer,
    ) -> Result<ConvertedMongoRow<'a>> {
        let converted =
            mongo::raw_to_sqlite_row(&source, self.options).map_err(Error::from_reason)?;
        let mut row = Object::new(env)?;
        for (key, value) in converted.row {
            match value {
                SqliteValue::Null => row.set(key, Null)?,
                SqliteValue::Integer(value) => row.set(key, BigInt::from(value))?,
                SqliteValue::Timestamp(value) => row.set(key, BigInt::from(value))?,
                // Integral BSON doubles are widened to BigInt, including doubles
                // outside the i64 range. Decode their exact IEEE-754 integer value.
                SqliteValue::Double(value) if value.is_finite() && value.fract() == 0.0 => {
                    row.set(key, integer_double(value))?
                }
                SqliteValue::Double(value) => row.set(key, value)?,
                SqliteValue::Text(value) => row.set(key, value)?,
                SqliteValue::Blob(value) => row.set(key, Uint8Array::new(value))?,
            }
        }
        Ok(ConvertedMongoRow {
            row,
            id_document: converted.id_document.into(),
        })
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
