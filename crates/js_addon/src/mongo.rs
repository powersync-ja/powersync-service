use napi::{
    Env, Error, Result,
    bindgen_prelude::{
        BigInt, BufferSlice, FromNapiValue, JsObjectValue, JsValue, Null, Object, ObjectFinalize,
        ObjectRef, ToNapiValue, Uint8Array, Unknown,
    },
    check_status, sys,
};
use napi_derive::napi;
use powersync_service::mongo::{self, ConversionOptions, DateRenderMode, SqliteValue};
use std::hash::{DefaultHasher, Hash, Hasher};

/// Experimental direct BSON converter. Replica identities are returned as a small
/// BSON document for decoding with the same Node BSON library as other converters.
#[napi(custom_finalize)]
pub struct MongoSourceRowConverter {
    options: ConversionOptions,
    shape: Option<RowShape>,
    pending_shape: Option<u64>,
}

struct RowShape {
    names: Vec<String>,
    keys: ObjectRef,
    bulk_safe: bool,
}

impl ObjectFinalize for MongoSourceRowConverter {
    fn finalize(self, env: Env) -> Result<()> {
        if let Some(shape) = self.shape {
            shape.keys.unref(&env)?;
        }
        Ok(())
    }
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
            shape: None,
            pending_shape: None,
        })
    }

    #[napi]
    pub fn raw_to_sqlite_row<'a>(
        &mut self,
        env: &'a Env,
        source: BufferSlice<'_>,
    ) -> Result<ConvertedMongoRow<'a>> {
        // BufferSlice borrows for this synchronous call without creating and
        // releasing the persistent reference needed by the async Buffer type.
        let converted =
            mongo::raw_to_sqlite_row(&source, self.options).map_err(Error::from_reason)?;
        let mut matches = self.shape.as_ref().is_some_and(|shape| {
            shape.names.len() == converted.row.len()
                && shape
                    .names
                    .iter()
                    .zip(&converted.row)
                    .all(|(previous, (key, _))| previous == key)
        });
        if matches {
            self.pending_shape = None;
        } else {
            // Retain only the last shape and cap the total key data. Heterogeneous
            // MongoDB documents must not grow an unbounded schema/key cache.
            if converted.row.len() <= 256
                && converted
                    .row
                    .iter()
                    .map(|(key, _)| key.len())
                    .sum::<usize>()
                    <= 16_384
            {
                // Warm a shape only after two consecutive occurrences. Alternating
                // documents should not allocate and discard a key cache per row.
                // A hash collision can only warm an extra shape; cache hits still
                // compare every key above before using any cached handles.
                let mut hash = DefaultHasher::new();
                for (key, _) in &converted.row {
                    key.hash(&mut hash);
                }
                let fingerprint = hash.finish();
                if self.pending_shape != Some(fingerprint) {
                    self.pending_shape = Some(fingerprint);
                } else {
                    let mut holder = Object::new(env)?;
                    let mut names = Vec::with_capacity(converted.row.len());
                    let mut bulk_safe = true;
                    for (i, (key, _)) in converted.row.iter().enumerate() {
                        bulk_safe &= !names.iter().any(|name| name == key);
                        let mut raw_key = std::ptr::null_mut();
                        check_status!(unsafe {
                            sys::node_api_create_property_key_utf8(
                                env.raw(),
                                key.as_ptr().cast(),
                                key.len() as isize,
                                &mut raw_key,
                            )
                        })?;
                        let key_value = unsafe { Unknown::from_napi_value(env.raw(), raw_key)? };
                        holder.set_element(i as u32, key_value)?;
                        names.push(key.to_string());
                    }
                    let shape = RowShape {
                        names,
                        keys: holder.create_ref()?,
                        bulk_safe,
                    };
                    if let Some(previous) = self.shape.replace(shape) {
                        previous.keys.unref(env)?;
                    }
                    self.pending_shape = None;
                    matches = true;
                }
            } else {
                self.pending_shape = None;
            }
        }
        let row = if let Some(shape) = self
            .shape
            .as_ref()
            .filter(|shape| matches && shape.bulk_safe)
        {
            let holder = shape.keys.get_value(env)?;
            let mut names = Vec::with_capacity(converted.row.len());
            let mut values = Vec::with_capacity(converted.row.len());
            for (i, (_, value)) in converted.row.into_iter().enumerate() {
                names.push(holder.get_element::<Unknown>(i as u32)?.raw());
                values.push(sqlite_value(env, value)?);
            }
            let prototype = holder.get_prototype()?;
            let mut raw_row = std::ptr::null_mut();
            // All handles belong to this call's scope; both arrays have the same
            // length and remain valid until the bulk constructor returns.
            check_status!(unsafe {
                sys::node_api_create_object_with_properties(
                    env.raw(),
                    prototype.raw(),
                    names.as_ptr(),
                    values.as_ptr(),
                    names.len(),
                    &mut raw_row,
                )
            })?;
            Object::from_raw(env.raw(), raw_row)
        } else {
            let row = Object::new(env)?;
            let mut properties = Vec::with_capacity(converted.row.len());
            for (key, value) in converted.row {
                properties.push(sys::napi_property_descriptor {
                    utf8name: std::ptr::null(),
                    name: env.create_string(&key)?.raw(),
                    method: None,
                    getter: None,
                    setter: None,
                    value: sqlite_value(env, value)?,
                    attributes: sys::PropertyAttributes::writable
                        | sys::PropertyAttributes::enumerable
                        | sys::PropertyAttributes::configurable,
                    data: std::ptr::null_mut(),
                });
            }
            // Define own data properties, including __proto__. Assignment would
            // invoke inherited setters and could change the row's prototype.
            check_status!(unsafe {
                sys::napi_define_properties(
                    env.raw(),
                    row.raw(),
                    properties.len(),
                    properties.as_ptr(),
                )
            })?;
            row
        };
        Ok(ConvertedMongoRow {
            row,
            // This document is small; a Node-owned copy avoids the external
            // buffer allocation and finalizer used when returning a Rust Vec.
            id_document: BufferSlice::copy_from(env, converted.id_document)?,
        })
    }
}

fn sqlite_value(env: &Env, value: SqliteValue<'_>) -> Result<sys::napi_value> {
    // Each branch constructs a new value in the current N-API handle scope.
    unsafe {
        match value {
            SqliteValue::Null => Null::to_napi_value(env.raw(), Null),
            SqliteValue::Integer(value) => {
                Integer::to_napi_value(env.raw(), Integer::Signed(value))
            }
            SqliteValue::Timestamp(value) => {
                Integer::to_napi_value(env.raw(), Integer::Unsigned(value))
            }
            // Integral BSON doubles are widened to BigInt, including doubles
            // outside the i64 range. Decode their exact IEEE-754 integer value.
            SqliteValue::Double(value) if value.is_finite() && value.fract() == 0.0 => {
                BigInt::to_napi_value(env.raw(), integer_double(value))
            }
            SqliteValue::Double(value) => f64::to_napi_value(env.raw(), value),
            SqliteValue::Text(value) => {
                // Avoid UTF-8 decoding at the N-API boundary for ASCII: the
                // Latin-1 entrypoint can copy it directly. For Unicode, decode in
                // Rust and pass UTF-16, avoiding V8's slower UTF-8 decoder.
                let value = if value.is_ascii() {
                    env.create_string_latin1(value.as_bytes())?
                } else {
                    env.create_string_utf16(value.encode_utf16().collect::<Vec<_>>())?
                };
                Ok(value.raw())
            }
            SqliteValue::Blob(value) => {
                Uint8Array::to_napi_value(env.raw(), Uint8Array::new(value.to_vec()))
            }
        }
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
