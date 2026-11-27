use std::ptr;

use bumpalo::Bump;
use napi::{
    JsString, JsValue, Unknown, ValueType,
    bindgen_prelude::{BigInt, Null, ToNapiValue, Uint8Array, Uint8ArraySlice, check_status},
    sys,
};
use powersync_service::sync_plan::SqliteValue;

/// Converts a JavaScript `SqliteValue` into a [SqliteValue].
///
/// This is only used for testing, a real integration decodes directly from BSON in Rust.
pub fn js_to_sqlite_value<'a>(
    column: &str,
    value: Unknown<'_>,
    arena: &'a Bump,
) -> napi::Result<SqliteValue<'a>> {
    Ok(match value.get_type()? {
        ValueType::Null => SqliteValue::null(),
        ValueType::String => {
            // Safety: We've checked the type.
            let string = unsafe { value.cast::<JsString>() }?.into_utf8()?;
            SqliteValue::Text(arena.alloc_str(string.as_str()?))
        }
        ValueType::Number => SqliteValue::Float(unsafe { value.cast::<f64>() }?),
        ValueType::BigInt => {
            let (value, lossless) = unsafe { value.cast::<BigInt>() }?.get_i64();
            if !lossless {
                panic!("bigint in column {column} doesn't fit into a 64-bit integer");
            }
            SqliteValue::Integer(value)
        }
        ValueType::Object if value.is_typedarray()? => {
            let bytes = unsafe { value.cast::<Uint8ArraySlice>() }?;
            SqliteValue::Blob(arena.alloc_slice_copy(&bytes))
        }
        other => panic!("Unsupported value of type {other} in column {column}"),
    })
}

/// Serializes a [SqliteValue] as a JavaScript value.
pub struct SqliteValueToJs<'a>(pub SqliteValue<'a>);

impl<'a> ToNapiValue for SqliteValueToJs<'a> {
    unsafe fn to_napi_value(env: sys::napi_env, val: Self) -> napi::Result<sys::napi_value> {
        match val.0 {
            SqliteValue::Null { is_undefined: _ } => unsafe {
                ToNapiValue::to_napi_value(env, Null)
            },
            SqliteValue::Blob(bytes) => {
                let value = Uint8Array::with_data_copied(bytes);
                unsafe { ToNapiValue::to_napi_value(env, value) }
            }
            SqliteValue::Float(f) => unsafe { ToNapiValue::to_napi_value(env, f) },
            SqliteValue::Integer(i) => {
                let mut raw_value = ptr::null_mut();
                check_status!(
                    unsafe { sys::napi_create_bigint_int64(env, i, &mut raw_value) },
                    "Failed to get reference value"
                )?;

                Ok(raw_value)
            }
            SqliteValue::Text(s) => unsafe { ToNapiValue::to_napi_value(env, s) },
        }
    }
}
