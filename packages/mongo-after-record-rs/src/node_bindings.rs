use napi::bindgen_prelude::{Buffer, Null, Object};
use napi::Env;
use napi_derive::napi;

use crate::converter::{self, FlatValue};

#[napi]
pub struct NativeMongoAfterRecordConverter;

#[napi]
impl NativeMongoAfterRecordConverter {
    #[napi(constructor)]
    pub fn new() -> Self {
        Self
    }

    #[napi(js_name = "constructAfterRecordJson")]
    pub fn construct_after_record_json(&self, bson_bytes: Buffer) -> napi::Result<String> {
        converter::construct_after_record_json(&bson_bytes)
            .map_err(|err| napi::Error::from_reason(err.to_string()))
    }

    #[napi(js_name = "constructAfterRecordObject")]
    pub fn construct_after_record_object(
        &self,
        env: Env,
        bson_bytes: Buffer,
    ) -> napi::Result<Object> {
        let mut object = env.create_object()?;
        let document = bson::raw::RawDocument::from_bytes(&bson_bytes[..])
            .map_err(|err| napi::Error::from_reason(err.to_string()))?;

        for element in document {
            let (key, value) = element.map_err(|err| napi::Error::from_reason(err.to_string()))?;
            let value = converter::convert_top_level_raw_value(value)
                .map_err(|err| napi::Error::from_reason(err.to_string()))?;
            match value {
                FlatValue::Null => object.set(&key, Null)?,
                FlatValue::String(v) => object.set(&key, v)?,
                FlatValue::Number(v) => object.set(&key, v)?,
                FlatValue::BigInt(v) => object.set(&key, env.create_bigint_from_i64(v)?)?,
                FlatValue::Bytes(v) => object.set(&key, Buffer::from(v))?,
            }
        }
        Ok(object)
    }
}
