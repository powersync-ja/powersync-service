use napi::{Result, bindgen_prelude::External};
use napi_derive::napi;
use powersync_service::sync_plan::{
    DeserializeSyncPlan, StreamDataSource, StreamParameterIndexLookupCreator,
};

#[napi]
pub fn deserialize_stream_data_source(
    default_schema: String,
    json: String,
) -> Result<External<StreamDataSource>> {
    let deserializer = DeserializeSyncPlan {
        default_schema: &default_schema,
    };
    let source = deserializer.data_source(&mut serde_json::Deserializer::from_str(&json))?;

    Ok(External::new(source))
}

#[napi]
pub fn deserialize_stream_parameter_lookup_creator(
    default_schema: String,
    json: String,
) -> Result<External<StreamParameterIndexLookupCreator>> {
    let deserializer = DeserializeSyncPlan {
        default_schema: &default_schema,
    };
    let source = deserializer.parameter_source(&mut serde_json::Deserializer::from_str(&json))?;

    Ok(External::new(source))
}
