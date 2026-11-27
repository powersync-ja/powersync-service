use powersync_service::PowerSyncService;

use napi::Result;
use napi::bindgen_prelude::External;
use napi_derive::napi;
use powersync_service_js_addon_shared::{RustInitializationOptions, initialize};

pub use powersync_service_js_addon_shared::evaluator::SyncStreamEvaluator;
pub use powersync_service_js_addon_shared::plan::{ProcessorGroup, SourceTable};
pub use powersync_service_js_addon_shared::plan::{
    deserialize_stream_data_source, deserialize_stream_parameter_lookup_creator,
};

#[napi]
pub fn initialize_rust(options: RustInitializationOptions) -> Result<External<PowerSyncService>> {
    let service = initialize(options, "PowerSync Open Edition")?;

    Ok(External::new(service))
}
