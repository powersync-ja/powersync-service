use powersync_service::PowerSyncService;

use napi::Result;
use napi::bindgen_prelude::External;
use napi_derive::napi;
use powersync_service_js_addon_shared::{RustInitializationOptions, initialize};

#[napi]
pub fn initialize_rust(options: RustInitializationOptions) -> Result<External<PowerSyncService>> {
    let service = initialize(options, "PowerSync Open Edition")?;

    Ok(External::new(service))
}
