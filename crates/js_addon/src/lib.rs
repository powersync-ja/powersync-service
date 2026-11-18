use napi::Result;
use napi::bindgen_prelude::Object;
use napi_derive::napi;
use powersync_service::PowerSyncService;

mod log;

#[napi(object)]
pub struct RustInitializationOptions<'a> {
    /// The winston logger object to use.
    pub logger: Object<'a>,
}

pub fn initialize(
    options: RustInitializationOptions,
    embedder_name: &str,
) -> Result<PowerSyncService> {
    log::configure_logger(options.logger)?;

    let service = PowerSyncService::init(embedder_name);
    Ok(service)
}
