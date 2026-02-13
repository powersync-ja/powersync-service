use napi_derive::napi;

use crate::{compile_sync_plan, CompileOptions};

#[napi(object)]
pub struct JsCompileOptions {
    pub default_schema: Option<String>,
}

#[napi(js_name = "compileSyncPlanJson")]
pub fn compile_sync_plan_json(
    yaml: String,
    options: Option<JsCompileOptions>,
) -> napi::Result<String> {
    let compiled = compile_sync_plan(
        &yaml,
        CompileOptions {
            default_schema: options
                .and_then(|value| value.default_schema)
                .unwrap_or_else(|| "public".to_string()),
        },
    )
    .map_err(|err| napi::Error::from_reason(err.to_string()))?;

    Ok(compiled.plan_json)
}
