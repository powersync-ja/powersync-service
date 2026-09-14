//! Rust/SQLite source-row evaluator. JS values are copied before background work starts.
mod engine;
mod functions;

use engine::{Engine, Fields, Processor};
use napi::bindgen_prelude::*;
use napi_derive::napi;
use rusqlite::types::Value;
use std::sync::{Arc, Mutex};

type JsValue = Either5<String, f64, BigInt, Uint8Array, Null>;

#[napi(object)]
pub struct Field {
    pub name: String,
    pub value: JsValue,
}
#[napi(object)]
pub struct DataResult {
    pub source: u32,
    pub bucket: String,
    pub id: String,
    pub table: String,
    pub data: String,
}
#[napi(object)]
pub struct ParameterResult {
    pub source: u32,
    pub values: Vec<JsValue>,
    pub rows: Vec<Vec<Field>>,
}
#[napi(object)]
pub struct EvaluationError {
    pub kind: String,
    pub error: String,
}
#[napi(object)]
pub struct RowResult {
    pub data: Vec<DataResult>,
    pub parameters: Vec<ParameterResult>,
    pub errors: Vec<EvaluationError>,
}
#[napi(object)]
pub struct ExecutionMeasurement {
    pub execution_ms: f64,
    pub result_count: u32,
}

fn encode(value: JsValue) -> Result<Value> {
    Ok(match value {
        Either5::A(v) => Value::Text(v),
        Either5::B(v) => Value::Real(v),
        Either5::C(v) => {
            let (i, lossless) = if v.words.is_empty() {
                (0, true)
            } else {
                v.get_i64()
            };
            if !lossless {
                return Err(Error::from_reason(
                    "Integer input exceeds SQLite's signed 64-bit range",
                ));
            }
            Value::Integer(i)
        }
        // Copy: JS can mutate its Uint8Array after evaluateAsync returns. No borrowed backing
        // store may cross the native task boundary.
        Either5::D(v) => Value::Blob(v.to_vec()),
        Either5::E(_) => Value::Null,
    })
}

fn decode(value: Value) -> JsValue {
    match value {
        Value::Text(v) => Either5::A(v),
        Value::Real(v) => Either5::B(v),
        Value::Integer(v) => Either5::C(BigInt::from(v)),
        Value::Blob(v) => Either5::D(v.into()),
        Value::Null => Either5::E(Null),
    }
}
fn encode_rows(rows: Vec<Vec<Field>>) -> Result<Vec<Fields>> {
    rows.into_iter()
        .map(|row| {
            row.into_iter()
                .map(|field| Ok((field.name, encode(field.value)?)))
                .collect()
        })
        .collect()
}
fn decode_fields(fields: Fields) -> Vec<Field> {
    fields
        .into_iter()
        .map(|(name, value)| Field {
            name,
            value: decode(value),
        })
        .collect()
}
fn decode_rows(rows: Vec<engine::RowResult>) -> Vec<RowResult> {
    rows.into_iter()
        .map(|r| RowResult {
            data: r
                .data
                .into_iter()
                .map(|d| DataResult {
                    source: d.source,
                    bucket: d.bucket,
                    id: d.id,
                    table: d.table,
                    data: d.data,
                })
                .collect(),
            parameters: r
                .parameters
                .into_iter()
                .map(|p| ParameterResult {
                    source: p.source,
                    values: p.values.into_iter().map(decode).collect(),
                    rows: p.rows.into_iter().map(decode_fields).collect(),
                })
                .collect(),
            errors: r
                .errors
                .into_iter()
                .map(|(kind, error)| EvaluationError {
                    kind: if kind == engine::Kind::Data {
                        "data"
                    } else {
                        "parameters"
                    }
                    .into(),
                    error,
                })
                .collect(),
        })
        .collect()
}

#[napi]
pub struct NativeEvaluator {
    engine: Arc<Mutex<Engine>>,
}

#[napi]
impl NativeEvaluator {
    #[napi(constructor)]
    pub fn new(plan: String) -> Result<Self> {
        let processors: Vec<Processor> =
            serde_json::from_str(&plan).map_err(|e| Error::from_reason(e.to_string()))?;
        let engine = Engine::new(processors).map_err(Error::from_reason)?;
        Ok(Self {
            engine: Arc::new(Mutex::new(engine)),
        })
    }
    #[napi]
    pub fn evaluate(&self, rows: Vec<Vec<Field>>) -> Result<Vec<RowResult>> {
        let rows = encode_rows(rows)?;
        let engine = self
            .engine
            .lock()
            .map_err(|_| Error::from_reason("Evaluator lock poisoned"))?;
        Ok(decode_rows(engine.evaluate(&rows)))
    }
    #[napi]
    pub fn evaluate_async(&self, rows: Vec<Vec<Field>>) -> Result<AsyncTask<EvaluationTask>> {
        Ok(AsyncTask::new(EvaluationTask {
            engine: self.engine.clone(),
            rows: encode_rows(rows)?,
        }))
    }
    /// Diagnostic benchmark only: excludes input conversion and output reconstruction.
    #[napi]
    pub fn measure_execution(
        &self,
        rows: Vec<Vec<Field>>,
        iterations: u32,
    ) -> Result<ExecutionMeasurement> {
        if iterations == 0 {
            return Err(Error::from_reason("iterations must be positive"));
        }
        let rows = encode_rows(rows)?;
        let engine = self
            .engine
            .lock()
            .map_err(|_| Error::from_reason("Evaluator lock poisoned"))?;
        let start = std::time::Instant::now();
        let mut result_count: u32 = 0;
        for _ in 0..iterations {
            for row in engine.evaluate(&rows) {
                if !row.errors.is_empty() {
                    return Err(Error::from_reason("Benchmark generated evaluation errors"));
                }
                result_count =
                    result_count.wrapping_add((row.data.len() + row.parameters.len()) as u32);
                std::hint::black_box(row);
            }
        }
        Ok(ExecutionMeasurement {
            execution_ms: start.elapsed().as_secs_f64() * 1000.0,
            result_count,
        })
    }
}

pub struct EvaluationTask {
    engine: Arc<Mutex<Engine>>,
    rows: Vec<Fields>,
}
impl Task for EvaluationTask {
    type Output = Vec<engine::RowResult>;
    type JsValue = Vec<RowResult>;
    fn compute(&mut self) -> Result<Self::Output> {
        let engine = self
            .engine
            .lock()
            .map_err(|_| Error::from_reason("Evaluator lock poisoned"))?;
        Ok(engine.evaluate(&self.rows))
    }
    fn resolve(&mut self, _env: Env, output: Self::Output) -> Result<Self::JsValue> {
        Ok(decode_rows(output))
    }
}

#[napi]
pub fn sqlite_version() -> String {
    rusqlite::version().into()
}

// Node test workers do not necessarily run a shared library's C atexit handlers. The
// instrumented test build explicitly flushes counters before the worker is terminated.
#[cfg(native_coverage)]
#[napi]
pub fn flush_coverage() {
    unsafe extern "C" {
        fn __llvm_profile_write_file() -> i32;
    }
    // SAFETY: this symbol is supplied by rustc's instrument-coverage runtime and this
    // function only exists in an instrumented build. It takes no pointers or arguments.
    unsafe {
        __llvm_profile_write_file();
    }
}
