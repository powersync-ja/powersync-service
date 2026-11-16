use std::sync::RwLock;

use log::{
    Level, LevelFilter, Log, Metadata, Record,
    kv::{Error as KvError, Key, Value, VisitSource},
};
use napi::{
    Env, Result, Status,
    bindgen_prelude::{Function, JsObjectValue, Object, ToNapiValue, Unknown},
    sys,
    threadsafe_function::{ThreadsafeFunction, ThreadsafeFunctionCallMode},
};

/// A weak threadsafe reference to `Winston.prototype.log`, bound to a logger passed to the Rust
/// module.
///
/// We use threadsafe functions because this is stored in a global field accessible from all Rust
/// threads, and a weak reference because this shouldn't keep the Node event loop alive.
type WinstonLogFn = ThreadsafeFunction<LogEntry, Unknown<'static>, LogEntry, Status, false, true>;

static WINSTON_LOGGER: RwLock<Option<WinstonLogFn>> = RwLock::new(None);

/// Forwards events from the `log` crate to a winston logger.
///
/// This may be called multiple times, e.g. to replace the logger or to pick up a changed log level:
/// Since checking whether a level is enabled has to happen synchronously on the thread emitting a
/// log event, we copy the current level of the winston logger into [log::set_max_level] here.
pub fn configure_logger(logger: Object) -> Result<()> {
    let max_level = winston_level_filter(&logger.get_named_property::<String>("level")?);

    let log: Function<LogEntry, Unknown<'static>> = logger.get_named_property("log")?;
    let log = log
        .bind(logger)?
        .build_threadsafe_function::<LogEntry>()
        .weak::<true>()
        .build()?;

    // Dropping the previous function (if any) releases it.
    *WINSTON_LOGGER.write().unwrap_or_else(|e| e.into_inner()) = Some(log);

    // This only fails if a logger has already been installed, which is fine since we always install
    // the same forwarder.
    let _ = log::set_logger(&WinstonForwarder);
    log::set_max_level(max_level);
    Ok(())
}

/// Maps a winston level (using the default npm levels) to the most verbose `log` level it enables.
fn winston_level_filter(level: &str) -> LevelFilter {
    match level {
        "error" => LevelFilter::Error,
        "warn" => LevelFilter::Warn,
        "info" | "http" => LevelFilter::Info,
        "verbose" | "debug" => LevelFilter::Debug,
        // For "silly" and custom levels, forward everything and let winston filter.
        _ => LevelFilter::Trace,
    }
}

struct WinstonForwarder;

impl Log for WinstonForwarder {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= log::max_level()
    }

    fn log(&self, record: &Record) {
        if !self.enabled(record.metadata()) {
            return;
        }

        let guard = WINSTON_LOGGER.read().unwrap_or_else(|e| e.into_inner());
        let Some(log) = guard.as_ref() else {
            return;
        };

        let mut fields = FieldCollector(vec![]);
        // Our visitor never fails.
        let _ = record.key_values().visit(&mut fields);

        let entry = LogEntry {
            level: match record.level() {
                Level::Error => "error",
                Level::Warn => "warn",
                Level::Info => "info",
                Level::Debug => "debug",
                Level::Trace => "silly",
            },
            message: match record.args().as_str() {
                Some(message) => message.to_owned(),
                None => record.args().to_string(),
            },
            target: record.target().to_owned(),
            fields: fields.0,
        };

        // A non-blocking call on an unbounded queue only fails when the function is being released
        // (e.g. during shutdown), in which case there's nothing useful we could do with the event.
        let _ = log.call(entry, ThreadsafeFunctionCallMode::NonBlocking);
    }

    fn flush(&self) {}
}

/// A log event, sent to the JavaScript thread and passed to winston as an info object.
struct LogEntry {
    level: &'static str,
    message: String,
    target: String,
    fields: Vec<(String, FieldValue)>,
}

enum FieldValue {
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
}

impl ToNapiValue for LogEntry {
    unsafe fn to_napi_value(raw_env: sys::napi_env, val: Self) -> Result<sys::napi_value> {
        let env = Env::from_raw(raw_env);
        let mut info = Object::new(&env)?;

        // Set structured fields first so that they can't override the level or message.
        for (key, value) in val.fields {
            match value {
                FieldValue::Bool(value) => info.set(key, value)?,
                FieldValue::Int(value) => info.set(key, value)?,
                FieldValue::Float(value) => info.set(key, value)?,
                FieldValue::String(value) => info.set(key, value)?,
            }
        }
        info.set("target", val.target)?;
        info.set("level", val.level)?;
        info.set("message", val.message)?;

        unsafe { Object::to_napi_value(raw_env, info) }
    }
}

struct FieldCollector(Vec<(String, FieldValue)>);

impl<'kvs> VisitSource<'kvs> for FieldCollector {
    fn visit_pair(
        &mut self,
        key: Key<'kvs>,
        value: Value<'kvs>,
    ) -> std::result::Result<(), KvError> {
        let value = if let Some(value) = value.to_bool() {
            FieldValue::Bool(value)
        } else if let Some(value) = value.to_i64() {
            FieldValue::Int(value)
        } else if let Some(value) = value.to_f64() {
            FieldValue::Float(value)
        } else if let Some(value) = value.to_borrowed_str() {
            FieldValue::String(value.to_owned())
        } else {
            FieldValue::String(value.to_string())
        };

        self.0.push((key.as_str().to_owned(), value));
        Ok(())
    }
}
