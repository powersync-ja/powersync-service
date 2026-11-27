use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Mutex},
};

use crate::{
    block::{BlockEvaluator, BlockEvaluatorOptions, BlockInput, BlockScratch, ResolvedSourceTable},
    format::{EvaluatedBlock, InputDocument},
};
use bson::RawDocument;
use napi::{
    Env, Error, JsString, Result, Status, Task, Unknown,
    bindgen_prelude::{AsyncTask, ClassInstance, JsObjectValue, Object, Uint8Array, Uint32Array},
};
use napi_derive::napi;
use powersync_service::sync_plan::{
    BsonSourceError, CompatibilityContext, SqliteValueFromBson, TimeValuePrecision,
};

use crate::{
    plan::SourceTable,
    values::{SqliteValueToJs, js_to_sqlite_value},
};

/// A Sync Streams evaluator exposed to JavaScript.
///
/// JavaScript resolves which elements (data sources and parameter index lookup creators) apply to
/// a source table and passes them as a [SourceTable] for each block. Elements are identified by the
/// index JavaScript assigned when adding them to a [crate::source_table::ProcessorGroup]. Outputs
/// use the columnar format described in [powersync_service::block].
///
/// Blocks can be evaluated synchronously ([Self::evaluate_bson_block]) or on the libuv thread pool
/// ([Self::evaluate_bson_block_async]), multiple async evaluations may run concurrently.
#[napi]
pub struct SyncStreamEvaluator {
    /// Immutable state, cheap to clone into tasks.
    inner: BlockEvaluator,
    /// Scratch state that isn't used by a pending evaluation.
    ///
    /// Evaluations take scratch state from here (or create new state if all are in use) and
    /// return it afterwards, so the pool grows to the amount of concurrent evaluations.
    scratch: Arc<Mutex<Vec<BlockScratch>>>,
}

#[napi]
impl SyncStreamEvaluator {
    #[napi(constructor, catch_unwind)]
    pub fn new(options: SyncStreamEvaluatorOptions) -> Result<Self> {
        let compatibility = CompatibilityContext {
            max_time_value_precision: match options.max_time_value_precision.as_deref() {
                None => None,
                Some("seconds") => Some(TimeValuePrecision::Seconds),
                Some("milliseconds") => Some(TimeValuePrecision::Milliseconds),
                Some("microseconds") => Some(TimeValuePrecision::Microseconds),
                Some("nanoseconds") => Some(TimeValuePrecision::Nanoseconds),
                _ => return Err(Error::from_status(Status::InvalidArg)),
            },
            timestamps_iso8601: options.timestamps_iso8601,
            fixed_json_extract: options.fixed_json_extract,
            fixed_booleans_in_json: options.fixed_booleans_in_json,
        };

        let defaults = BlockEvaluatorOptions::default();
        let block_options = BlockEvaluatorOptions {
            max_row_size: options
                .max_row_size
                .map_or(defaults.max_row_size, |size| size as usize),
            include_lookup_scope: options
                .include_lookup_scope
                .unwrap_or(defaults.include_lookup_scope),
        };
        Ok(Self {
            inner: BlockEvaluator::new(compatibility, block_options),
            scratch: Default::default(),
        })
    }

    /// Evaluates a block of BSON documents.
    ///
    /// See [crate::format] for a description of serialized inputs and outputs.
    #[napi(catch_unwind)]
    pub fn evaluate_bson_block(
        &self,
        tables: Vec<ClassInstance<SourceTable>>,
        buffers: Vec<Uint8Array>,
        documents: Uint32Array,
    ) -> Result<EvaluatedBlock> {
        let tables = resolve_tables(&tables);
        let documents = InputDocument::parse_documents(&tables, &buffers, &documents)?;
        let outputs =
            evaluate_documents(&self.inner, &self.scratch, &tables, &buffers, &documents)?;
        Ok(outputs.into())
    }

    /// Like [Self::evaluate_bson_block], but evaluates the block on the libuv thread pool.
    ///
    /// See [crate::format] for a description of serialized inputs and outputs.
    #[napi()]
    pub fn evaluate_bson_block_async(
        &self,
        tables: Vec<ClassInstance<SourceTable>>,
        buffers: Vec<Uint8Array>,
        documents: Uint32Array,
    ) -> Result<AsyncTask<EvaluateBlockTask>> {
        let tables = resolve_tables(&tables);
        let documents = InputDocument::parse_documents(&tables, &buffers, &documents)?;
        Ok(AsyncTask::new(EvaluateBlockTask {
            evaluator: self.inner.clone(),
            scratch: self.scratch.clone(),
            tables,
            buffers,
            documents,
        }))
    }

    /// Decodes a BSON document as a `SqliteRecord` using compatibility options configured on this
    /// evaluator.
    ///
    /// This is exclusively used for testing compatibility with JavaScript.
    #[napi(catch_unwind)]
    pub fn test_decode_bson<'a>(&self, env: &'a Env, buffer: &[u8]) -> Result<Object<'a>> {
        fn bson_err(e: bson::error::Error) -> Error {
            Error::new(Status::InvalidArg, e)
        }
        fn value_err(e: BsonSourceError) -> Error {
            Error::new(Status::InvalidArg, e)
        }

        let allocator = bumpalo::Bump::new();
        let bson = RawDocument::from_bytes(buffer).map_err(bson_err)?;
        let decoder = SqliteValueFromBson::new(&allocator, &self.inner.compatibility);

        let mut obj = Object::new(env)?;

        for element in bson {
            let (key, value) = element.map_err(bson_err)?;
            let value = decoder.parse_top_level_value(value).map_err(value_err)?;

            obj.set_named_property(key.as_str(), SqliteValueToJs(value))?;
        }

        Ok(obj)
    }

    /// Evaluates a JavaScript `Record<string, SqliteValue>` as an input row, reporting it as
    /// document `0`.
    ///
    /// This path is only used for testing and not optimized.
    #[napi(catch_unwind)]
    pub fn test_evaluate_object(
        &self,
        table: &SourceTable,
        source: Object,
    ) -> Result<EvaluatedBlock> {
        let mut scratch = self.inner.scratch();
        let mut outputs = EvaluatedBlock::default();
        self.inner
            .evaluate_one(&mut scratch, 0, &table.inner, &mut outputs, |mut input| {
                let arena = input.arena();

                let names = source.get_property_names()?;
                for i in 0..names.get_array_length()? {
                    let name: JsString = names.get_element(i)?;
                    let name = arena.alloc_str(name.into_utf8()?.as_str()?);
                    let Some(field) = input.prepare_field(name) else {
                        // Not referenced by any processor.
                        continue;
                    };

                    let value: Unknown = source.get_named_property(name)?;
                    field.set(js_to_sqlite_value(name, value, arena)?);
                }

                Ok::<_, Error>(input.finish())
            })?;

        Ok(outputs.into())
    }
}

fn resolve_tables(tables: &[ClassInstance<SourceTable>]) -> Vec<Arc<ResolvedSourceTable>> {
    tables.iter().map(|table| table.inner.clone()).collect()
}

/// Evaluates validated documents with scratch state from `pool`.
///
/// This can run on any thread. Panics are reported as errors, since they must not unwind into
/// N-API callers (for async tasks, that would abort the process).
fn evaluate_documents(
    evaluator: &BlockEvaluator,
    pool: &Mutex<Vec<BlockScratch>>,
    tables: &[Arc<ResolvedSourceTable>],
    buffers: &[Uint8Array],
    documents: &[InputDocument],
) -> Result<EvaluatedBlock> {
    let mut scratch = pool
        .lock()
        .unwrap()
        .pop()
        .unwrap_or_else(|| evaluator.scratch());

    // The evaluator really shouldn't panic, but a panic would crash the entire process which is
    // also not what we want.
    let result = catch_unwind(AssertUnwindSafe(|| {
        let mut outputs = scratch.allocate_outputs(documents.len());
        let inputs = documents.iter().map(|document| BlockInput {
            table: &tables[document.table as usize],
            bson: &buffers[document.buffer as usize][document.range.clone()],
        });
        evaluator.evaluate_block(&mut scratch, inputs, &mut outputs);
        outputs
    }));

    match result {
        Ok(outputs) => {
            pool.lock().unwrap().push(scratch);
            Ok(outputs)
        }
        // Scratch state may be inconsistent after a panic, so we drop it instead of returning it to
        // the pool.
        Err(panic) => {
            let message = panic
                .downcast_ref::<&str>()
                .copied()
                .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
                .unwrap_or("unknown panic");
            Err(Error::new(
                Status::GenericFailure,
                format!("Evaluating block panicked: {message}"),
            ))
        }
    }
}

/// Evaluates a block on the libuv thread pool, see [SyncStreamEvaluator::evaluate_bson_block_async].
pub struct EvaluateBlockTask {
    evaluator: BlockEvaluator,
    scratch: Arc<Mutex<Vec<BlockScratch>>>,
    tables: Vec<Arc<ResolvedSourceTable>>,
    /// Typed arrays keep a reference to the JavaScript buffer, so they stay valid while the task is
    /// pending. Reading them doesn't require N-API calls, so this is safe on other threads. The
    /// task is dropped on the JavaScript thread, which releases the references.
    buffers: Vec<Uint8Array>,
    documents: Vec<InputDocument>,
}

impl Task for EvaluateBlockTask {
    type Output = EvaluatedBlock;
    type JsValue = EvaluatedBlock;

    fn compute(&mut self) -> Result<EvaluatedBlock> {
        evaluate_documents(
            &self.evaluator,
            &self.scratch,
            &self.tables,
            &self.buffers,
            &self.documents,
        )
    }

    fn resolve(&mut self, _env: Env, outputs: EvaluatedBlock) -> Result<EvaluatedBlock> {
        Ok(outputs.into())
    }
}

#[napi(object, object_to_js = false)]
pub struct SyncStreamEvaluatorOptions {
    pub max_time_value_precision: Option<String>,
    pub timestamps_iso8601: bool,
    pub fixed_json_extract: bool,
    pub fixed_booleans_in_json: bool,

    /// The maximum size of serialized row data, in bytes. Larger rows are reported as errors
    /// instead of being emitted. Unlimited if not set.
    pub max_row_size: Option<u32>,

    /// Whether serialized parameter lookups include the lookup name and query id (the default).
    /// MongoDB storage v3 stores lookups without them.
    pub include_lookup_scope: Option<bool>,
}
