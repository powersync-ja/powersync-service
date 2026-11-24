//! Evaluates blocks of source rows into a columnar format that bucket storage can consume without
//! creating intermediate objects per row.
//!
//! This builds on the [crate::sync_plan] evaluator, and additionally computes everything storage
//! needs to persist outputs:
//!
//!  1. For rows, the bucket name, output table, row id, serialized JSON data and checksum.
//!  2. For parameter lookups, the BSON-serialized lookup (as used by storage, see
//!     [BlockEvaluatorOptions::include_lookup_scope]) and the BSON-serialized bucket parameters,
//!     grouped by lookup like the JavaScript implementation.
//!
//! ## Output format
//!
//! All text and binary outputs of a block are written into a single [BlockOutputs::bytes] buffer.
//! [BlockOutputs::rows] and [BlockOutputs::parameters] describe outputs with a fixed number of
//! `u32`s each (see [RowColumn] and [ParameterColumn]), referencing byte ranges in
//! [BlockOutputs::bytes]. A range is described by a start and an end offset.

use std::sync::Arc;

use bson::serialize_to_buffer;
use serde::{
    Serialize,
    ser::{SerializeMap, SerializeSeq},
};
use sha2::{Digest, Sha256};
use smallvec::SmallVec;

use crate::sync_plan::{
    BucketDataIterator, CompatibilityContext, Evaluator, InputRow, Parameter,
    ParameterLookupIterator, ProcessedGroupMember, ProcessorGroup, ReplicationEmbedder,
    RowEvaluation, SourceTableRef, cast,
};

/// The [ReplicationEmbedder] for block evaluation, using numeric element indices as tags.
#[derive(Debug)]
pub struct BlockEmbedder;

impl ReplicationEmbedder for BlockEmbedder {
    type BucketId = Arc<BucketElement>;
    type ParameterId = Arc<ParameterElement>;
}

/// A data source evaluated by a [BlockEvaluator].
#[derive(Debug)]
pub struct BucketElement {
    /// The index of this element, used to identify it in outputs.
    pub index: u32,
    /// Bucket name prefixes of this source.
    ///
    /// A data source can be part of multiple hydrated sync configs, each giving it a different
    /// prefix. Each evaluated row is emitted once per prefix, with [RowColumn::Prefix] being the
    /// index into this list.
    pub prefixes: Vec<String>,
}

/// A parameter index lookup creator evaluated by a [BlockEvaluator].
#[derive(Debug)]
pub struct ParameterElement {
    /// The index of this element, used to identify it in outputs.
    pub index: u32,
    /// The first value of serialized lookups (`ParameterLookupScope.lookupName` in TypeScript).
    pub lookup_name: String,
    /// The second value of serialized lookups (`ParameterLookupScope.queryId` in TypeScript).
    pub query_id: String,
}

/// Columns of a row in [BlockOutputs::rows].
#[repr(usize)]
#[derive(Clone, Copy, Debug)]
pub enum RowColumn {
    /// The index of the source document in the evaluated block.
    Document,
    /// [BucketElement::index] of the data source.
    Element,
    /// The index into [BucketElement::prefixes].
    Prefix,
    BucketStart,
    BucketEnd,
    TableStart,
    TableEnd,
    IdStart,
    IdEnd,
    DataStart,
    DataEnd,
    /// The checksum of the row, like `hashData` in TypeScript.
    Checksum,
}

impl RowColumn {
    pub const WIDTH: usize = RowColumn::Checksum as usize + 1;
}

/// Columns of a parameter lookup in [BlockOutputs::parameters].
#[repr(usize)]
#[derive(Clone, Copy, Debug)]
pub enum ParameterColumn {
    /// The index of the source document in the evaluated block.
    Document,
    /// [ParameterElement::index] of the parameter source.
    Element,
    /// A BSON document `{l: [lookup_name, query_id, ...inputs]}`, matching `serializeLookupBuffer`.
    ///
    /// Without [BlockEvaluatorOptions::include_lookup_scope], this is `{l: [...inputs]}` instead,
    /// matching `serializeParameterLookup` in MongoDB storage v3.
    LookupStart,
    LookupEnd,
    /// A BSON document `{p: [{"0": output0, "1": output1, ...}, ...]}` with bucket parameters of
    /// all outputs for that lookup.
    OutputsStart,
    OutputsEnd,
}

impl ParameterColumn {
    pub const WIDTH: usize = ParameterColumn::OutputsEnd as usize + 1;
}

/// An error evaluating a document of a block.
#[derive(Debug)]
pub struct BlockError {
    pub document: u32,
    /// The element that failed to evaluate, or [None] if the document couldn't be decoded.
    pub element: Option<u32>,
    pub message: String,
}

/// Outputs of evaluating a block, see the module documentation for the format.
#[derive(Default, Debug)]
pub struct BlockOutputs {
    pub bytes: Vec<u8>,
    pub rows: Vec<u32>,
    pub parameters: Vec<u32>,
    pub errors: Vec<BlockError>,
}

impl BlockOutputs {
    pub fn with_capacity(bytes: usize, rows: usize, parameters: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(bytes),
            rows: Vec::with_capacity(rows * RowColumn::WIDTH),
            parameters: Vec::with_capacity(parameters * ParameterColumn::WIDTH),
            errors: vec![],
        }
    }

    fn offset(&self) -> u32 {
        u32::try_from(self.bytes.len()).expect("block outputs must not exceed 4GiB")
    }

    fn push_str(&mut self, value: &str) -> [u32; 2] {
        let start = self.offset();
        self.bytes.extend_from_slice(value.as_bytes());
        [start, self.offset()]
    }

    fn range(&self, range: [u32; 2]) -> &[u8] {
        &self.bytes[range[0] as usize..range[1] as usize]
    }
}

/// A source table that documents of a block can belong to, along with the processors to evaluate
/// for rows in that table.
#[derive(Debug)]
pub struct BlockSourceTable {
    pub connection_tag: String,
    pub schema: String,
    pub name: String,
    /// The elements matching this table.
    ///
    /// Groups can be shared between tables (e.g. for tables matched by the same wildcard patterns).
    pub group: Arc<ProcessorGroup<BlockEmbedder>>,
}

impl BlockSourceTable {
    pub fn as_ref(&self) -> SourceTableRef<'_> {
        SourceTableRef {
            connection_tag: &self.connection_tag,
            schema: &self.schema,
            name: &self.name,
        }
    }
}

/// A source document in a block.
#[derive(Clone, Copy)]
pub struct BlockInput<'a> {
    pub table: &'a BlockSourceTable,
    pub bson: &'a [u8],
}

/// Options for [BlockEvaluator].
#[derive(Clone, Copy, Debug)]
pub struct BlockEvaluatorOptions {
    /// The maximum size of serialized row data, in bytes. Larger rows are reported as errors.
    pub max_row_size: usize,
    /// Whether serialized parameter lookups start with [ParameterElement::lookup_name] and
    /// [ParameterElement::query_id].
    ///
    /// Storage versions identifying the lookup scope separately (MongoDB storage v3) don't include
    /// them in the lookup.
    pub include_lookup_scope: bool,
}

impl Default for BlockEvaluatorOptions {
    fn default() -> Self {
        Self {
            max_row_size: usize::MAX,
            include_lookup_scope: true,
        }
    }
}

/// Evaluates blocks of source rows.
///
/// This only contains immutable state and is cheap to clone, so it can be shared with tasks
/// evaluating blocks on other threads. Mutable state needed while evaluating is in a
/// [BlockScratch], of which each concurrent evaluation needs its own.
#[derive(Clone)]
pub struct BlockEvaluator {
    pub compatibility: Arc<CompatibilityContext>,
    options: BlockEvaluatorOptions,
}

impl BlockEvaluator {
    pub fn new(compatibility: CompatibilityContext, options: BlockEvaluatorOptions) -> Self {
        Self {
            compatibility: Arc::new(compatibility),
            options,
        }
    }

    /// Creates mutable state for evaluating blocks with this evaluator (or clones of it).
    pub fn scratch(&self) -> BlockScratch {
        BlockScratch {
            evaluator: Evaluator::new(self.compatibility.clone()),
            writer: OutputWriter {
                options: self.options,
                bucket_parameters: vec![],
            },
            last_sizes: [0; 3],
        }
    }

    /// Evaluates BSON documents, appending outputs to `outputs`.
    ///
    /// Documents are numbered in the order of `inputs`, starting at `0`. Documents that can't be
    /// decoded are reported as an error without an element.
    pub fn evaluate_block<'a>(
        &self,
        scratch: &mut BlockScratch,
        inputs: impl IntoIterator<Item = BlockInput<'a>>,
        outputs: &mut BlockOutputs,
    ) {
        for (document, input) in inputs.into_iter().enumerate() {
            let document = document as u32;
            let result = self.evaluate_one(scratch, document, input.table, outputs, |row| {
                row.push_bson(input.bson)
            });

            if let Err(e) = result {
                outputs.errors.push(BlockError {
                    document,
                    element: None,
                    message: e.to_string(),
                });
            }
        }

        scratch.last_sizes = [
            outputs.bytes.len(),
            outputs.rows.len() / RowColumn::WIDTH,
            outputs.parameters.len() / ParameterColumn::WIDTH,
        ];
    }

    /// Evaluates a single document, using `decode` to fill the [InputRow].
    ///
    /// Documents of tables without any elements are skipped without calling `decode`.
    pub fn evaluate_one<'s, E>(
        &self,
        scratch: &'s mut BlockScratch,
        document: u32,
        table: &'s BlockSourceTable,
        outputs: &mut BlockOutputs,
        decode: impl FnOnce(InputRow<'s, BlockEmbedder>) -> Result<RowEvaluation<'s, BlockEmbedder>, E>,
    ) -> Result<(), E> {
        if table.group.processors.is_empty() {
            return Ok(());
        }

        let BlockScratch {
            evaluator, writer, ..
        } = scratch;

        let input = evaluator.evaluate(table.as_ref(), &table.group);
        let mut eval = decode(input)?;
        writer.write(document, &mut eval, outputs);
        Ok(())
    }
}

/// Mutable state used while evaluating a block, see [BlockEvaluator::scratch].
///
/// Scratch state can be reused across blocks to avoid allocations, but shouldn't be reused after
/// a panic while evaluating.
pub struct BlockScratch {
    evaluator: Evaluator,
    writer: OutputWriter,
    /// Sizes (bytes, rows, parameters) of the last evaluated block.
    last_sizes: [usize; 3],
}

impl BlockScratch {
    /// Allocates outputs for a block of `documents`, using the previous block as an estimate.
    pub fn allocate_outputs(&self, documents: usize) -> BlockOutputs {
        let [bytes, rows, parameters] = self.last_sizes;
        BlockOutputs::with_capacity(bytes.max(documents * 256), rows.max(documents), parameters)
    }
}

/// Writes evaluation results into [BlockOutputs].
struct OutputWriter {
    options: BlockEvaluatorOptions,
    /// Scratch buffer for serialized bucket parameters of a row.
    bucket_parameters: Vec<u8>,
}

impl OutputWriter {
    fn write(
        &mut self,
        document: u32,
        eval: &mut RowEvaluation<'_, BlockEmbedder>,
        outputs: &mut BlockOutputs,
    ) {
        for member in eval.members() {
            match member {
                ProcessedGroupMember::Data(source) => {
                    self.rows(document, eval.process_data(source), outputs);
                }
                ProcessedGroupMember::Parameter(source) => {
                    self.parameter_lookups(document, eval.process_parameters(source), outputs);
                }
            }
        }
    }

    fn rows(
        &mut self,
        document: u32,
        mut iterator: BucketDataIterator<'_, '_, BlockEmbedder>,
        outputs: &mut BlockOutputs,
    ) {
        let element = iterator.source.id.index;
        let output_table = iterator.table;
        let arena = iterator.arena();
        // The output table is the same for all rows of this source, so we only write it once.
        let mut table_range: Option<[u32; 2]> = None;

        while let Some(result) = iterator.next() {
            let row = match result {
                Ok(row) => row,
                Err(e) => {
                    outputs.errors.push(BlockError {
                        document,
                        element: Some(element),
                        message: format!("Error evaluating data for Sync Stream: {e}"),
                    });
                    continue;
                }
            };

            let data_start = outputs.offset();
            let id = {
                let mut serializer = serde_json::Serializer::new(&mut outputs.bytes);
                match row.row.serialize_and_extract_id(&mut serializer) {
                    Ok((_, id)) => cast::id_from_data(id, arena),
                    Err(e) => {
                        outputs.bytes.truncate(data_start as usize);
                        outputs.errors.push(BlockError {
                            document,
                            element: Some(element),
                            message: format!("Error serializing row: {e}"),
                        });
                        continue;
                    }
                }
            };
            let data = [data_start, outputs.offset()];

            let data_size = (data[1] - data[0]) as usize;
            if data_size > self.options.max_row_size {
                outputs.bytes.truncate(data_start as usize);
                outputs.errors.push(BlockError {
                    document,
                    element: Some(element),
                    message: format!(
                        "Row too large: {data_size} bytes, the maximum size is {} bytes",
                        self.options.max_row_size
                    ),
                });
                continue;
            }

            let id = outputs.push_str(id);
            let table = *table_range.get_or_insert_with(|| outputs.push_str(output_table));

            let checksum = {
                let mut hash = Sha256::new();
                hash.update(b"put.");
                hash.update(outputs.range(table));
                hash.update(b".");
                hash.update(outputs.range(id));
                hash.update(b".");
                hash.update(outputs.range(data));
                let digest = hash.finalize();
                u32::from_le_bytes(digest[..4].try_into().unwrap())
            };

            // Bucket parameters are the same for all prefixes, serialize them once.
            let parameters = &mut self.bucket_parameters;
            parameters.clear();
            serde_json::to_writer(&mut *parameters, &row.parameters)
                .expect("serializing parameters to a Vec can't fail");

            for (prefix_index, prefix) in row.bucket.prefixes.iter().enumerate() {
                let bucket_start = outputs.offset();
                outputs.bytes.extend_from_slice(prefix.as_bytes());
                outputs.bytes.extend_from_slice(parameters);
                let bucket_end = outputs.offset();

                outputs.rows.extend_from_slice(&[
                    document,
                    element,
                    prefix_index as u32,
                    bucket_start,
                    bucket_end,
                    table[0],
                    table[1],
                    id[0],
                    id[1],
                    data[0],
                    data[1],
                    checksum,
                ]);
            }
        }
    }

    fn parameter_lookups(
        &mut self,
        document: u32,
        mut iterator: ParameterLookupIterator<'_, '_, BlockEmbedder>,
        outputs: &mut BlockOutputs,
    ) {
        let source = iterator.source;
        let element = source.id.index;

        // An evaluator can create multiple lookups on the same row (when there's a table-valued
        // function). Like JavaScript, we group outputs by lookup values.
        let mut groups = SmallVec::<[(Values, SmallVec<[Values; 1]>); 1]>::new();

        while let Some(result) = iterator.next() {
            let lookup = match result {
                Ok(lookup) => lookup,
                Err(e) => {
                    outputs.errors.push(BlockError {
                        document,
                        element: Some(element),
                        message: format!("Error evaluating parameter for Sync Stream: {e}"),
                    });
                    continue;
                }
            };

            let row_outputs: Values = lookup.outputs.iter().copied().collect();
            // Typically there's just one lookup, so we can use a linear scan here.
            if let Some((_, group_outputs)) = groups
                .iter_mut()
                .find(|(inputs, _)| inputs.as_slice() == lookup.lookup)
            {
                group_outputs.push(row_outputs);
            } else {
                let inputs = lookup.lookup.iter().copied().collect();
                groups.push((inputs, smallvec::smallvec![row_outputs]));
            }
        }

        for (inputs, group_outputs) in groups {
            let lookup_start = outputs.offset();

            // Mirrored after serializeParameterLookup in v3/models.ts, allowing the mongo storage
            // module to skip decoding these bytes again.
            #[derive(Serialize)]
            struct ScopedParameterLookup<'a> {
                l: ParameterLookup<'a>,
            }

            serialize_to_buffer(
                &ScopedParameterLookup {
                    l: ParameterLookup {
                        definition: if self.options.include_lookup_scope {
                            Some((&source.id.lookup_name, &source.id.query_id))
                        } else {
                            None
                        },
                        values: inputs.as_ref(),
                    },
                },
                &mut outputs.bytes,
            )
            .expect("writing BSON is infallible");

            let lookup_end = outputs.offset();

            // These are deserialized in JS, and then serialized again as bucket_parameters. This is
            // unavoidable with the current architecture.
            #[derive(Serialize)]
            struct OutputParameters<'a, 'v> {
                p: OutputRows<'a, 'v>,
            }

            serialize_to_buffer(
                &OutputParameters {
                    p: OutputRows(&group_outputs),
                },
                &mut outputs.bytes,
            )
            .expect("writing BSON is infallible");
            let outputs_end = outputs.offset();

            outputs.parameters.extend_from_slice(&[
                document,
                element,
                lookup_start,
                lookup_end,
                lookup_end,
                outputs_end,
            ]);
        }
    }
}

struct ParameterLookup<'a> {
    // Embedded scope for the parameter, only serialized for testing purposes.
    pub definition: Option<(&'a str, &'a str)>,
    pub values: &'a [Parameter<'a>],
}

impl Serialize for ParameterLookup<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut arr = serializer.serialize_seq(Some(
            if self.definition.is_some() { 2 } else { 0 } + self.values.len(),
        ))?;

        if let Some((scope, query_id)) = self.definition {
            arr.serialize_element(scope)?;
            arr.serialize_element(query_id)?;
        }

        for value in self.values {
            arr.serialize_element(value)?;
        }
        arr.end()
    }
}

type Values<'a> = SmallVec<[Parameter<'a>; 4]>;

/// Serializes output rows of a parameter lookup as a list of [ParametersToObject].
struct OutputRows<'a, 'v>(&'a [Values<'v>]);

impl Serialize for OutputRows<'_, '_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_seq(self.0.iter().map(|row| ParametersToObject(row)))
    }
}

/// Serializes an array of parameters ["a", "b", "c"] as {"0": "a", "1": "b", "2": "c"}, matching
/// the JavaScript implementation.
struct ParametersToObject<'a, 'v>(&'a [Parameter<'v>]);

impl Serialize for ParametersToObject<'_, '_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        const INDEX_KEYS: [&str; 8] = ["0", "1", "2", "3", "4", "5", "6", "7"];

        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (i, param) in self.0.iter().enumerate() {
            match INDEX_KEYS.get(i) {
                Some(key) => map.serialize_key(key),
                None => map.serialize_key(&i.to_string()),
            }?;

            // Like in JS, _outputs_ of parameter lookups are not normalized (arguably a bug).
            map.serialize_value(param)?;
        }

        map.end()
    }
}
