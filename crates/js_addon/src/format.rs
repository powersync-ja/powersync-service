//! The format used to evaluate BSON sources for Mongo source and storage modules.
//!
//! Accessing JavaScript object members in Rust is slow, so we instead have JavaScript serialize its
//! input as a typed buffer. Rust outputs are also emitted in a pre-serialized form of a single
//! buffer.
//!
//! ## Inputs
//!
//! To evaluate a batch of source document, JavaScript passes three arrays:
//!
//!  1. An array of source table ([ResolvedSourceTable]) references, typically small.
//!  2. An array of Node buffers from which to read rows. Note that many BSON rows may be read from
//!     a single source buffer, so this array is also typically small.
//!  3. An array of documents as a single `Uint32Array`. Documents contain the index into their
//!     buffer, the offset and length within that buffer, and the index into the source table array.
//!
//! In a microbenchmark, this has substantially reduced the overhead of napi calls.
//!
//! ## Outputs
//!
//!

use std::{ops::Range, sync::Arc};

use napi::bindgen_prelude::{
    Buffer, JsObjectValue, Object, ToNapiValue, TypeName, Uint8Array, Uint32Array,
};
use napi::{Env, Error, JsValue, Result, Status, sys};
use napi_derive::napi;
use powersync_service::sync_plan::{Parameter, SqliteValue};

use crate::{block::ResolvedSourceTable, values::SqliteValueToJs};

/// A serialized input row being processed.
pub struct InputDocument {
    pub buffer: u32,
    pub range: Range<usize>,
    pub table: u32,
}

impl InputDocument {
    /// The amount of u32 fields making up a document.
    ///
    /// Input documents are serialized as [bufferIndex, startOffset, length, tableIndex].
    pub const SERIALIZED_WIDTH: usize = 4;

    /// Deserializes documents from a serialized u32 array.
    pub fn parse_documents(
        tables: &[Arc<ResolvedSourceTable>],
        buffers: &[Uint8Array],
        documents: &[u32],
    ) -> Result<Vec<Self>> {
        let (documents, rest) = documents.as_chunks::<{ Self::SERIALIZED_WIDTH }>();
        if !rest.is_empty() {
            return Err(Error::new(
                Status::InvalidArg,
                "documents must describe each document with 4 numbers",
            ));
        }

        documents
            .iter()
            .map(|&[buffer, offset, length, table]| {
                let buffer_len = buffers
                    .get(buffer as usize)
                    .ok_or_else(|| {
                        Error::new(Status::InvalidArg, "document references unknown buffer")
                    })?
                    .len();
                let range = offset as usize..(offset as usize).saturating_add(length as usize);
                if range.end > buffer_len {
                    return Err(Error::new(Status::InvalidArg, "document is out of bounds"));
                }
                if table as usize >= tables.len() {
                    return Err(Error::new(
                        Status::InvalidArg,
                        "document references unknown table",
                    ));
                }

                Ok(Self {
                    buffer,
                    range,
                    table,
                })
            })
            .collect()
    }
}

#[napi(object, object_from_js = false)]
#[derive(Debug)]
pub struct SerializedEvaluationError {
    pub document: u32,
    pub element: Option<u32>,
    pub message: String,
}

#[derive(Default)]
pub struct EvaluatedBlock {
    pub bytes: Vec<u8>,
    pub rows: Vec<u32>,
    pub parameters: Vec<u32>,
    /// Bucket parameters of each parameter lookup, one entry per output row.
    pub bucket_parameters: Vec<Vec<BucketParameterRow>>,
    errors: Vec<SerializedEvaluationError>,
}

impl EvaluatedBlock {
    pub fn with_capacity(bytes: usize, rows: usize, parameters: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(bytes),
            rows: Vec::with_capacity(rows * Self::ROW_COLUMNS),
            parameters: Vec::with_capacity(parameters * EvaluatedBlock::PARAMETER_COLUMNS),
            bucket_parameters: Vec::with_capacity(parameters),
            errors: vec![],
        }
    }

    pub fn error(&mut self, document: u32, element: Option<u32>, message: String) {
        self.errors.push(SerializedEvaluationError {
            document,
            element,
            message,
        });
    }

    pub fn offset(&self) -> u32 {
        u32::try_from(self.bytes.len()).expect("block outputs must not exceed 4GiB")
    }

    pub fn push_str(&mut self, value: &str) -> [u32; 2] {
        let start = self.offset();
        self.bytes.extend_from_slice(value.as_bytes());
        [start, self.offset()]
    }

    pub fn range(&self, range: [u32; 2]) -> &[u8] {
        &self.bytes[range[0] as usize..range[1] as usize]
    }

    /// The amount of u32 values used to serialize a row. The format is:
    ///
    /// 1. The index of the source row being processed.
    /// 2. The [crate::block::BucketElement::index] of the data source.
    /// 3. The bucket prefix, as an index into [crate::block::BucketElement::prefixes].
    /// 4. and 5: Start (inclusive) and end (exlusive) offset of serialized bucket parameters in
    ///    [Self::bytes].
    /// 6. and 7. The same for the output table name.
    /// 8. and 9. The same for the id column.
    /// 10. and 11. The same for the projection, serialized as JSON.
    /// 13. Index to a 4-byte buffer containing the checksum for the `PUT` operation.
    pub const ROW_COLUMNS: usize = 12;

    /// The amount of u32 values used to serialize a parameter. The format is:
    ///
    /// 1. The index of the source row being processed.
    /// 2. The [crate::block::ParameterElement::index] of the parameter source.
    /// 3. and 4. Byte range (in [Self::bytes]) of a BSON document having the format `{l: inputs}`,
    /// matching `serializeLookupBuffer` for Mongo bucket storage v3+.
    ///
    /// Bucket parameters of each lookup are not part of this array, they're in
    /// [Self::bucket_parameters] (at the same index as the lookup).
    pub const PARAMETER_COLUMNS: usize = 4;
}

impl ToNapiValue for EvaluatedBlock {
    unsafe fn to_napi_value(env: napi::sys::napi_env, val: Self) -> Result<napi::sys::napi_value> {
        let mut obj = Object::new(&Env::from_raw(env))?;
        obj.set_named_property("bytes", Buffer::from(val.bytes))?;
        obj.set_named_property("rows", Uint32Array::new(val.rows))?;
        obj.set_named_property("parameters", Uint32Array::new(val.parameters))?;
        obj.set_named_property("bucketParameters", val.bucket_parameters)?;
        obj.set_named_property("errors", val.errors)?;
        Ok(obj.raw())
    }
}

impl TypeName for EvaluatedBlock {
    fn type_name() -> &'static str {
        "EvaluatedBlock"
    }

    fn value_type() -> napi::ValueType {
        napi::ValueType::Object
    }
}

/// Bucket parameters for a single output row of a parameter lookup, exposed to JavaScript as a
/// `Record<string, SqliteValue>` of the form `{"0": output0, "1": output1, ...}`, matching the
/// JavaScript implementation.
///
/// Blocks may be evaluated on other threads, so we can't create JavaScript objects right away and
/// store owned values instead.
pub struct BucketParameterRow(Vec<OwnedParameter>);

impl BucketParameterRow {
    pub fn new(values: &[Parameter<'_>]) -> Self {
        Self(
            values
                .iter()
                .map(|value| match *value.as_ref() {
                    SqliteValue::Integer(i) => OwnedParameter::Integer(i),
                    SqliteValue::Float(f) => OwnedParameter::Float(f),
                    SqliteValue::Text(text) => OwnedParameter::Text(text.to_owned()),
                    SqliteValue::Null { .. } | SqliteValue::Blob(_) => {
                        unreachable!("parameters can't be null or blobs")
                    }
                })
                .collect(),
        )
    }
}

enum OwnedParameter {
    Integer(i64),
    Float(f64),
    Text(String),
}

impl ToNapiValue for BucketParameterRow {
    unsafe fn to_napi_value(env: sys::napi_env, val: Self) -> Result<sys::napi_value> {
        const INDEX_KEYS: [&str; 8] = ["0", "1", "2", "3", "4", "5", "6", "7"];

        let mut obj = Object::new(&Env::from_raw(env))?;
        for (i, value) in val.0.iter().enumerate() {
            // Like in JS, _outputs_ of parameter lookups are not normalized (arguably a bug).
            let value = SqliteValueToJs(match value {
                OwnedParameter::Integer(i) => SqliteValue::Integer(*i),
                OwnedParameter::Float(f) => SqliteValue::Float(*f),
                OwnedParameter::Text(text) => SqliteValue::Text(text),
            });

            match INDEX_KEYS.get(i) {
                Some(key) => obj.set_named_property(key, value),
                None => obj.set_named_property(&i.to_string(), value),
            }?;
        }

        Ok(obj.raw())
    }
}
