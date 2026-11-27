//! A fast evaluator for Sync Plans.
//!
//! This implements a subset of `HydratedSyncConfig` and `DirectSourceRowConverter` in Rust:
//! Starting from source rows given from e.g. BSON bytes, we decode fields into [SqliteValue] and
//! evaluate bucket data ([RowEvaluation::process_data]) and parameters
//! ([RowEvaluation::process_parameters]).
//!
//! Emitted outputs are not guaranteed to match the TS implementation exactly on all inputs, but
//! should be functionally equivalent (modulo minor things like whitespace in JSON or the order of
//! keys in projections).
//!
//! To start evaluating plans, begin by deserializing the structures emitted by
//! `createTableProcessorSerializer` in TypeScript by using [DeserializeSyncPlan]. Also implement
//! [ReplicationEmbedder] to assign tags to buckets and parameters. These tags are opaque as far as
//! this evaluator is concerned, but are typically implemented by bucket prefixes and scoped
//! parameter lookups, respectively. All outputs contain a reference to the original tag, allowing
//! the caller to correctly store buckets and parameter data.
//!
//! ## Preparation
//!
//! This evaluator does not match source tables against table patterns. Instead, callers are
//! responsible for resolving which data and parameter sources apply to a source table, and for
//! combining them into a [ProcessorGroup] (via [ProcessorGroup::include_data] and
//! [ProcessorGroup::include_parameter]). Members of a group share evaluation state, so callers
//! should typically build one group per source table and add all sources matching that table.
//!
//! Each group declares the amount of memory (as a slice of [SqliteValue]s) needed to evaluate it.
//! There is one slice for inputs (shared between all members of the group) and one for internal
//! columns. Inputs are values extracted from the decoded row, meaning that we can represent input
//! rows as arrays instead of allocating a `Record<string, SqliteValue>` per row.
//!
//! In a prepared processor, Sync Plan expressions are lowered into
//! [processor_group::EvaluatorInstruction]s, which avoids a class of intermediate allocations by
//! pre-assigning static slots for inputs and outputs of expressions.
//!
//! ## Evaluation
//!
//! To evaluate rows, create an [Evaluator] with the [CompatibilityContext] to use. For each row to
//! process, call [Evaluator::evaluate] with the source table and the [ProcessorGroup] the caller
//! has prepared for it.
//!
//! [InputRow::push_bson] (and friends) then deserialize source data as bytes. While evaluating a
//! row, intermediate values reference source data as slices instead of copying. When that is not
//! possible, we use the [bumpalo] crate for allocations, which have the same lifetime as the row
//! being processed. We clear the arena after the row.
//!
//! Outputs report everything needed to serialize results and store them in bucket storage:
//!
//!  1. For replicated rows, the bucket's tag, parameters, and the row projection.
//!  2. For parameters, the parameter's tag as well as input and output parameters.

mod compatibility;
mod deserialize;
mod error;
mod evaluation;
mod expression;
mod operators;
mod plan;
mod processor_group;
mod sources;
mod table_pattern;
mod value;

use std::fmt::Debug;

pub use compatibility::*;
pub use deserialize::DeserializeSyncPlan;
pub use error::EvaluationError;
pub use evaluation::{
    BucketData, BucketDataIterator, EvaluatedParameter, Evaluator, InputRow,
    ParameterLookupIterator, PreparedField, ProjectedRow, RowEvaluation,
};
pub use processor_group::ProcessorGroup;
pub use processor_group::{ProcessedDataSource, ProcessedGroupMember, ProcessedParameterSource};
pub use table_pattern::SourceTableRef;
pub use value::SqliteValue;

/// Interns column names and aliases referenced by processors in a [ProcessorGroup].
///
/// This allows looking up symbols by `&str` without allocating, which is used to resolve field
/// names of source rows.
pub type StringInterner = string_interner::DefaultStringInterner;
pub type Symbol = string_interner::DefaultSymbol;

/// A trait defining how processed rows are formatted or saved.
pub trait ReplicationEmbedder {
    /// Something that, along with bucket parameters, uniquely identifies a bucket.
    ///
    /// This is typically a prefix (and an id of the replication stream).
    type BucketId: Debug;

    /// Something that uniquely identifies a parameter lookup query.
    ///
    /// This is typically a lookup name and query id.
    type ParameterId: Debug;
}

/// Test fixtures and utilities
#[cfg(test)]
mod test {

    use std::sync::Arc;

    use crate::sync_plan::{
        CompatibilityContext, Evaluator,
        table_pattern::{NameMatcher, SourceTableRef, TablePattern},
    };

    use super::ReplicationEmbedder;

    pub struct TestEmbedder;

    impl ReplicationEmbedder for TestEmbedder {
        type BucketId = &'static str;
        type ParameterId = &'static str;
    }

    pub fn source_table_ref<'a>(name: &'a str) -> SourceTableRef<'a> {
        SourceTableRef {
            connection_tag: TablePattern::DEFAULT_TAG,
            schema: "public",
            name,
        }
    }

    pub fn table_pattern(name: &str) -> TablePattern {
        TablePattern {
            connection_tag: TablePattern::DEFAULT_TAG.to_owned(),
            schema: NameMatcher::from_string("public".to_owned()),
            table: NameMatcher::from_string(name.to_owned()),
        }
    }

    pub fn evaluator() -> Evaluator {
        Evaluator::new(Arc::new(CompatibilityContext::default()))
    }
}
