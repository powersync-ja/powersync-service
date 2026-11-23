//! A fast evaluator for Sync Plans.
//!
//! This implements `HydratedSyncConfig` and `DirectSourceRowConverter` in Rust: Starting from
//! source rows given from e.g. BSON bytes, we decode fields into [SqliteValue] and evaluate
//! bucket data (via [EvaluationSink::row]), parameters ([EvaluationSink::parameter_lookup]) and
//! potential event matches ([EvaluationSink::event]).
//!
//! Emitted outputs are not guaranteed to match the TS implementation exactly on all inputs, but
//! should be functionally equivalent (modulo minor things like whitespace in JSON or the order of
//! keys in projections).
//!
//! To start evaluating plans, begin by deserializing the structures emitted by
//! `createTableProcessorSerializer` in TypeScript by using [DeserializeSyncPlan]. When
//! deserializing elements, also implement [ReplicationEmbedder] to assign tags to buckets,
//! parameters and events. These tags are opaque as far as this evaluator is concerned, but are
//! typically implemented by bucket prefixes, scoped parameter lookups and hydrated event
//! descriptors, respectively. All outputs contain a reference to the original tag, allowing the
//! caller to correctly store buckets and parameter data.
//!
//! ## Preparation
//!
//! From deserialized structures, create a [HydratedSyncConfig]. This creates a trie-like data
//! structure grouping all unique overlapping groups of table patterns. This allows us to
//! efficiently find processors for a given source table name, and lets us share evaluation state
//! between them.
//!
//! Each group of processors is represented as a [processor_group::ProcessorGroup]. Each row is
//! evaluated by at most one matching group. It is possible for a source to appear in multiple
//! groups when wildcard matchers are involved, though.
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
//! To evaluate rows, create an [Evaluator] and pass the hydrated sync config. For each row to
//! process, [Evaluator::evaluate] finds a matching group (if none is found, we can safely ignore
//! the row).
//!
//! Once a matching group is found, [RowEvaluation::push_bson] (and friends) deserialize source data
//! as bytes. While evaluating a row, intermediate values reference source data as slices instead of
//! copying. When that is not possible, we use the [bumpalo] crate for allocations, which have the
//! same lifetime as the row being processed. We clear the arena after the row.
//!
//! Outputs report everything needed to serialize results and store them in bucket storage:
//!
//!  1. For replicated rows, the bucket's tag, parameters, and the row projection.
//!  2. For parameters, the parameter's tag as well as input and output parameters.
//!  3. For events, a notification that the event might match.

mod compatibility;
mod deserialize;
mod error;
mod evaluator;
mod expression;
mod hydrated;
mod operators;
mod plan;
mod processor_group;
mod sources;
mod table_pattern;
mod value;

pub use compatibility::*;
pub use deserialize::DeserializeSyncPlan;
pub use error::EvaluationError;
pub use evaluator::{ErrorSource, EvaluationSink, Evaluator, ProjectedRow, RowEvaluation};
pub use hydrated::*;
pub use table_pattern::SourceTableRef;
pub use value::{Parameters, SqliteValue};

/// Interns column names and aliases referenced by processors in a [HydratedSyncConfig].
///
/// Since a processor can be part of multiple processor groups, this avoids storing the same names
/// once per group. It also allows looking up symbols by `&str` without allocating, which is used to
/// resolve field names of source rows.
pub type StringInterner = string_interner::DefaultStringInterner;
pub type Symbol = string_interner::DefaultSymbol;

/// Test fixtures and utilities
#[cfg(test)]
mod test {
    use std::sync::Arc;

    use serde_json::{Value, json};

    use crate::sync_plan::{
        CompatibilityContext, DeserializeSyncPlan, Evaluator, HydratedSource, HydratedSyncConfig,
        table_pattern::{NameMatcher, SourceTableRef, TablePattern},
    };

    use super::ReplicationEmbedder;

    pub struct TestEmbedder;

    impl ReplicationEmbedder for TestEmbedder {
        type BucketId = &'static str;
        type ParameterId = &'static str;
        type EventId = &'static str;
    }

    pub static DESERIALIZE_PLAN: DeserializeSyncPlan<'static> = DeserializeSyncPlan {
        default_schema: "public",
    };

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

    /// Defines an event source matching the given tables.
    pub fn event_source(id: &'static str, sources_tables: &[&str]) -> HydratedSource<TestEmbedder> {
        let queries = sources_tables
            .iter()
            .map(|tbl| json!({"table": {"table": tbl}}));

        DESERIALIZE_PLAN
            .event_source(
                Arc::new(id),
                json!({
                    "name": id,
                    "sourceQueries": &Value::Array(queries.collect()),
                }),
            )
            .expect("event source")
    }

    pub fn config(sources: &[HydratedSource<TestEmbedder>]) -> HydratedSyncConfig<TestEmbedder> {
        let context = CompatibilityContext::default();
        HydratedSyncConfig::new(context, sources)
    }

    pub fn evaluator(sources: &[HydratedSource<TestEmbedder>]) -> Evaluator<TestEmbedder> {
        let config = config(sources);
        Evaluator::new(Arc::new(config))
    }
}
