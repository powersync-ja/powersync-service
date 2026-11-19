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
pub use evaluator::{ErrorSource, EvaluationSink, Evaluator, ProjectedRow};
pub use hydrated::*;
pub use table_pattern::SourceTableRef;
pub use value::*;

/// Interns column names and aliases referenced by processors in a [HydratedSyncConfig].
///
/// Since a processor can be part of multiple processor groups, this avoids storing the same names
/// once per group. It also allows looking up symbols by `&str` without allocating, which is used to
/// resolve field names of source rows.
pub type StringInterner = string_interner::DefaultStringInterner;
pub type Symbol = string_interner::DefaultSymbol;

/// Test fixtures and utilities for Sync Plans.
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
        default_connection_tag: "tag",
        default_schema: "public",
    };

    pub fn source_table_ref<'a>(name: &'a str) -> SourceTableRef<'a> {
        SourceTableRef {
            connection_tag: "tag",
            schema: "public",
            name,
        }
    }

    pub fn table_pattern(name: &str) -> TablePattern {
        TablePattern {
            connection_tag: "tag".to_owned(),
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
