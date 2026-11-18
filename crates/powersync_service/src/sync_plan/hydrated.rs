use std::{fmt::Debug, sync::Arc};

use crate::sync_plan::{
    StringInterner,
    compatibility::CompatibilityContext,
    plan::{StreamDataSource, StreamParameterIndexLookupCreator},
    processor_group::ProcessorGroup,
    table_pattern::{TablePattern, TablePatternIndex},
};

/// A hydrated sync config.
///
/// This consists of all bucket data sources and parameter index lookup creators to evaluate. These
/// may come from multiple sync plans originally (the rest of the service calls that mixing
/// incremental reprocessing), but must all share the same compatibility context.
///
/// To decode and evaluate rows efficiently, we group data and parameter sources by their matched
/// tables (as a [ProcessorGroup]).
pub struct HydratedSyncConfig<E: ReplicationEmbedder> {
    pub compatibility: CompatibilityContext,
    pub groups: TablePatternIndex<ProcessorGroup<E>>,
    /// Interned column names and aliases referenced by processors in [Self::groups].
    pub interner: StringInterner,
}

impl<E: ReplicationEmbedder> HydratedSyncConfig<E> {
    pub fn new(compatibility: CompatibilityContext, sources: &[HydratedSource<E>]) -> Self {
        // Flatten sources into one member per table pattern, so that the pattern index can tell
        // us which members to include in each group.
        let mut patterns: Vec<TablePattern> = vec![];
        let mut members: Vec<PatternMember<E>> = vec![];
        let mut interner = StringInterner::default();

        for (source_index, source) in sources.iter().enumerate() {
            match source {
                HydratedSource::Bucket { id, sources } => {
                    for source in *sources {
                        patterns.push(source.processor.source_table.clone());
                        members.push(PatternMember::Data(id, source));
                    }
                }
                HydratedSource::Parameter { id, source } => {
                    patterns.push(source.processor.source_table.clone());
                    members.push(PatternMember::Parameter(id, source));
                }
                HydratedSource::Event {
                    id,
                    patterns: event_patterns,
                } => {
                    for pattern in *event_patterns {
                        patterns.push(pattern.clone());
                        members.push(PatternMember::Event { id, source_index });
                    }
                }
            }
        }

        let groups = TablePatternIndex::build(&patterns, |matching| {
            let mut group = ProcessorGroup::default();
            // Events only need to be included once, even if multiple of their patterns match.
            // Patterns of the same event are adjacent, so it's enough to track the last one.
            let mut last_event = None;

            for &index in matching {
                match &members[index] {
                    PatternMember::Data(id, source) => {
                        group.include_data(&mut interner, Arc::clone(id), source)
                    }
                    PatternMember::Parameter(id, source) => {
                        group.include_parameter(&mut interner, Arc::clone(id), source)
                    }
                    PatternMember::Event { id, source_index } => {
                        if last_event != Some(*source_index) {
                            last_event = Some(*source_index);
                            group.include_event(Arc::clone(id));
                        }
                    }
                }
            }

            group
        });

        Self {
            compatibility,
            groups,
            interner,
        }
    }
}

/// A trait defining how processed rows are formatted or saved.
pub trait ReplicationEmbedder {
    /// Something that, along with bucket parameters, uniquely identifies a bucket.
    ///
    /// This is typically a prefix (and an id of the replication stream).
    type BucketId: Debug + Send;

    /// Something that uniquely identifies a parameter lookup query.
    ///
    /// This is typically a lookup name and query id.
    type ParameterId: Debug + Send;

    /// Something that uniquely identifies an event source.
    ///
    /// This is typically an arbitrary id string.
    type EventId: Debug + Send;
}

pub enum HydratedSource<'a, E: ReplicationEmbedder> {
    Bucket {
        id: Arc<E::BucketId>,
        sources: &'a [StreamDataSource],
    },
    Parameter {
        id: Arc<E::ParameterId>,
        source: &'a StreamParameterIndexLookupCreator,
    },
    Event {
        id: Arc<E::EventId>,
        patterns: &'a [TablePattern],
    },
}

/// A source associated with a single [TablePattern], used while building the
/// [HydratedSyncConfig::groups] index.
enum PatternMember<'a, E: ReplicationEmbedder> {
    Data(&'a Arc<E::BucketId>, &'a StreamDataSource),
    Parameter(
        &'a Arc<E::ParameterId>,
        &'a StreamParameterIndexLookupCreator,
    ),
    Event {
        id: &'a Arc<E::EventId>,
        /// The index of the [HydratedSource] defining the event, used to avoid including the same
        /// event multiple times in a group.
        source_index: usize,
    },
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use super::{HydratedSource, HydratedSyncConfig, ReplicationEmbedder};
    use crate::sync_plan::{
        CompatibilityContext,
        processor_group::ProcessedGroupMember,
        table_pattern::{SourceTableRef, TablePattern},
    };

    struct TestEmbedder;

    impl ReplicationEmbedder for TestEmbedder {
        type BucketId = &'static str;
        type ParameterId = &'static str;
        type EventId = &'static str;
    }

    fn pattern(table: &str) -> TablePattern {
        let Ok(schema) = "public".parse();
        let Ok(table) = table.parse();

        TablePattern {
            connection_tag: "tag".to_string(),
            schema,
            table,
        }
    }

    fn events_for(config: &HydratedSyncConfig<TestEmbedder>, name: &str) -> Vec<&'static str> {
        let Some(group) = config.groups.lookup(SourceTableRef {
            connection_tag: "tag",
            schema: "public",
            name,
        }) else {
            return vec![];
        };

        group
            .processors
            .iter()
            .map(|p| match p {
                ProcessedGroupMember::Event(id) => **id,
                _ => panic!("expected event"),
            })
            .collect()
    }

    #[test]
    fn groups_events() {
        let all = [pattern("%")];
        let users = [pattern("user%"), pattern("users")];

        let config = HydratedSyncConfig::<TestEmbedder>::new(
            CompatibilityContext::default(),
            &[
                HydratedSource::Event {
                    id: Arc::new("all"),
                    patterns: &all,
                },
                HydratedSource::Event {
                    id: Arc::new("users"),
                    patterns: &users,
                },
            ],
        );

        assert_eq!(events_for(&config, "accounts"), ["all"]);
        assert_eq!(events_for(&config, "user_x"), ["all", "users"]);
        // Both patterns of the users event match, but it should only be included once.
        assert_eq!(events_for(&config, "users"), ["all", "users"]);

        let a = config.groups.lookup(SourceTableRef {
            connection_tag: "tag",
            schema: "public",
            name: "users",
        });
        let b = config.groups.lookup(SourceTableRef {
            connection_tag: "tag",
            schema: "public",
            name: "accounts",
        });
        let (ProcessedGroupMember::Event(in_users), ProcessedGroupMember::Event(in_accounts)) =
            (&a.unwrap().processors[0], &b.unwrap().processors[0])
        else {
            unreachable!()
        };
        // Ids are shared between groups.
        assert!(Arc::ptr_eq(in_users, in_accounts));
    }
}
