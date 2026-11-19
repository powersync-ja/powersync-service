use std::{fmt::Debug, sync::Arc};

use crate::sync_plan::{
    StringInterner,
    compatibility::CompatibilityContext,
    plan::{EventDescriptor, StreamDataSource, StreamParameterIndexLookupCreator},
    processor_group::{ProcessedGroupMember, ProcessorGroup},
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
        let mut members: Vec<(&TablePattern, PatternMember<E>)> = vec![];
        let mut interner = StringInterner::default();

        for source in sources {
            match source {
                HydratedSource::Bucket { id, source } => {
                    let pattern = &source.processor.source_table;
                    members.push((pattern, PatternMember::Data(id, source)));
                }
                HydratedSource::Parameter { id, source } => {
                    let pattern = &source.processor.source_table;
                    members.push((pattern, PatternMember::Parameter(id, source)));
                }
                HydratedSource::Event { id, descriptor } => {
                    for pattern in &descriptor.patterns {
                        members.push((pattern, PatternMember::Event(id)));
                    }
                }
            }
        }

        let groups = TablePatternIndex::build(
            &members,
            |(pattern, _)| pattern,
            |group: &mut ProcessorGroup<E>, (_, member)| match member {
                PatternMember::Data(id, source) => {
                    group.include_data(&mut interner, Arc::clone(id), source)
                }
                PatternMember::Parameter(id, source) => {
                    group.include_parameter(&mut interner, Arc::clone(id), source)
                }
                PatternMember::Event(id) => {
                    // Events only need to be included once, even if multiple of their patterns
                    // match. Patterns of the same event are adjacent and added in order, so it's
                    // enough to check whether the last processor is this event.
                    let already_included = matches!(
                        group.processors.last(),
                        Some(ProcessedGroupMember::Event(last)) if Arc::ptr_eq(last, id)
                    );
                    if !already_included {
                        group.include_event(Arc::clone(id));
                    }
                }
            },
        );

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

pub enum HydratedSource<E: ReplicationEmbedder> {
    Bucket {
        id: Arc<E::BucketId>,
        source: StreamDataSource,
    },
    Parameter {
        id: Arc<E::ParameterId>,
        source: StreamParameterIndexLookupCreator,
    },
    Event {
        id: Arc<E::EventId>,
        descriptor: EventDescriptor,
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
    Event(&'a Arc<E::EventId>),
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use super::HydratedSyncConfig;
    use crate::sync_plan::{
        processor_group::ProcessedGroupMember,
        test::{TestEmbedder, config, event_source, source_table_ref},
    };

    fn events_for(config: &HydratedSyncConfig<TestEmbedder>, name: &str) -> Vec<&'static str> {
        let Some(group) = config.groups.lookup(source_table_ref(name)) else {
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
        let all = event_source("all", &["%"]);
        let users = event_source("users", &["user%", "users"]);

        let config = config(&[all, users]);

        assert_eq!(events_for(&config, "accounts"), ["all"]);
        assert_eq!(events_for(&config, "user_x"), ["all", "users"]);
        assert_eq!(events_for(&config, "users"), ["all", "users"]);

        let a = config.groups.lookup(source_table_ref("users")).unwrap();
        let b = config.groups.lookup(source_table_ref("accounts")).unwrap();
        let (ProcessedGroupMember::Event(in_users), ProcessedGroupMember::Event(in_accounts)) =
            (&a.processors[0], &b.processors[0])
        else {
            panic!()
        };
        // Ids are shared between groups.
        assert!(Arc::ptr_eq(in_users, in_accounts));
    }
}
