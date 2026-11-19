use std::{fmt::Debug, sync::Arc};

use crate::sync_plan::{
    StringInterner,
    compatibility::CompatibilityContext,
    plan::{EventDescriptor, StreamDataSource, StreamParameterIndexLookupCreator},
    processor_group::ProcessorGroup,
    table_pattern::TablePatternIndex,
};

/// A hydrated Sync Config.
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
        let mut interner = StringInterner::default();

        let groups = TablePatternIndex::build(
            sources,
            |source| match source {
                HydratedSource::Bucket { source, .. } => {
                    std::slice::from_ref(&source.processor.source_table)
                }
                HydratedSource::Parameter { source, .. } => {
                    std::slice::from_ref(&source.processor.source_table)
                }
                HydratedSource::Event { descriptor, .. } => descriptor.patterns.as_slice(),
            },
            |group: &mut ProcessorGroup<E>, source| match source {
                HydratedSource::Bucket { id, source } => {
                    group.include_data(&mut interner, Arc::clone(id), source)
                }
                HydratedSource::Parameter { id, source } => {
                    group.include_parameter(&mut interner, Arc::clone(id), source)
                }
                HydratedSource::Event { id, .. } => group.include_event(Arc::clone(id)),
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
