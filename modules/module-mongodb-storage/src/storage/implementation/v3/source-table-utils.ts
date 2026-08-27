import { ReplicationAssertionError } from '@powersync/lib-services-framework';
import { BucketDefinitionMapping, ColumnDescriptor, JsonValue, storage } from '@powersync/service-core';
import {
  BucketDataSource,
  BucketDefinitionId,
  EventDefinitionId,
  HydratedEventDescriptor,
  HydratedSyncConfig,
  MatchingSources,
  ParameterIndexId,
  ParameterIndexLookupCreator,
  SourceTableRef
} from '@powersync/service-sync-rules';
import * as bson from 'bson';
import { ReplicaIdColumn, SourceTableDocumentV3 } from './models.js';

export interface SourceTableIdentity {
  schema: string;
  name: string;
  objectId: number | string | undefined;
  replicaIdColumns: ReplicaIdColumn[];
}

export interface SourceTableMembershipIds {
  bucketDataSourceIds: BucketDefinitionId[];
  parameterLookupSourceIds: ParameterIndexId[];
  eventDefinitionIds: EventDefinitionId[];
}

export interface SourceTableDesiredResolution {
  bucketSourceById: Map<BucketDefinitionId, BucketDataSource>;
  parameterLookupSourceById: Map<ParameterIndexId, ParameterIndexLookupCreator>;
  eventDefinitionById: Map<EventDefinitionId, HydratedEventDescriptor>;
}

export interface SourceTableReconciliationContext {
  connectionId: number;
  connectionTag: string;
  identity: SourceTableIdentity;
  storeCurrentData: boolean;
  syncConfig: HydratedSyncConfig;
  mapping: BucketDefinitionMapping;
  desired: SourceTableDesiredResolution;
  /**
   * Candidates the source connector considers safe to reuse.
   */
  sourceCompatibleTables: readonly storage.SourceTableCandidate[];
  /**
   * Source metadata for records created by this resolution.
   */
  newTableSourceMetadata: JsonValue;
}

export interface SourceTableReconciliationPlan {
  /** Retained source tables, with memberships narrowed to the desired set. */
  tables: storage.SourceTable[];
  /** Membership narrowing to persist for retained snapshot-incomplete docs. */
  narrowingUpdates: SourceTableMembershipUpdate[];
  /**
   * Memberships for a new source-table doc covering desired ids no existing doc covers,
   * or null if no new doc is needed.
   */
  newTableMemberships: SourceTableMembershipIds | null;
  /** Identity-overlapping docs that conflict with the current identity and must be dropped. */
  dropDocs: SourceTableDocumentV3[];
}

export interface SourceTableMembershipUpdate {
  id: bson.ObjectId;
  memberships: SourceTableMembershipIds;
}

export interface NewSourceTable {
  doc: SourceTableDocumentV3;
  table: storage.SourceTable;
}

export function sourceTableDesiredResolution(
  syncConfig: HydratedSyncConfig,
  ref: SourceTableRef,
  mapping: BucketDefinitionMapping,
  eventById: ReadonlyMap<EventDefinitionId, HydratedEventDescriptor>
): SourceTableDesiredResolution {
  const matchingSources = syncConfig.getMatchingSources(ref);
  return {
    bucketSourceById: new Map(
      matchingSources.bucketDataSources.map((source) => [mapping.bucketSourceId(source), source] as const)
    ),
    parameterLookupSourceById: new Map(
      matchingSources.parameterLookupSources.map((source) => [mapping.parameterLookupId(source), source] as const)
    ),
    eventDefinitionById: new Map([...eventById].filter(([, event]) => event.tableTriggersEvent(ref)))
  };
}

export function planSourceTableReconciliation(
  candidateDocs: SourceTableDocumentV3[],
  context: SourceTableReconciliationContext
): SourceTableReconciliationPlan {
  return new SourceTableReconciliationPlanner(context).plan(candidateDocs);
}

function sameStringArray(left: string[], right: string[]) {
  return left.length == right.length && left.every((value, index) => value == right[index]);
}

function matchingSourcesFor(
  desired: SourceTableDesiredResolution,
  memberships: SourceTableMembershipIds
): MatchingSources {
  return {
    bucketDataSources: memberships.bucketDataSourceIds.map((id) => desired.bucketSourceById.get(id)!),
    parameterLookupSources: memberships.parameterLookupSourceIds.map((id) => desired.parameterLookupSourceById.get(id)!)
  };
}

function intersectMembershipIds(
  doc: SourceTableDocumentV3,
  desired: SourceTableDesiredResolution
): SourceTableMembershipIds {
  return {
    bucketDataSourceIds: doc.bucket_data_source_ids.filter((id) => desired.bucketSourceById.has(id)),
    parameterLookupSourceIds: doc.parameter_lookup_source_ids.filter((id) => desired.parameterLookupSourceById.has(id)),
    eventDefinitionIds: doc.event_definition_ids.filter((id) => desired.eventDefinitionById.has(id))
  };
}

function hasMembershipIds(memberships: SourceTableMembershipIds) {
  return (
    memberships.bucketDataSourceIds.length > 0 ||
    memberships.parameterLookupSourceIds.length > 0 ||
    memberships.eventDefinitionIds.length > 0
  );
}

function sameMembershipIds(doc: SourceTableDocumentV3, memberships: SourceTableMembershipIds) {
  return (
    sameStringArray(doc.bucket_data_source_ids, memberships.bucketDataSourceIds) &&
    sameStringArray(doc.parameter_lookup_source_ids, memberships.parameterLookupSourceIds) &&
    sameStringArray(doc.event_definition_ids, memberships.eventDefinitionIds)
  );
}

class SourceTableReconciliationPlanner {
  private readonly coveredBucketDataSourceIds = new Set<string>();
  private readonly coveredParameterLookupSourceIds = new Set<string>();
  private readonly coveredEventDefinitionIds = new Set<string>();
  private readonly tables: storage.SourceTable[] = [];
  private readonly narrowingUpdates: SourceTableMembershipUpdate[] = [];

  constructor(private readonly context: SourceTableReconciliationContext) {}

  plan(candidateDocs: SourceTableDocumentV3[]): SourceTableReconciliationPlan {
    for (const doc of candidateDocs) {
      if (this.isCompatible(doc)) {
        this.retainDoc(doc);
      }
    }

    return {
      tables: this.tables,
      narrowingUpdates: this.narrowingUpdates,
      newTableMemberships: this.newTableMemberships(),
      // Compatible records remain available for reuse even when not needed by this config.
      dropDocs: candidateDocs.filter((doc) => !this.isCompatible(doc))
    };
  }

  private isCompatible(doc: SourceTableDocumentV3) {
    return this.compatibleTableFor(doc) != null;
  }

  private compatibleTableFor(doc: SourceTableDocumentV3): storage.SourceTableCandidate | undefined {
    return this.context.sourceCompatibleTables.find((table) => storage.sourceTableIdEquals(table.id, doc._id));
  }

  private retainDoc(doc: SourceTableDocumentV3) {
    const memberships = intersectMembershipIds(doc, this.context.desired);
    const coversDesiredMembership = hasMembershipIds(memberships);

    this.recordCoverage(doc, memberships);
    this.planNarrowingUpdate(doc, memberships, coversDesiredMembership);

    if (coversDesiredMembership) {
      this.tables.push(this.sourceTableFor(doc, memberships));
    }
  }

  private recordCoverage(doc: SourceTableDocumentV3, memberships: SourceTableMembershipIds) {
    this.addCoverage(doc, 'bucket data source', this.coveredBucketDataSourceIds, memberships.bucketDataSourceIds);
    this.addCoverage(
      doc,
      'parameter lookup source',
      this.coveredParameterLookupSourceIds,
      memberships.parameterLookupSourceIds
    );
    this.addCoverage(doc, 'event definition', this.coveredEventDefinitionIds, memberships.eventDefinitionIds);
  }

  // Membership sets must be pairwise disjoint across the docs of one physical table:
  // each desired id is covered by exactly one doc, so each definition is evaluated through
  // only one SourceTable for a physical row. Different docs may own different event ids.
  // The algorithm maintains this (new docs only get uncovered ids, narrowing only removes ids) -
  // overlap means the persisted state is corrupt.
  private addCoverage(doc: SourceTableDocumentV3, kind: string, covered: Set<string>, ids: string[]) {
    const { schema, name } = this.context.identity;
    for (const id of ids) {
      if (covered.has(id)) {
        throw new ReplicationAssertionError(
          `Source table ${doc._id} duplicates coverage of ${kind} ${id} for ${schema}.${name}`
        );
      }
      covered.add(id);
    }
  }

  private planNarrowingUpdate(
    doc: SourceTableDocumentV3,
    memberships: SourceTableMembershipIds,
    coversDesiredMembership: boolean
  ) {
    const shouldNarrow = coversDesiredMembership && !doc.snapshot_done && !sameMembershipIds(doc, memberships);

    if (!shouldNarrow) {
      return;
    }

    this.narrowingUpdates.push({
      id: doc._id,
      memberships
    });
  }

  private newTableMemberships(): SourceTableMembershipIds | null {
    const { desired } = this.context;
    const uncovered: SourceTableMembershipIds = {
      bucketDataSourceIds: [...desired.bucketSourceById.keys()].filter(
        (id) => !this.coveredBucketDataSourceIds.has(id)
      ),
      parameterLookupSourceIds: [...desired.parameterLookupSourceById.keys()].filter(
        (id) => !this.coveredParameterLookupSourceIds.has(id)
      ),
      eventDefinitionIds: [...desired.eventDefinitionById.keys()].filter(
        (id) => !this.coveredEventDefinitionIds.has(id)
      )
    };
    if (hasMembershipIds(uncovered)) {
      return uncovered;
    }
    return null;
  }

  private sourceTableFor(doc: SourceTableDocumentV3, memberships: SourceTableMembershipIds): storage.SourceTable {
    const { connectionTag, syncConfig, mapping, desired, storeCurrentData } = this.context;
    const built = sourceTableFromDocument(
      doc,
      connectionTag,
      syncConfig,
      mapping,
      desired.eventDefinitionById,
      matchingSourcesFor(desired, memberships),
      memberships
    );
    // Use any metadata update returned by the reconciler.
    const resolved = this.compatibleTableFor(doc);
    const table = resolved == null ? built : built.withSourceMetadata(resolved.sourceMetadata);
    table.storeCurrentData = storeCurrentData;
    return table;
  }
}

export function overlappingSourceTableFilter(
  connectionId: number,
  identity: Pick<SourceTableIdentity, 'schema' | 'name' | 'objectId'>
): Record<string, unknown> {
  const clauses = [{ schema_name: identity.schema, table_name: identity.name }] as Record<string, unknown>[];
  if (identity.objectId != null) {
    clauses.push({ relation_id: identity.objectId });
  }

  return {
    connection_id: connectionId,
    $or: clauses
  };
}

export function createNewSourceTable(
  id: bson.ObjectId,
  memberships: SourceTableMembershipIds,
  context: SourceTableReconciliationContext
): NewSourceTable {
  const {
    connectionId,
    connectionTag,
    identity,
    syncConfig,
    mapping,
    desired,
    storeCurrentData,
    newTableSourceMetadata
  } = context;
  const doc: SourceTableDocumentV3 = {
    _id: id,
    connection_id: connectionId,
    relation_id: identity.objectId,
    schema_name: identity.schema,
    table_name: identity.name,
    replica_id_columns: identity.replicaIdColumns,
    snapshot_done: false,
    snapshot_status: undefined,
    bucket_data_source_ids: memberships.bucketDataSourceIds,
    parameter_lookup_source_ids: memberships.parameterLookupSourceIds,
    event_definition_ids: memberships.eventDefinitionIds,
    // Records created together share the same source metadata.
    source_metadata: newTableSourceMetadata
  };
  const table = sourceTableFromDocument(
    doc,
    connectionTag,
    syncConfig,
    mapping,
    desired.eventDefinitionById,
    matchingSourcesFor(desired, memberships),
    memberships
  );
  table.storeCurrentData = storeCurrentData;

  return { doc, table };
}

function sourceTableMembershipsFromDocument(
  doc: SourceTableDocumentV3,
  syncConfig: HydratedSyncConfig,
  mapping: BucketDefinitionMapping
): MatchingSources {
  const bucketDataSourceIds = new Set(doc.bucket_data_source_ids);
  const parameterLookupSourceIds = new Set(doc.parameter_lookup_source_ids);

  return {
    bucketDataSources: syncConfig.bucketDataSources.filter((source) =>
      bucketDataSourceIds.has(mapping.bucketSourceId(source))
    ),
    parameterLookupSources: syncConfig.bucketParameterLookupSources.filter((source) =>
      parameterLookupSourceIds.has(mapping.parameterLookupId(source))
    )
  };
}

export function sourceTableFromDocument(
  doc: SourceTableDocumentV3,
  connectionTag: string,
  syncConfig: HydratedSyncConfig,
  mapping: BucketDefinitionMapping,
  eventById: ReadonlyMap<EventDefinitionId, HydratedEventDescriptor>,
  memberships?: MatchingSources,
  membershipIds?: SourceTableMembershipIds
): storage.SourceTable {
  const resolvedMemberships = memberships ?? sourceTableMembershipsFromDocument(doc, syncConfig, mapping);
  const resolvedMembershipIds = membershipIds ?? {
    bucketDataSourceIds: resolvedMemberships.bucketDataSources.map((source) => mapping.bucketSourceId(source)),
    parameterLookupSourceIds: resolvedMemberships.parameterLookupSources.map((source) =>
      mapping.parameterLookupId(source)
    ),
    eventDefinitionIds: doc.event_definition_ids.filter((id) => eventById.has(id))
  };
  const table = new storage.SourceTable({
    id: doc._id,
    ref: {
      connectionTag,
      schema: doc.schema_name,
      name: doc.table_name
    },
    objectId: doc.relation_id,
    replicaIdColumns:
      doc.replica_id_columns?.map(
        (column) => ({ name: column.name, typeId: column.type_oid, type: column.type }) satisfies ColumnDescriptor
      ) ?? [],
    snapshotComplete: doc.snapshot_done,
    bucketDataSources: resolvedMemberships.bucketDataSources,
    parameterLookupSources: resolvedMemberships.parameterLookupSources,
    bucketDataSourceIds: new Set(resolvedMembershipIds.bucketDataSourceIds),
    parameterLookupSourceIds: new Set(resolvedMembershipIds.parameterLookupSourceIds),
    eventDefinitionIds: new Set(resolvedMembershipIds.eventDefinitionIds),
    sourceMetadata: doc.source_metadata ?? null
  });
  table.syncData = table.bucketDataSources.length > 0;
  table.syncParameters = table.parameterLookupSources.length > 0;
  table.syncEvent = table.eventDefinitionIds!.size > 0;
  table.snapshotStatus =
    doc.snapshot_status == null
      ? undefined
      : {
          lastKey: doc.snapshot_status.last_key?.buffer ?? null,
          totalEstimatedCount: doc.snapshot_status.total_estimated_count,
          replicatedCount: doc.snapshot_status.replicated_count
        };
  return table;
}
