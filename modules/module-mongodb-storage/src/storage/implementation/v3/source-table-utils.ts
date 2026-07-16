import { ReplicationAssertionError } from '@powersync/lib-services-framework';
import { BucketDefinitionMapping, ColumnDescriptor, storage } from '@powersync/service-core';
import {
  BucketDataSource,
  BucketDefinitionId,
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
}

export interface SourceTableDesiredResolution {
  bucketSourceById: Map<BucketDefinitionId, BucketDataSource>;
  parameterLookupSourceById: Map<ParameterIndexId, ParameterIndexLookupCreator>;
  triggersEvent: boolean;
}

export interface SourceTableReconciliationContext {
  connectionId: number;
  connectionTag: string;
  identity: SourceTableIdentity;
  storeCurrentData: boolean;
  syncConfig: HydratedSyncConfig;
  mapping: BucketDefinitionMapping;
  desired: SourceTableDesiredResolution;
}

export interface SourceTableReconciliationPlan {
  /** Retained source tables, with memberships narrowed to the desired set. */
  tables: storage.SourceTable[];
  /** Membership narrowing to persist for retained snapshot-incomplete docs. */
  narrowingUpdates: SourceTableMembershipUpdate[];
  /**
   * Memberships for a new source-table doc covering desired ids no existing doc covers,
   * or null if no new doc is needed. Empty memberships indicate an event-only table.
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
  mapping: BucketDefinitionMapping
): SourceTableDesiredResolution {
  const matchingSources = syncConfig.getMatchingSources(ref);
  return {
    bucketSourceById: new Map(
      matchingSources.bucketDataSources.map((source) => [mapping.bucketSourceId(source), source] as const)
    ),
    parameterLookupSourceById: new Map(
      matchingSources.parameterLookupSources.map((source) => [mapping.parameterLookupId(source), source] as const)
    ),
    triggersEvent: syncConfig.tableTriggersEvent(ref)
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
    parameterLookupSourceIds: doc.parameter_lookup_source_ids.filter((id) => desired.parameterLookupSourceById.has(id))
  };
}

function hasMembershipIds(memberships: SourceTableMembershipIds) {
  return memberships.bucketDataSourceIds.length > 0 || memberships.parameterLookupSourceIds.length > 0;
}

function sameMembershipIds(doc: SourceTableDocumentV3, memberships: SourceTableMembershipIds) {
  return (
    sameStringArray(doc.bucket_data_source_ids, memberships.bucketDataSourceIds) &&
    sameStringArray(doc.parameter_lookup_source_ids, memberships.parameterLookupSourceIds)
  );
}

class SourceTableReconciliationPlanner {
  private readonly coveredBucketDataSourceIds = new Set<string>();
  private readonly coveredParameterLookupSourceIds = new Set<string>();
  private readonly retainedDocIds = new Set<string>();
  private readonly tables: storage.SourceTable[] = [];
  private readonly narrowingUpdates: SourceTableMembershipUpdate[] = [];
  private retainedEventOnlyTable = false;

  constructor(private readonly context: SourceTableReconciliationContext) {}

  plan(candidateDocs: SourceTableDocumentV3[]): SourceTableReconciliationPlan {
    const { identity } = this.context;
    for (const doc of candidateDocs) {
      if (matchingSourceTableIdentity(doc, identity)) {
        this.retainDoc(doc);
      }
    }

    return {
      tables: this.tables,
      narrowingUpdates: this.narrowingUpdates,
      newTableMemberships: this.newTableMemberships(),
      // Non-retained overlapping docs represent renames / relation-id changes /
      // replica-identity changes. Same-identity stale docs are retained in production.
      dropDocs: candidateDocs.filter(
        (doc) => !this.retainedDocIds.has(doc._id.toHexString()) && !matchingSourceTableIdentity(doc, identity)
      )
    };
  }

  private retainDoc(doc: SourceTableDocumentV3) {
    const memberships = intersectMembershipIds(doc, this.context.desired);
    const coversDesiredMembership = hasMembershipIds(memberships);
    const coversEventOnlyTable =
      !this.desiredHasMembership() && this.context.desired.triggersEvent && !this.retainedEventOnlyTable;

    this.recordCoverage(doc, memberships);
    this.planNarrowingUpdate(doc, memberships, coversDesiredMembership, coversEventOnlyTable);

    if (coversDesiredMembership || coversEventOnlyTable) {
      this.retainedEventOnlyTable ||= coversEventOnlyTable;
      this.retainedDocIds.add(doc._id.toHexString());
      this.tables.push(this.sourceTableFor(doc, memberships));
    }
  }

  private desiredHasMembership() {
    const { desired } = this.context;
    return desired.bucketSourceById.size > 0 || desired.parameterLookupSourceById.size > 0;
  }

  private recordCoverage(doc: SourceTableDocumentV3, memberships: SourceTableMembershipIds) {
    this.addCoverage(doc, 'bucket data source', this.coveredBucketDataSourceIds, memberships.bucketDataSourceIds);
    this.addCoverage(
      doc,
      'parameter lookup source',
      this.coveredParameterLookupSourceIds,
      memberships.parameterLookupSourceIds
    );
  }

  // Membership sets must be pairwise disjoint across the docs of one physical table:
  // each desired id is covered by exactly one doc, so each definition receives each
  // row exactly once. The algorithm maintains this (new docs only get uncovered ids,
  // narrowing only removes ids) - overlap means the persisted state is corrupt.
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
    coversDesiredMembership: boolean,
    coversEventOnlyTable: boolean
  ) {
    const shouldNarrow =
      (coversDesiredMembership || coversEventOnlyTable) && !doc.snapshot_done && !sameMembershipIds(doc, memberships);

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
      )
    };
    if (hasMembershipIds(uncovered) || (desired.triggersEvent && this.tables.length == 0)) {
      return uncovered;
    }
    return null;
  }

  private sourceTableFor(doc: SourceTableDocumentV3, memberships: SourceTableMembershipIds): storage.SourceTable {
    const { connectionTag, syncConfig, mapping, desired, storeCurrentData } = this.context;
    const table = sourceTableFromDocument(
      doc,
      connectionTag,
      syncConfig,
      mapping,
      matchingSourcesFor(desired, memberships),
      memberships
    );
    table.storeCurrentData = storeCurrentData;
    return table;
  }
}

function sameReplicaIdColumns(left: ReplicaIdColumn[] | undefined, right: ReplicaIdColumn[]) {
  return (
    left != null &&
    left.length == right.length &&
    left.every(
      (column, index) =>
        column.name == right[index].name && column.type == right[index].type && column.type_oid == right[index].type_oid
    )
  );
}

function matchingSourceTableIdentity(doc: SourceTableDocumentV3, identity: SourceTableIdentity) {
  return (
    doc.schema_name == identity.schema &&
    doc.table_name == identity.name &&
    (identity.objectId == null || doc.relation_id == identity.objectId) &&
    sameReplicaIdColumns(doc.replica_id_columns, identity.replicaIdColumns)
  );
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
  const { connectionId, connectionTag, identity, syncConfig, mapping, desired, storeCurrentData } = context;
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
    parameter_lookup_source_ids: memberships.parameterLookupSourceIds
  };
  const table = sourceTableFromDocument(
    doc,
    connectionTag,
    syncConfig,
    mapping,
    matchingSourcesFor(desired, memberships),
    memberships
  );
  table.storeCurrentData = storeCurrentData;

  return { doc, table };
}

export function designateEventCarrier(tables: storage.SourceTable[], triggersEvent: boolean) {
  if (!triggersEvent) {
    return;
  }

  const eventCarrier = tables.find((table) => table.snapshotComplete) ?? tables[0];
  for (const table of tables) {
    table.syncEvent = table === eventCarrier;
  }
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
  memberships?: MatchingSources,
  membershipIds?: SourceTableMembershipIds
): storage.SourceTable {
  const resolvedMemberships = memberships ?? sourceTableMembershipsFromDocument(doc, syncConfig, mapping);
  const resolvedMembershipIds = membershipIds ?? {
    bucketDataSourceIds: resolvedMemberships.bucketDataSources.map((source) => mapping.bucketSourceId(source)),
    parameterLookupSourceIds: resolvedMemberships.parameterLookupSources.map((source) =>
      mapping.parameterLookupId(source)
    )
  };
  const table = new storage.SourceTable({
    id: doc._id,
    ref: {
      connectionTag,
      schema: doc.schema_name,
      name: doc.table_name
    },
    objectId: doc.relation_id,
    replicaIdColumns: doc.replica_id_columns!.map(
      (c) => ({ name: c.name, typeId: c.type_oid, type: c.type }) satisfies ColumnDescriptor
    ),
    snapshotComplete: doc.snapshot_done,
    bucketDataSources: resolvedMemberships.bucketDataSources,
    parameterLookupSources: resolvedMemberships.parameterLookupSources,
    bucketDataSourceIds: new Set(resolvedMembershipIds.bucketDataSourceIds),
    parameterLookupSourceIds: new Set(resolvedMembershipIds.parameterLookupSourceIds)
  });
  table.syncData = table.bucketDataSources.length > 0;
  table.syncParameters = table.parameterLookupSources.length > 0;
  table.syncEvent = syncConfig.tableTriggersEvent(table.ref);
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
