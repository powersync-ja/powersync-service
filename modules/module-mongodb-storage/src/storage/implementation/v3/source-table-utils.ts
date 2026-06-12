import { ReplicationAssertionError } from '@powersync/lib-services-framework';
import { ColumnDescriptor, storage } from '@powersync/service-core';
import {
  BucketDataSource,
  BucketDefinitionId,
  HydratedSyncConfig,
  MatchingSources,
  ParameterIndexId,
  ParameterIndexLookupCreator
} from '@powersync/service-sync-rules';
import * as bson from 'bson';
import { BucketDefinitionMapping } from '../BucketDefinitionMapping.js';
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

export interface SourceTableMembershipIdSets {
  bucketDataSourceIds: Set<BucketDefinitionId>;
  parameterLookupSourceIds: Set<ParameterIndexId>;
}

export interface SourceTableRetentionPlanningContext {
  source: {
    schema: string;
    name: string;
    connectionTag: string;
    storeCurrentData: boolean;
  };
  config: {
    forceNarrowing: boolean;
    syncConfig: HydratedSyncConfig;
    mapping: BucketDefinitionMapping;
  };
  desired: {
    membershipIds: SourceTableMembershipIdSets;
    triggersEvent: boolean;
    bucketSourceById: Map<string, BucketDataSource>;
    parameterLookupSourceById: Map<string, ParameterIndexLookupCreator>;
  };
}

export interface SourceTableRetentionPlan {
  coveredMembershipIds: SourceTableMembershipIdSets;
  retainedDocIds: bson.ObjectId[];
  tables: storage.SourceTable[];
  narrowingUpdates: SourceTableMembershipUpdate[];
}

export interface SourceTableMembershipUpdate {
  id: bson.ObjectId;
  memberships: SourceTableMembershipIds;
}

export interface SourceTableDesiredResolution {
  membershipIds: SourceTableMembershipIdSets;
  bucketSourceById: Map<string, BucketDataSource>;
  parameterLookupSourceById: Map<string, ParameterIndexLookupCreator>;
}

export interface NewSourceTable {
  doc: SourceTableDocumentV3;
  table: storage.SourceTable;
}

function sameStringArray(left: string[], right: string[]) {
  return left.length == right.length && left.every((value, index) => value == right[index]);
}

export function sourceTableDesiredResolution(
  matchingSources: MatchingSources,
  mapping: BucketDefinitionMapping
): SourceTableDesiredResolution {
  const bucketSourceById = new Map(
    matchingSources.bucketDataSources.map((source) => [mapping.bucketSourceId(source), source] as const)
  );
  const parameterLookupSourceById = new Map(
    matchingSources.parameterLookupSources.map((source) => [mapping.parameterLookupId(source), source] as const)
  );

  return {
    bucketSourceById,
    parameterLookupSourceById,
    membershipIds: {
      bucketDataSourceIds: new Set(bucketSourceById.keys()),
      parameterLookupSourceIds: new Set(parameterLookupSourceById.keys())
    }
  };
}

export function planSourceTableRetention(
  exactDocs: SourceTableDocumentV3[],
  context: SourceTableRetentionPlanningContext
): SourceTableRetentionPlan {
  return new SourceTableRetentionPlanner(context).plan(exactDocs);
}

export function intersectSourceTableMembershipIds(
  doc: SourceTableDocumentV3,
  desired: SourceTableMembershipIdSets
): SourceTableMembershipIds {
  return {
    bucketDataSourceIds: doc.bucket_data_source_ids.filter((id) => desired.bucketDataSourceIds.has(id)),
    parameterLookupSourceIds: doc.parameter_lookup_source_ids.filter((id) => desired.parameterLookupSourceIds.has(id))
  };
}

export function uncoveredSourceTableMembershipIds(
  desired: SourceTableMembershipIdSets,
  covered: SourceTableMembershipIdSets
): SourceTableMembershipIds {
  return {
    bucketDataSourceIds: [...desired.bucketDataSourceIds].filter((id) => !covered.bucketDataSourceIds.has(id)),
    parameterLookupSourceIds: [...desired.parameterLookupSourceIds].filter(
      (id) => !covered.parameterLookupSourceIds.has(id)
    )
  };
}

export function hasSourceTableMembershipIds(memberships: SourceTableMembershipIds) {
  return memberships.bucketDataSourceIds.length > 0 || memberships.parameterLookupSourceIds.length > 0;
}

export function hasSourceTableMembershipIdSets(memberships: SourceTableMembershipIdSets) {
  return memberships.bucketDataSourceIds.size > 0 || memberships.parameterLookupSourceIds.size > 0;
}

export function sameSourceTableMembershipIds(doc: SourceTableDocumentV3, memberships: SourceTableMembershipIds) {
  return (
    sameStringArray(doc.bucket_data_source_ids, memberships.bucketDataSourceIds) &&
    sameStringArray(doc.parameter_lookup_source_ids, memberships.parameterLookupSourceIds)
  );
}

class SourceTableRetentionPlanner {
  private readonly coveredMembershipIds = {
    bucketDataSourceIds: new Set<string>(),
    parameterLookupSourceIds: new Set<string>()
  };
  private readonly retainedDocIds: SourceTableDocumentV3['_id'][] = [];
  private readonly tables: storage.SourceTable[] = [];
  private readonly narrowingUpdates: SourceTableMembershipUpdate[] = [];
  private retainedEventOnlyTable = false;

  constructor(private readonly context: SourceTableRetentionPlanningContext) {}

  plan(exactDocs: SourceTableDocumentV3[]): SourceTableRetentionPlan {
    for (const doc of exactDocs) {
      this.retainDoc(doc);
    }

    return {
      coveredMembershipIds: this.coveredMembershipIds,
      retainedDocIds: this.retainedDocIds,
      tables: this.tables,
      narrowingUpdates: this.narrowingUpdates
    };
  }

  private retainDoc(doc: SourceTableDocumentV3) {
    const memberships = intersectSourceTableMembershipIds(doc, this.context.desired.membershipIds);
    const coversDesiredMembership = hasSourceTableMembershipIds(memberships);
    const coversEventOnlyTable =
      !this.desiredHasMembership() && this.context.desired.triggersEvent && !this.retainedEventOnlyTable;

    this.recordCoverage(doc, memberships);
    this.planNarrowingUpdate(doc, memberships, coversDesiredMembership, coversEventOnlyTable);

    if (coversDesiredMembership || coversEventOnlyTable) {
      this.retainedEventOnlyTable ||= coversEventOnlyTable;
      this.retainedDocIds.push(doc._id);
      this.tables.push(this.sourceTableFor(doc, memberships));
    }
  }

  private desiredHasMembership() {
    return hasSourceTableMembershipIdSets(this.context.desired.membershipIds);
  }

  private recordCoverage(doc: SourceTableDocumentV3, memberships: SourceTableMembershipIds) {
    const { schema, name } = this.context.source;

    // Membership sets must be pairwise disjoint across the docs of one physical table:
    // each desired id is covered by exactly one doc, so each definition receives each
    // row exactly once. The algorithm maintains this (new docs only get uncovered ids,
    // narrowing only removes ids) - overlap means the persisted state is corrupt.
    for (const id of memberships.bucketDataSourceIds) {
      if (this.coveredMembershipIds.bucketDataSourceIds.has(id)) {
        throw new ReplicationAssertionError(
          `Source table ${doc._id} duplicates coverage of bucket data source ${id} for ${schema}.${name}`
        );
      }
      this.coveredMembershipIds.bucketDataSourceIds.add(id);
    }
    for (const id of memberships.parameterLookupSourceIds) {
      if (this.coveredMembershipIds.parameterLookupSourceIds.has(id)) {
        throw new ReplicationAssertionError(
          `Source table ${doc._id} duplicates coverage of parameter lookup source ${id} for ${schema}.${name}`
        );
      }
      this.coveredMembershipIds.parameterLookupSourceIds.add(id);
    }
  }

  private planNarrowingUpdate(
    doc: SourceTableDocumentV3,
    memberships: SourceTableMembershipIds,
    coversDesiredMembership: boolean,
    coversEventOnlyTable: boolean
  ) {
    const shouldNarrow =
      (this.context.config.forceNarrowing || coversDesiredMembership || coversEventOnlyTable) &&
      !doc.snapshot_done &&
      !sameSourceTableMembershipIds(doc, memberships);

    if (!shouldNarrow) {
      return;
    }

    this.narrowingUpdates.push({
      id: doc._id,
      memberships
    });
  }

  private sourceTableFor(doc: SourceTableDocumentV3, memberships: SourceTableMembershipIds): storage.SourceTable {
    const { source, config, desired } = this.context;
    const table = sourceTableFromDocument(
      {
        ...doc,
        bucket_data_source_ids: memberships.bucketDataSourceIds,
        parameter_lookup_source_ids: memberships.parameterLookupSourceIds
      },
      source.connectionTag,
      config.syncConfig,
      config.mapping,
      {
        bucketDataSources: memberships.bucketDataSourceIds.map((id) => desired.bucketSourceById.get(id)!),
        parameterLookupSources: memberships.parameterLookupSourceIds.map(
          (id) => desired.parameterLookupSourceById.get(id)!
        )
      }
    );
    table.storeCurrentData = source.storeCurrentData;
    return table;
  }
}

export function sameReplicaIdColumns(left: ReplicaIdColumn[] | undefined, right: ReplicaIdColumn[]) {
  return (
    left != null &&
    left.length == right.length &&
    left.every(
      (column, index) =>
        column.name == right[index].name && column.type == right[index].type && column.type_oid == right[index].type_oid
    )
  );
}

export function matchingSourceTableIdentity(doc: SourceTableDocumentV3, identity: SourceTableIdentity) {
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

export function createNewSourceTable(options: {
  id: bson.ObjectId;
  connectionId: number;
  source: storage.SourceEntityDescriptor;
  replicaIdColumns: ReplicaIdColumn[];
  memberships: SourceTableMembershipIds;
  syncConfig: HydratedSyncConfig;
  mapping: BucketDefinitionMapping;
  bucketSourceById: Map<string, BucketDataSource>;
  parameterLookupSourceById: Map<string, ParameterIndexLookupCreator>;
}): NewSourceTable {
  const { id, connectionId, source, replicaIdColumns, memberships, syncConfig, mapping } = options;
  const doc: SourceTableDocumentV3 = {
    _id: id,
    connection_id: connectionId,
    relation_id: source.objectId,
    schema_name: source.schema,
    table_name: source.name,
    replica_id_columns: replicaIdColumns,
    snapshot_done: false,
    snapshot_status: undefined,
    bucket_data_source_ids: memberships.bucketDataSourceIds,
    parameter_lookup_source_ids: memberships.parameterLookupSourceIds
  };
  const table = sourceTableFromDocument(doc, source.connectionTag, syncConfig, mapping, {
    bucketDataSources: memberships.bucketDataSourceIds.map((id) => options.bucketSourceById.get(id)!),
    parameterLookupSources: memberships.parameterLookupSourceIds.map((id) => options.parameterLookupSourceById.get(id)!)
  });
  table.storeCurrentData = source.sendsCompleteRows !== true;

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

export function conflictingSourceTableDocs(
  candidateDocs: SourceTableDocumentV3[],
  retainedDocIds: bson.ObjectId[],
  currentIdentity: SourceTableIdentity,
  options: { dropSameIdentity: boolean }
) {
  const retainedDocIdStrings = new Set(retainedDocIds.map((id) => id.toHexString()));
  return candidateDocs.filter(
    (doc) =>
      !retainedDocIdStrings.has(doc._id.toHexString()) &&
      (options.dropSameIdentity || !matchingSourceTableIdentity(doc, currentIdentity))
  );
}

export function sourceTableMembershipsFromDocument(
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
  memberships?: MatchingSources
): storage.SourceTable {
  const resolvedMemberships = memberships ?? sourceTableMembershipsFromDocument(doc, syncConfig, mapping);
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
    snapshotComplete: doc.snapshot_done ?? true,
    bucketDataSources: resolvedMemberships.bucketDataSources,
    parameterLookupSources: resolvedMemberships.parameterLookupSources
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
