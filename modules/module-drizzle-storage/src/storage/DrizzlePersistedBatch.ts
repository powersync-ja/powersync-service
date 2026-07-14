import type { Logger } from '@powersync/lib-services-framework';
import { ReplicationAssertionError } from '@powersync/lib-services-framework';
import { storage, utils } from '@powersync/service-core';
import { JSONBig } from '@powersync/service-jsonbig';
import * as sync_rules from '@powersync/service-sync-rules';
import { inArray } from 'drizzle-orm';
import * as uuid from 'uuid';
import { bucketData, bucketParameters, type CurrentDataRow } from '../drivers/sqlite/schema.js';
import type { DrizzleStorageTransaction } from '../drivers/sqlite/sqlite-config.js';
import type { DrizzleStorageDialect } from './DrizzleStorageDialect.js';

export interface DrizzlePersistedBatchOptions {
  tx: DrizzleStorageTransaction;
  dialect: DrizzleStorageDialect;
  logger: Logger;
  syncRules: sync_rules.HydratedSyncConfig;
  replicationStreamId: number;
  storeCurrentData: boolean;
  skipExistingRows: boolean;
  markRecordUnavailable: storage.BucketStorageMarkRecordUnavailable | undefined;
}

export type CurrentBucket = {
  bucket: string;
  table: string;
  id: string;
};

const MAX_ROW_SIZE = 15 * 1024 * 1024;

/**
 * Handles the writes for a single persisted operation chunk.
 *
 * `DrizzleBucketBatch` owns the public batch lifecycle, checkpointing, and
 * listener hooks. This class owns the transactional write state for bucket
 * data, parameter rows, and current data so large flushes can be split into
 * smaller persisted chunks without keeping every tracked entity in one unit of
 * work.
 */
export class DrizzlePersistedBatch {
  private readonly currentDataById = new Map<string, CurrentDataRow>();

  constructor(private readonly options: DrizzlePersistedBatchOptions) {}

  persistOperations(operations: storage.SaveOptions[], nextOpId: bigint): bigint {
    this.loadCurrentData(operations);
    for (const operation of operations) {
      nextOpId = this.persistOperation(operation, nextOpId);
    }
    return nextOpId;
  }

  persistBucketData(options: {
    table: storage.SourceTable;
    sourceKey: storage.ReplicaId;
    existingBuckets: CurrentBucket[];
    evaluated: sync_rules.EvaluatedRow[];
    nextOpId: bigint;
  }): bigint {
    const remainingBuckets = new Map(options.existingBuckets.map((bucket) => [currentBucketKey(bucket), bucket]));
    const serializedSourceKey = storage.serializeReplicaId(options.sourceKey);
    const deleteChecksum = utils.hashDelete(replicaIdToSubkey(options.table.id, options.sourceKey));
    let nextOpId = options.nextOpId;
    const values: (typeof bucketData.$inferInsert)[] = [];

    for (const row of options.evaluated) {
      remainingBuckets.delete(currentBucketKey(row));
      const data = JSONBig.stringify(row.data);
      const checksum = utils.hashData(row.table, row.id, data);
      values.push({
        id: uuid.v4(),
        groupId: this.options.replicationStreamId,
        bucketName: row.bucket,
        opId: nextOpId++,
        op: 'PUT',
        sourceTable: String(options.table.id),
        sourceKey: serializedSourceKey,
        tableName: row.table,
        rowId: row.id,
        checksum: BigInt(checksum),
        data,
        targetOp: null
      });
    }

    for (const bucket of remainingBuckets.values()) {
      values.push({
        id: uuid.v4(),
        groupId: this.options.replicationStreamId,
        bucketName: bucket.bucket,
        opId: nextOpId++,
        op: 'REMOVE',
        sourceTable: String(options.table.id),
        sourceKey: serializedSourceKey,
        tableName: bucket.table,
        rowId: bucket.id,
        checksum: BigInt(deleteChecksum),
        data: null,
        targetOp: null
      });
    }

    if (values.length > 0) {
      this.options.tx.insert(this.options.dialect.tables.bucketData).values(values).run();
    }

    return nextOpId;
  }

  persistParameterData(options: {
    table: storage.SourceTable;
    sourceKey: storage.ReplicaId;
    existingLookups: Buffer[];
    evaluated: sync_rules.EvaluatedParameters[];
    nextOpId: bigint;
  }): bigint {
    const remainingLookups = new Map(options.existingLookups.map((lookup) => [lookup.toString('base64'), lookup]));
    const serializedSourceKey = storage.serializeReplicaId(options.sourceKey);
    let nextOpId = options.nextOpId;
    const values: (typeof bucketParameters.$inferInsert)[] = [];

    for (const row of options.evaluated) {
      const lookup = storage.serializeLookupBuffer(row.lookup);
      remainingLookups.delete(lookup.toString('base64'));
      values.push({
        id: nextOpId++,
        groupId: this.options.replicationStreamId,
        sourceTable: String(options.table.id),
        sourceKey: serializedSourceKey,
        lookup,
        bucketParameters: JSONBig.stringify(row.bucketParameters)
      });
    }

    for (const lookup of remainingLookups.values()) {
      values.push({
        id: nextOpId++,
        groupId: this.options.replicationStreamId,
        sourceTable: String(options.table.id),
        sourceKey: serializedSourceKey,
        lookup,
        bucketParameters: '[]'
      });
    }

    if (values.length > 0) {
      this.options.tx.insert(this.options.dialect.tables.bucketParameters).values(values).run();
    }

    return nextOpId;
  }

  private persistOperation(record: storage.SaveOptions, nextOpId: bigint): bigint {
    const sourceTable = record.sourceTable;
    const tableId = String(sourceTable.id);
    const afterId = record.afterReplicaId ?? null;
    const beforeId = record.beforeReplicaId ?? record.afterReplicaId;
    const serializedBeforeId = storage.serializeReplicaId(beforeId);
    const existingCurrentDataId = currentDataId(this.options.replicationStreamId, tableId, serializedBeforeId);
    const existingCurrentData = this.currentDataById.get(existingCurrentDataId) ?? null;

    const storeCurrentData = this.options.storeCurrentData && sourceTable.storeCurrentData;
    let existingBuckets = currentBuckets(existingCurrentData);
    let existingLookups = currentLookups(existingCurrentData);
    let after: sync_rules.ToastableSqliteRow | null | undefined = record.after;

    if (this.options.skipExistingRows) {
      if (record.tag == storage.SaveOperationTag.INSERT) {
        if (existingCurrentData != null) {
          return nextOpId;
        }
      } else {
        throw new ReplicationAssertionError(`${record.tag} not supported with skipExistingRows: true`);
      }
    }

    if (record.tag == storage.SaveOperationTag.UPDATE) {
      if (existingCurrentData != null && storeCurrentData) {
        after = storage.mergeToast(record.after, storage.deserializeBson(Buffer.from(existingCurrentData.data)));
      } else if (existingCurrentData == null && storeCurrentData) {
        this.options.markRecordUnavailable?.(record);
      }
    }

    if (beforeId != null && (afterId == null || !storage.replicaIdEquals(beforeId, afterId))) {
      if (sourceTable.syncData) {
        nextOpId = this.persistBucketData({
          table: sourceTable,
          sourceKey: beforeId,
          existingBuckets,
          evaluated: [],
          nextOpId
        });
        existingBuckets = [];
      }

      if (sourceTable.syncParameters) {
        nextOpId = this.persistParameterData({
          table: sourceTable,
          sourceKey: beforeId,
          existingLookups,
          evaluated: [],
          nextOpId
        });
        existingLookups = [];
      }
    }

    let newBuckets: CurrentBucket[] = [];
    let newLookups: Buffer[] = [];
    let afterData: Buffer | undefined;
    let afterDataWasTruncated = false;
    if (afterId != null && after != null && utils.isCompleteRow(storeCurrentData, after)) {
      if (storeCurrentData) {
        const prepared = this.serializeCurrentData(record, after);
        after = prepared.after;
        afterData = prepared.data;
        afterDataWasTruncated = prepared.truncated;
      } else {
        afterData = storage.serializeBson({});
      }

      if (sourceTable.syncData) {
        const { results: rawResults, errors } = this.options.syncRules.evaluateRowWithErrors({
          record: after as sync_rules.SqliteRow,
          sourceTable: sourceTable.ref,
          bucketDataSources: sourceTable.bucketDataSources
        });
        const results = afterDataWasTruncated ? rawResults.filter(hasUsableObjectId) : rawResults;
        for (const error of errors) {
          this.options.logger.error(
            `Failed to evaluate data query on ${sourceTable.qualifiedName}.${after.id}: ${error.error}`
          );
        }
        nextOpId = this.persistBucketData({
          table: sourceTable,
          sourceKey: afterId,
          existingBuckets,
          evaluated: results,
          nextOpId
        });
        newBuckets = results.map((row) => ({
          bucket: row.bucket,
          table: row.table,
          id: row.id
        }));
      }

      if (sourceTable.syncParameters) {
        const { results, errors } = this.options.syncRules.evaluateParameterRowWithErrors(
          sourceTable.ref,
          after as sync_rules.SqliteRow,
          {
            parameterLookupSources: sourceTable.parameterLookupSources
          }
        );
        for (const error of errors) {
          this.options.logger.error(
            `Failed to evaluate parameter query on ${sourceTable.qualifiedName}.${after.id}: ${error.error}`
          );
        }
        nextOpId = this.persistParameterData({
          table: sourceTable,
          sourceKey: afterId,
          existingLookups,
          evaluated: results,
          nextOpId
        });
        newLookups = results.map((row) => storage.serializeLookupBuffer(row.lookup));
      }
    }

    if (afterId != null && afterData != null) {
      this.upsertCurrentData({
        tableId,
        sourceKey: afterId,
        buckets: newBuckets,
        lookups: newLookups,
        data: afterData,
        pendingDelete: null
      });
    }

    if (afterId == null || !storage.replicaIdEquals(beforeId, afterId)) {
      nextOpId = this.deleteCurrentData(tableId, beforeId, nextOpId);
    }

    return nextOpId;
  }

  private loadCurrentData(operations: storage.SaveOptions[]): void {
    const ids = new Set<string>();
    for (const operation of operations) {
      const tableId = String(operation.sourceTable.id);
      const beforeId = operation.beforeReplicaId ?? operation.afterReplicaId;
      ids.add(currentDataId(this.options.replicationStreamId, tableId, storage.serializeReplicaId(beforeId)));

      const afterId = operation.afterReplicaId ?? null;
      if (afterId != null) {
        ids.add(currentDataId(this.options.replicationStreamId, tableId, storage.serializeReplicaId(afterId)));
      }
    }

    const rows =
      ids.size == 0
        ? []
        : this.options.tx
            .select()
            .from(this.options.dialect.tables.currentData)
            .where(inArray(this.options.dialect.tables.currentData.id, [...ids]))
            .all();
    for (const row of rows) {
      this.currentDataById.set(row.id, row);
    }
  }

  private serializeCurrentData(
    record: storage.SaveOptions,
    after: sync_rules.ToastableSqliteRow
  ): { after: sync_rules.ToastableSqliteRow; data: Buffer; truncated: boolean } {
    try {
      const serialized = storage.serializeBson(after);
      if (serialized.byteLength > MAX_ROW_SIZE) {
        throw new Error(`Row too large: ${serialized.byteLength}`);
      }
      return { after, data: serialized, truncated: false };
    } catch (e) {
      const error = e instanceof Error ? e : new Error(String(e));
      this.options.logger.warn(
        `Data too big on ${record.sourceTable.qualifiedName}.${record.after?.id}: ${error.message}`
      );

      // Keep the current_data row present, but drop field values. This mirrors
      // the Postgres storage behavior for oversized BSON payloads and allows
      // future TOAST-style updates to be marked unavailable instead of crashing
      // the replication batch.
      const emptyValues = Object.fromEntries(Object.keys(after).map((key) => [key, undefined]));
      return { after: emptyValues, data: storage.serializeBson(emptyValues), truncated: true };
    }
  }

  private upsertCurrentData(options: {
    tableId: string;
    sourceKey: storage.ReplicaId;
    buckets: CurrentBucket[];
    lookups: Buffer[];
    data: Buffer;
    pendingDelete: bigint | null;
  }): void {
    const serializedSourceKey = storage.serializeReplicaId(options.sourceKey);
    const id = currentDataId(this.options.replicationStreamId, options.tableId, serializedSourceKey);
    const payload = {
      id,
      groupId: this.options.replicationStreamId,
      sourceTable: options.tableId,
      sourceKey: serializedSourceKey,
      buckets: options.buckets,
      lookups: options.lookups.map((lookup) => lookup.toString('hex')),
      data: options.data,
      pendingDelete: options.pendingDelete
    };
    this.options.tx
      .insert(this.options.dialect.tables.currentData)
      .values(payload)
      .onConflictDoUpdate({
        target: this.options.dialect.tables.currentData.id,
        set: payload
      })
      .run();
    this.currentDataById.set(id, payload);
  }

  private deleteCurrentData(tableId: string, sourceKey: storage.ReplicaId, nextOpId: bigint): bigint {
    const serializedSourceKey = storage.serializeReplicaId(sourceKey);
    const id = currentDataId(this.options.replicationStreamId, tableId, serializedSourceKey);
    const payload = {
      id,
      groupId: this.options.replicationStreamId,
      sourceTable: tableId,
      sourceKey: serializedSourceKey,
      buckets: [],
      lookups: [],
      data: storage.serializeBson({}),
      pendingDelete: nextOpId
    };
    this.options.tx
      .insert(this.options.dialect.tables.currentData)
      .values(payload)
      .onConflictDoUpdate({ target: this.options.dialect.tables.currentData.id, set: payload })
      .run();
    this.currentDataById.set(id, payload);
    return nextOpId + 1n;
  }
}

export function currentBuckets(row: CurrentDataRow | null): CurrentBucket[] {
  return Array.isArray(row?.buckets) ? (row.buckets as CurrentBucket[]) : [];
}

export function currentLookups(row: CurrentDataRow | null): Buffer[] {
  return Array.isArray(row?.lookups) ? (row.lookups as string[]).map((lookup) => Buffer.from(lookup, 'hex')) : [];
}

function currentDataId(groupId: number, sourceTable: string, sourceKey: Buffer): string {
  return `${groupId}:${sourceTable}:${sourceKey.toString('hex')}`;
}

function currentBucketKey(bucket: CurrentBucket | sync_rules.EvaluatedRow): string {
  return `${bucket.bucket}/${bucket.table}/${bucket.id}`;
}

function hasUsableObjectId(row: sync_rules.EvaluatedRow): boolean {
  return row.id !== '' || row.data.id != null;
}

function replicaIdToSubkey(tableId: storage.SourceTableId, id: storage.ReplicaId): string {
  if (storage.isUUID(id)) {
    return `${tableId}/${id.toHexString()}`;
  }
  return uuid.v5(storage.serializeBson({ table: tableId, id }), utils.ID_NAMESPACE);
}
