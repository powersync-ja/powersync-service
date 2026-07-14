import type { storage } from '@powersync/service-core';
import { blob, index, integer, sqliteTable, text } from 'drizzle-orm/sqlite-core';
import { sqliteBigInt, sqliteBoolean, sqliteInteger, sqliteTimestampMs } from './column-types.js';

export type CurrentBucket = {
  bucket: string;
  table: string;
  id: string;
};

export const bucketData = sqliteTable(
  'bucket_data',
  {
    id: text('id').primaryKey(),
    groupId: sqliteInteger('group_id').notNull(),
    bucketName: text('bucket_name').notNull(),
    opId: sqliteBigInt('op_id').notNull(),
    op: text('op').notNull(),
    sourceTable: text('source_table'),
    sourceKey: blob('source_key', { mode: 'buffer' }),
    tableName: text('table_name'),
    rowId: text('row_id'),
    checksum: sqliteBigInt('checksum').notNull(),
    data: text('data'),
    targetOp: sqliteBigInt('target_op')
  },
  (table) => [
    index('bucket_data_bucket_op_index').on(table.groupId, table.bucketName, table.opId),
    index('bucket_data_source_index').on(table.groupId, table.sourceTable, table.sourceKey)
  ]
);

export const bucketParameters = sqliteTable(
  'bucket_parameters',
  {
    id: sqliteBigInt('id').primaryKey(),
    groupId: sqliteInteger('group_id').notNull(),
    sourceTable: text('source_table').notNull(),
    sourceKey: blob('source_key', { mode: 'buffer' }).notNull(),
    lookup: blob('lookup', { mode: 'buffer' }).notNull(),
    bucketParameters: text('bucket_parameters', { mode: 'json' }).$type<unknown>().notNull()
  },
  (table) => [
    index('bucket_parameters_lookup_index').on(table.groupId, table.lookup, table.id),
    index('bucket_parameters_source_index').on(table.groupId, table.sourceTable, table.sourceKey)
  ]
);

export const currentData = sqliteTable(
  'current_data',
  {
    id: text('id').primaryKey(),
    groupId: sqliteInteger('group_id').notNull(),
    sourceTable: text('source_table').notNull(),
    sourceKey: blob('source_key', { mode: 'buffer' }).notNull(),
    buckets: text('buckets', { mode: 'json' }).$type<CurrentBucket[]>().notNull(),
    lookups: text('lookups', { mode: 'json' }).$type<string[]>().notNull(),
    data: blob('data', { mode: 'buffer' }).notNull(),
    pendingDelete: sqliteBigInt('pending_delete')
  },
  (table) => [
    index('current_data_source_index').on(table.groupId, table.sourceTable, table.sourceKey),
    index('current_data_pending_delete_index').on(table.groupId, table.pendingDelete)
  ]
);

export const instance = sqliteTable('instance', {
  id: text('id').primaryKey()
});

export const sourceTables = sqliteTable(
  'source_tables',
  {
    id: text('id').primaryKey(),
    groupId: sqliteInteger('group_id').notNull(),
    connectionId: sqliteInteger('connection_id').notNull(),
    relationId: text('relation_id', { mode: 'json' }).$type<unknown>(),
    schemaName: text('schema_name').notNull(),
    tableName: text('table_name').notNull(),
    replicaIdColumns: text('replica_id_columns', { mode: 'json' }).$type<unknown>(),
    snapshotDone: sqliteBoolean('snapshot_done').notNull().default(true),
    snapshotTotalEstimatedCount: sqliteBigInt('snapshot_total_estimated_count'),
    snapshotReplicatedCount: sqliteBigInt('snapshot_replicated_count'),
    snapshotLastKey: blob('snapshot_last_key', { mode: 'buffer' })
  },
  (table) => [index('source_table_lookup').on(table.groupId, table.tableName)]
);

export const syncRules = sqliteTable('sync_rules', {
  id: integer('id').primaryKey({ autoIncrement: true }),
  state: text('state').$type<storage.SyncRuleState>().notNull(),
  snapshotDone: sqliteBoolean('snapshot_done').notNull().default(false),
  snapshotLsn: text('snapshot_lsn'),
  lastCheckpoint: sqliteBigInt('last_checkpoint'),
  lastCheckpointLsn: text('last_checkpoint_lsn'),
  noCheckpointBefore: text('no_checkpoint_before'),
  slotName: text('slot_name').notNull(),
  lastCheckpointTs: sqliteTimestampMs('last_checkpoint_ts'),
  lastKeepaliveTs: sqliteTimestampMs('last_keepalive_ts'),
  lastFatalError: text('last_fatal_error'),
  lastFatalErrorTs: sqliteTimestampMs('last_fatal_error_ts'),
  keepaliveOp: sqliteBigInt('keepalive_op'),
  storageVersion: sqliteInteger('storage_version'),
  content: text('content').notNull(),
  syncPlan: text('sync_plan', { mode: 'json' }).$type<storage.SerializedSyncPlan>()
});

export const writeCheckpoints = sqliteTable(
  'write_checkpoints',
  {
    id: text('id').primaryKey(),
    syncRulesId: sqliteInteger('sync_rules_id'),
    userId: text('user_id').notNull(),
    checkpoint: sqliteBigInt('checkpoint').notNull(),
    heads: text('heads', { mode: 'json' }).$type<Record<string, string>>(),
    createdAt: sqliteTimestampMs('created_at').notNull()
  },
  (table) => [index('write_checkpoints_user_checkpoint_index').on(table.userId, table.syncRulesId, table.checkpoint)]
);

export const sqliteSchema = {
  bucketData,
  bucketParameters,
  currentData,
  instance,
  sourceTables,
  syncRules,
  writeCheckpoints
};

export type BucketDataRow = typeof bucketData.$inferSelect;
export type BucketParametersRow = typeof bucketParameters.$inferSelect;
export type CurrentDataRow = typeof currentData.$inferSelect;
export type InstanceRow = typeof instance.$inferSelect;
export type SourceTableRow = typeof sourceTables.$inferSelect;
export type SyncRulesRow = typeof syncRules.$inferSelect;
export type WriteCheckpointRow = typeof writeCheckpoints.$inferSelect;
