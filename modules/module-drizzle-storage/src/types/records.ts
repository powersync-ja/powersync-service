import type { storage } from '@powersync/service-core';
import type {
  BucketDataRow,
  BucketParametersRow,
  CurrentDataRow,
  SourceTableRow,
  SyncRulesRow,
  WriteCheckpointRow
} from '../drivers/sqlite/schema.js';

export interface StorageBucketDataRecord {
  id: string;
  groupId: number;
  bucketName: string;
  opId: bigint;
  op: string;
  sourceTable: string | null;
  sourceKey: Buffer | null;
  tableName: string | null;
  rowId: string | null;
  checksum: bigint;
  data: string | null;
  targetOp: bigint | null;
}

export interface StorageBucketParametersRecord {
  id: bigint;
  groupId: number;
  sourceTable: string;
  sourceKey: Buffer;
  lookup: Buffer;
  bucketParameters: unknown;
}

export interface StorageCurrentDataRecord {
  id: string;
  groupId: number;
  sourceTable: string;
  sourceKey: Buffer;
  buckets: { bucket: string; table: string; id: string }[];
  lookups: string[];
  data: Buffer;
  pendingDelete: bigint | null;
}

export interface StorageSourceTableRecord {
  id: string;
  groupId: number;
  connectionId: number;
  relationId: unknown;
  schemaName: string;
  tableName: string;
  replicaIdColumns: unknown;
  snapshotDone: boolean;
  snapshotTotalEstimatedCount: bigint | null;
  snapshotReplicatedCount: bigint | null;
  snapshotLastKey: Buffer | null;
}

export interface StorageSyncRulesRecord {
  id: number;
  state: storage.SyncRuleState;
  snapshotDone: boolean;
  snapshotLsn: string | null;
  lastCheckpoint: bigint | null;
  lastCheckpointLsn: string | null;
  noCheckpointBefore: string | null;
  slotName: string;
  lastCheckpointTs: Date | null;
  lastKeepaliveTs: Date | null;
  lastFatalError: string | null;
  lastFatalErrorTs: Date | null;
  keepaliveOp: bigint | null;
  storageVersion: number | null;
  content: string;
  syncPlan: storage.SerializedSyncPlan | null;
}

export interface StorageWriteCheckpointRecord {
  id: string;
  syncRulesId: number | null;
  userId: string;
  checkpoint: bigint;
  heads: Record<string, string> | null;
  createdAt: Date;
}

type Equal<Left, Right> = (<T>() => T extends Left ? 1 : 2) extends <T>() => T extends Right ? 1 : 2 ? true : false;
type Assert<T extends true> = T;

// A future driver gets the same assertions against its own inferred table
// models. This keeps query results identical without sharing table builders.
type _BucketDataMapping = Assert<Equal<BucketDataRow, StorageBucketDataRecord>>;
type _BucketParametersMapping = Assert<Equal<BucketParametersRow, StorageBucketParametersRecord>>;
type _CurrentDataMapping = Assert<Equal<CurrentDataRow, StorageCurrentDataRecord>>;
type _SourceTableMapping = Assert<Equal<SourceTableRow, StorageSourceTableRecord>>;
type _SyncRulesMapping = Assert<Equal<SyncRulesRow, StorageSyncRulesRecord>>;
type _WriteCheckpointMapping = Assert<Equal<WriteCheckpointRow, StorageWriteCheckpointRecord>>;
