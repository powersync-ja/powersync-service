import { Logger } from '@powersync/lib-services-framework';
import { InternalOpId, storage, utils } from '@powersync/service-core';
import { JSONBig } from '@powersync/service-jsonbig';
import {
  BucketDataSource,
  BucketDefinitionId,
  EvaluatedParameters,
  EvaluatedRow,
  ParameterIndexLookupCreator,
  SqliteJsonValue
} from '@powersync/service-sync-rules';
import * as bson from 'bson';
import { MongoIdSequence } from '../MongoIdSequence.js';
import { ReplicationBatchLimits } from '../ReplicationBatchLimits.js';
import { SourceRecordBucketState, SourceRecordLookupState } from './SourceRecordStore.js';

/**
 * An evaluated row with its data serialized, ready to be persisted.
 *
 * Rows evaluated in JavaScript are serialized with {@link serializeEvaluatedRow}, the Rust evaluator emits serialized
 * rows directly.
 */
export interface SerializedEvaluatedRow {
  source: BucketDataSource;
  bucket: string;
  table: string;
  id: string;
  /**
   * The JSON-serialized row data.
   */
  data: string;
  /**
   * The checksum of the row, see `utils.hashData`.
   */
  checksum: number;
}

export function serializeEvaluatedRow(row: EvaluatedRow): SerializedEvaluatedRow {
  const data = JSONBig.stringify(row.data);
  return {
    source: row.source,
    bucket: row.bucket,
    table: row.table,
    id: row.id,
    data,
    checksum: utils.hashData(row.table, row.id, data)
  };
}

/**
 * A parameter lookup output with the lookup already serialized in the v3 storage format (`serializeParameterLookup`).
 *
 * This is emitted by the Rust evaluator, and only supported by v3 storage.
 */
export class SerializedParameterLookup {
  constructor(
    readonly source: ParameterIndexLookupCreator,
    readonly lookup: bson.Binary,
    readonly bucketParameters: Record<string, SqliteJsonValue>[]
  ) {}
}

export type ParameterLookupOutput = EvaluatedParameters | SerializedParameterLookup;

export function parameterLookupSource(output: ParameterLookupOutput): ParameterIndexLookupCreator {
  return output instanceof SerializedParameterLookup ? output.source : output.lookup.source;
}

export interface SaveBucketDataOptions {
  op_seq: MongoIdSequence;
  sourceKey: storage.ReplicaId;
  table: storage.SourceTable;
  evaluated: SerializedEvaluatedRow[];
  before_buckets: SourceRecordBucketState[];
}

export interface SaveParameterDataOptions {
  op_seq: MongoIdSequence;
  sourceKey: storage.ReplicaId;
  sourceTable: storage.SourceTable;
  evaluated: ParameterLookupOutput[];
  existing_lookups: SourceRecordLookupState[];
}

export interface UpsertCurrentDataOptions {
  sourceTableId: bson.ObjectId;
  replicaId: storage.ReplicaId;
  data: bson.Binary | null;
  buckets: SourceRecordBucketState[];
  lookups: SourceRecordLookupState[];
}

export interface PersistedBatchOptions {
  batchLimits?: ReplicationBatchLimits;
  logger?: Logger;
}

/** Operations used by the shared writer; each storage version owns its lifecycle. */
export interface PersistedBatch {
  readonly currentSize: number;
  saveBucketData(options: SaveBucketDataOptions): void;
  saveParameterData(options: SaveParameterDataOptions): void;
  hardDeleteCurrentData(sourceTableId: bson.ObjectId, replicaId: storage.ReplicaId): void;
  softDeleteCurrentData(
    sourceTableId: bson.ObjectId,
    replicaId: storage.ReplicaId,
    checkpointGreaterThan: bigint
  ): void;
  upsertCurrentData(values: UpsertCurrentDataOptions): void;
  shouldFlushTransaction(): boolean;
}

export interface BucketStateUpdate {
  definitionId: BucketDefinitionId | null;
  bucket: string;
  lastOp: InternalOpId;
  incrementCount: number;
  incrementBytes: number;
  incrementChunks: number;
}
