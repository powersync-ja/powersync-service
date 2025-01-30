import * as t from 'ts-codec';
import { BucketDescription, BucketPriority, SqliteJsonValue } from '@powersync/service-sync-rules';

export const BucketRequest = t.object({
  name: t.string,

  /**
   * Base-10 number. Sync all data from this bucket with op_id > after.
   */
  after: t.string
});

export type BucketRequest = t.Decoded<typeof BucketRequest>;

export const StreamingSyncRequest = t.object({
  /**
   * Existing bucket states.
   */
  buckets: t.array(BucketRequest).optional(),

  /**
   * If specified, limit the response to only include these buckets.
   */
  only: t.array(t.string).optional(),

  /**
   * Whether or not to compute a checksum for each checkpoint
   */
  include_checksum: t.boolean.optional(),

  /**
   * True to keep `data` as a string, instead of nested JSON.
   */
  raw_data: t.boolean.optional(),

  /**
   * Data is received in a serialized BSON Buffer
   */
  binary_data: t.boolean.optional(),

  /**
   * Client parameters to be passed to the sync rules.
   */
  parameters: t.record(t.any).optional(),

  /**
   * Unique client id.
   */
  client_id: t.string.optional()
});

export type StreamingSyncRequest = t.Decoded<typeof StreamingSyncRequest>;

export interface StreamingSyncCheckpoint {
  checkpoint: Checkpoint;
}

export interface StreamingSyncCheckpointDiff {
  checkpoint_diff: {
    last_op_id: OpId;
    write_checkpoint?: OpId;
    updated_buckets: BucketChecksumWithDescription[];
    removed_buckets: string[];
  };
}

export interface StreamingSyncData {
  data: SyncBucketData;
}

export interface StreamingSyncCheckpointComplete {
  checkpoint_complete: {
    last_op_id: OpId;
  };
}

export interface StreamingSyncCheckpointPartiallyComplete {
  partial_checkpoint_complete: {
    last_op_id: OpId;
    priority: BucketPriority;
  }
}

export interface StreamingSyncKeepalive {
  token_expires_in: number;
}

export type StreamingSyncLine =
  | StreamingSyncData
  | StreamingSyncCheckpoint
  | StreamingSyncCheckpointDiff
  | StreamingSyncCheckpointComplete
  | StreamingSyncCheckpointPartiallyComplete
  | StreamingSyncKeepalive;

/**
 * 64-bit unsigned number, as a base-10 string.
 */
export type OpId = string;

export interface Checkpoint {
  last_op_id: OpId;
  write_checkpoint?: OpId;
  buckets: BucketChecksumWithDescription[];
}

export interface BucketState {
  bucket: string;
  op_id: string;
}

export interface SyncDataBatch {
  buckets: SyncBucketData[];
}

export interface SyncBucketData {
  bucket: string;
  data: OplogEntry[];
  /**
   * True if the response does not contain all the data for this bucket, and another request must be made.
   */
  has_more: boolean;
  /**
   * The `after` specified in the request.
   */
  after: OpId;
  /**
   * Use this for the next request.
   */
  next_after: OpId;
}

export interface OplogEntry {
  op_id: OpId;
  op: 'PUT' | 'REMOVE' | 'MOVE' | 'CLEAR';
  object_type?: string;
  object_id?: string;
  data?: Record<string, SqliteJsonValue> | string | null;
  checksum: number | bigint;
  subkey?: string;
}

export interface BucketChecksum {
  bucket: string;
  /**
   * 32-bit unsigned hash.
   */
  checksum: number;

  /**
   * Count of operations - informational only.
   */
  count: number;
}

export interface BucketChecksumWithDescription extends BucketChecksum, BucketDescription {
}
