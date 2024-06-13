import * as t from 'ts-codec';
import { SqliteJsonValue } from '@powersync/service-sync-rules';

/**
 * For sync2.json
 */
export interface ContinueCheckpointRequest {
  /**
   * Existing bucket states. Only these buckets are synchronized.
   */
  buckets: BucketRequest[];

  checkpoint_token: string;

  limit?: number;
}

export interface SyncNewCheckpointRequest {
  /**
   * Existing bucket states. Used if include_data is specified.
   */
  buckets?: BucketRequest[];

  request_checkpoint: {
    /**
     * Whether or not to include an initial data request.
     */
    include_data: boolean;

    /**
     * Whether or not to compute a checksum.
     */
    include_checksum: boolean;
  };

  limit?: number;
}

export type SyncRequest = ContinueCheckpointRequest | SyncNewCheckpointRequest;

export interface SyncResponse {
  /**
   * Data for the buckets returned. May not have an an entry for each bucket in the request.
   */
  data?: SyncBucketData[];

  /**
   * True if the response limit has been reached, and another request must be made.
   */
  has_more: boolean;

  checkpoint_token?: string;

  checkpoint?: Checkpoint;
}

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
  binary_data: t.boolean.optional()
});

export type StreamingSyncRequest = t.Decoded<typeof StreamingSyncRequest>;

export interface StreamingSyncCheckpoint {
  checkpoint: Checkpoint;
}

export interface StreamingSyncCheckpointDiff {
  checkpoint_diff: {
    last_op_id: OpId;
    write_checkpoint?: OpId;
    updated_buckets: BucketChecksum[];
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

export interface StreamingSyncKeepalive {}

export type StreamingSyncLine =
  | StreamingSyncData
  | StreamingSyncCheckpoint
  | StreamingSyncCheckpointDiff
  | StreamingSyncCheckpointComplete
  | StreamingSyncKeepalive;

/**
 * 64-bit unsigned number, as a base-10 string.
 */
export type OpId = string;

export interface Checkpoint {
  last_op_id: OpId;
  write_checkpoint?: OpId;
  buckets: BucketChecksum[];
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

export type ChecksumMap = Map<string, BucketChecksum>;

export function isContinueCheckpointRequest(request: SyncRequest): request is ContinueCheckpointRequest {
  return (
    Array.isArray((request as ContinueCheckpointRequest).buckets) &&
    typeof (request as ContinueCheckpointRequest).checkpoint_token == 'string'
  );
}

export function isSyncNewCheckpointRequest(request: SyncRequest): request is SyncNewCheckpointRequest {
  return typeof (request as SyncNewCheckpointRequest).request_checkpoint == 'object';
}

/**
 * For crud.json
 */
export interface CrudRequest {
  data: CrudEntry[];
}

export interface CrudEntry {
  op: 'PUT' | 'PATCH' | 'DELETE';
  type: string;
  id: string;
  data: string;
}

export interface CrudResponse {
  /**
   * A sync response with a checkpoint >= this checkpoint would contain all the changes in this request.
   *
   * Any earlier checkpoint may or may not contain these changes.
   *
   * May be empty when the request contains no ops.
   */
  checkpoint?: OpId;
}
