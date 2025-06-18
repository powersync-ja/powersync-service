import * as t from 'ts-codec';
import { BucketDescription, BucketPriority, SqliteJsonRow } from '@powersync/service-sync-rules';
import { JsonContainer } from '@powersync/service-jsonbig';

export const BucketRequest = t.object({
  name: t.string,

  /**
   * Base-10 number. Sync all data from this bucket with op_id > after.
   */
  after: t.string
});

export type BucketRequest = t.Decoded<typeof BucketRequest>;

/**
 * An explicit subscription to a defined sync stream made by the client.
 */
export const StreamSubscription = t.object({
  /**
   * The defined name of the stream as it appears in sync rules.
   */
  stream: t.string,
  /**
   * An optional dictionary of parameters to pass to this specific stream.
   */
  parameters: t.record(t.any).optional(),
  /**
   * Set when the client wishes to re-assign a different priority to this subscription.
   *
   * Streams and sync rules can also assign a default priority, but clients are allowed to override those. This can be
   * useful when the priority for partial syncs depends on e.g. the current page opened in a client.
   */
  override_priority: t.number.optional()
});

export type StreamSubscription = t.Decoded<typeof StreamSubscription>;

/**
 * An overview of all subscriptions as part of a streaming sync request.
 */
export const StreamSubscriptions = t.object({
  /**
   * Whether to sync default streams.
   *
   * When disabled,only
   */
  include_defaults: t.boolean.optional(),

  /**
   * An array of streams the client has subscribed to.
   */
  subscriptions: t.array(StreamSubscription)
});

export type StreamSubscriptions = t.Decoded<typeof StreamSubscriptions>;

export const StreamingSyncRequest = t.object({
  /**
   * Existing client-side bucket states.
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
  client_id: t.string.optional(),

  /**
   * If the client is aware of stream subscriptions, an array of streams the client is subscribing to.
   */
  subscriptions: StreamSubscriptions.optional()
});

export type StreamingSyncRequest = t.Decoded<typeof StreamingSyncRequest>;

export interface StreamingSyncCheckpoint {
  checkpoint: Checkpoint;
}

export interface StreamingSyncCheckpointDiff {
  checkpoint_diff: {
    last_op_id: ProtocolOpId;
    write_checkpoint?: ProtocolOpId;
    updated_buckets: BucketChecksumWithDescription[];
    removed_buckets: string[];
  };
}

export interface StreamingSyncData {
  data: SyncBucketData<ProtocolOplogData>;
}

export interface StreamingSyncCheckpointComplete {
  checkpoint_complete: {
    last_op_id: ProtocolOpId;
  };
}

export interface StreamingSyncCheckpointPartiallyComplete {
  partial_checkpoint_complete: {
    last_op_id: ProtocolOpId;
    priority: BucketPriority;
  };
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
export type ProtocolOpId = string;

export interface Checkpoint {
  last_op_id: ProtocolOpId;
  write_checkpoint?: ProtocolOpId;
  buckets: BucketChecksumWithDescription[];
}

export interface BucketState {
  bucket: string;
  op_id: string;
}

export interface SyncBucketData<Data extends ProtocolOplogData = StoredOplogData> {
  bucket: string;
  data: OplogEntry<Data>[];
  /**
   * True if there _could_ be more data for this bucket, and another request must be made.
   */
  has_more: boolean;
  /**
   * The `after` specified in the request, or the next_after from the previous batch returned.
   */
  after: ProtocolOpId;
  /**
   * Use this for the next request.
   */
  next_after: ProtocolOpId;
}

export type StoredOplogData = string | null;

// Note: When clients have both raw_data and binary_data disabled (this only affects legacy
// clients), data is actually a `Record<string, SqliteJsonValue>`. Oplog entries are always
// stored as a serialized (JSON) string so that they don't have to be parsed in the sync service,
// this representation only exists on the way out for legacy clients.
export type ProtocolOplogData = SqliteJsonRow | JsonContainer | StoredOplogData;

export interface OplogEntry<Data extends ProtocolOplogData = StoredOplogData> {
  op_id: ProtocolOpId;
  op: 'PUT' | 'REMOVE' | 'MOVE' | 'CLEAR';
  object_type?: string;
  object_id?: string;
  data?: Data;
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

export interface BucketChecksumWithDescription extends BucketChecksum, BucketDescription {}
