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
 * A sync steam that a client has expressed interest in by explicitly opening it on the client side.
 */
export const RequestedStreamSubscription = t.object({
  /**
   * The defined name of the stream as it appears in sync stream definitions.
   */
  stream: t.string,
  /**
   * An optional dictionary of parameters to pass to this specific stream.
   */
  parameters: t.record(t.any).optional(),
  /**
   * Set when the client wishes to re-assign a different priority to this stream.
   *
   * Streams and sync rules can also assign a default priority, but clients are allowed to override those. This can be
   * useful when the priority for partial syncs depends on e.g. the current page opened in a client.
   */
  override_priority: t.number.optional()
});

export type RequestedStreamSubscription = t.Decoded<typeof RequestedStreamSubscription>;

/**
 * An overview of all subscribed streams as part of a streaming sync request.
 */
export const StreamSubscriptionRequest = t.object({
  /**
   * Whether to sync default streams.
   *
   * When disabled, only explicitly-opened subscriptions are included.
   */
  include_defaults: t.boolean.optional(),

  /**
   * An array of sync streams the client has opened explicitly.
   */
  subscriptions: t.array(RequestedStreamSubscription)
});

export type StreamSubscriptionRequest = t.Decoded<typeof StreamSubscriptionRequest>;

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
   * If the client is aware of streams, an array of streams the client has opened.
   */
  streams: StreamSubscriptionRequest.optional()
});

export type StreamingSyncRequest = t.Decoded<typeof StreamingSyncRequest>;

export interface StreamingSyncCheckpoint {
  checkpoint: Checkpoint;
}

export interface StreamingSyncCheckpointDiff {
  checkpoint_diff: {
    last_op_id: ProtocolOpId;
    write_checkpoint?: ProtocolOpId;
    updated_buckets: CheckpointBucket[];
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

export interface StreamDescription {
  /**
   * The name of the stream as it appears in the sync configuration.
   */
  name: string;

  /**
   * Whether this stream is subscribed to by default.
   *
   * For default streams, this field is still `true` if clients have an explicit subscription to the stream.
   */
  is_default: boolean;

  /**
   * If some subscriptions on this stream could not be resolved, e.g. due to an error, tis
   */
  errors: StreamSubscriptionError[];
}

export interface StreamSubscriptionError {
  /**
   * The subscription that errored - either the default subscription or some of the explicit subscriptions.
   */
  subscription: 'default' | number;
  /**
   * A message describing the error on the subscription.
   */
  message: string;
}

export interface Checkpoint {
  last_op_id: ProtocolOpId;
  write_checkpoint?: ProtocolOpId;
  buckets: CheckpointBucket[];

  /**
   * All streams that the client is subscribed to.
   *
   * This field has two purposes:
   *
   *  1. It allows clients to determine which of their subscriptions actually works. E.g. if a user does
   *     `db.syncStream('non_existent_stream').subscribe()`, clients don't immediately know that the stream doesn't
   *     exist. Only after the next `checkpoint` line can they query this field and mark unresolved subscriptions.
   *. 2. It allows clients to learn which default streams they have been subscribed to. This is relevant for APIs
   *     listing all streams on the client-side.
   */
  streams: StreamDescription[];
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

/**
 * The reason a particular bucket is included in a checkpoint.
 *
 * This information allows clients to associate individual buckets with sync streams they're subscribed to. Having that
 * association is useful because it enables clients to track progress for individual sync streams.
 */
export type BucketSubscriptionReason = BucketDerivedFromDefaultStream | BucketDerivedFromExplicitSubscription;

/**
 * A bucket has been included in a checkpoint because it's part of a default stream.
 */
export type BucketDerivedFromDefaultStream = {
  /**
   * The index (into {@link Checkpoint.streams}) of the stream defining the bucket.
   */
  default: number;
};

/**
 * The bucket has been included in a checkpoint because it's part of a stream that a client has explicitly subscribed
 * to.
 */
export type BucketDerivedFromExplicitSubscription = {
  /**
   * The index (into {@link StreamSubscriptionRequest.subscriptions}) of the subscription yielding this bucket.
   */
  sub: number;
};

export interface ClientBucketDescription {
  /**
   * An opaque id of the bucket.
   */
  bucket: string;
  /**
   * The priority used to synchronize this bucket, derived from its definition and an optional priority override from
   * the stream subscription.
   */
  priority: BucketPriority;
  subscriptions: BucketSubscriptionReason[];
}

export interface CheckpointBucket extends BucketChecksum, ClientBucketDescription {}
