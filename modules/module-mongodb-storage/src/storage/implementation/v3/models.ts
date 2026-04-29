import { InternalOpId, SerializedSyncPlan, SyncRuleState } from '@powersync/service-core';
import * as bson from 'bson';
import { BucketDefinitionId, ParameterIndexId } from '../BucketDefinitionMapping.js';
import { BucketDataDoc, BucketKey } from '../common/BucketDataDoc.js';
import {
  BucketDataDocumentBase,
  BucketDataKey,
  BucketParameterDocumentBase,
  BucketStateDocumentBase,
  CurrentBucket,
  ReplicaId,
  SourceTableDocument,
  SourceTableKey,
  SyncRuleDocument,
  TaggedBucketParameterDocument
} from '../models.js';

export interface SyncRuleDocumentV3 extends SyncRuleDocument {
  storage_version: number;

  /**
   * These contain the _state_ for each sync config.
   *
   * In common cases, we'd have either only one "active" sync config, or one active + one processing.
   *
   * But we don't restrict ourselves to that - we support any number of sync configs in any state here.
   *
   * In general, we we attempt to keep the small but regularly updated state here, while keeping the more static configuration
   * in a separate sync_config collection.
   *
   * Any checkpoint updates all sync configs at the same time, but the effect on each checkpoint may be different. We consider
   * snapshot state / no_checkpoint_before separately for each.
   *
   * A replication stream can only have a single resume point, so we keep that outside of this sync_configs array, still
   * in the top_level snapshot_lsn field.
   */
  sync_configs: {
    _id: bson.ObjectId;
    /**
     * If false, we cannot create any checkpoints.
     */
    snapshot_done: boolean;

    /**
     * Permutations we want to support eventually:
     *
     * { state: ACTIVE, sync_configs: [{ state: ACTIVE }]}
     * { state: ACTIVE, sync_configs: [{ state: ACTIVE }, { state: PROCESSING }]}
     * { state: ACTIVE, sync_configs: [{ state: ERRORED }, { state: PROCESSING }]}
     * { state: ACTIVE, sync_configs: [{ state: ACTIVE }, { state: STOP }]}
     * { state: ACTIVE, sync_configs: [{ state: ACTIVE }, { state: PROCESSING }, { state: STOP }]}
     * { state: PROCESSING, sync_configs: [{ state: PROCESSING }]}
     * { state: PROCESSING, sync_configs: [{ state: STOP }]}
     * { state: PROCESSING, sync_configs: [{ state: PROCESSING }, { state: STOP }]}
     * { state: TERMINATED, sync_configs: []}
     *
     * These are also supported, but we may replace the outer state with simpler ACTIVE/PROCESSING states:
     * { state: STOP, sync_configs: [{ state: STOP }]}
     * { state: STOP, sync_configs: [{ state: STOP }, { state: STOP }]}
     * { state: ERRORED, sync_configs: [{ state: ERRORED }]}
     *
     * In general:
     * 1. The outer state does not indicate much anymore - the inner state is the important one.
     * 2. We can only have one ACTIVE inner state globally.
     * 3. Inner STOP state indicates sync configs for which we have data, that needs to be cleaned up.
     * 4. Inner ERROR state indicates sync config we still use for syncing, but not for replication.
     * 5. In theory, when sync rules are updated while we are busy with PROCESSING, we can re-use some of the partially replicated data.
     * 6. In theory we could have multiple inner and outer states in PROCESSING. In practice, we transition them to stop as soon as we put a new one in PROCESSING.
     * 7. We don't keep inner TERMINATED state around. At the point the inner sync config transitions to a TERMINATED state, there is nothing tying it to the replication stream anymore, and we just remove that reference completely.
     */
    state: SyncRuleState;

    /**
     * The last consistent checkpoint.
     *
     * There may be higher OpIds used in the database if we're in the middle of replicating a large transaction.
     */
    last_checkpoint: bigint | null;
    /**
     * The LSN associated with the last consistent checkpoint.
     *
     * This is specifically used to correlate with write checkpoints, and not for resuming replication.
     */
    last_checkpoint_lsn: string | null;

    /**
     * If set, no new checkpoints may be created < this value.
     */
    no_checkpoint_before: string | null;

    /**
     * Goes together with no_checkpoint_before.
     *
     * If a keepalive is triggered that creates the checkpoint > no_checkpoint_before,
     * then the checkpoint must be equal to this keepalive_op.
     *
     * TODO: Re-check this - the description above is confusing.
     */
    keepalive_op: bigint | null;

    // TODO: check whether these fields should be top-level or per-sync-config
    // last_checkpoint_ts: Date | null;
    // last_keepalive_ts: Date | null;
    // last_fatal_error: string | null;
    // last_fatal_error_ts: Date | null;
  }[];
}

/**
 * Static sync config definition.
 *
 * This should be treated as immutable - we don't update this after initial creation.
 */
export interface SyncConfigDefinition {
  _id: bson.ObjectId;
  created_at: Date;
  storage_version: number;
  /**
   * The related SyncRuleDocumentV3.
   *
   * Note that a specific sync config definition never moves between replication streams. Instead, we can create a new copy for the new replication stream.
   *
   * When terminating a specific sync config definition, we remove the reference from replication stream -> sync config, but this reference here remains as a historical record.
   */
  replication_stream_id: number;

  content: string;
  serialized_plan?: SerializedSyncPlan | null;

  rule_mapping: {
    /**
     * Map of uniqueName -> id, unique per replication stream.
     */
    definitions: Record<string, string>;
    /**
     * Map of (lookupName, queryId) -> id, unique per replication stream.
     */
    parameter_indexes: Record<string, string>;
  };
}

export interface CurrentBucketV3 extends CurrentBucket {
  def: BucketDefinitionId;
}

export interface RecordedLookupV3 {
  i: ParameterIndexId;
  l: bson.Binary;
}

export interface CurrentDataDocumentV3 {
  _id: ReplicaId;
  data: bson.Binary | null;
  buckets: CurrentBucketV3[];
  lookups: RecordedLookupV3[];
  /**
   * If set, this can be deleted, once there is a consistent checkpoint >= pending_delete.
   *
   * This must only be set if buckets = [], lookups = [].
   */
  pending_delete?: bigint;
}

export interface BucketParameterDocumentV3 extends BucketParameterDocumentBase<SourceTableKey> {}

export type BucketDataKeyV3 = BucketDataKey;

export interface BucketDataDocumentV3 extends BucketDataDocumentBase {
  _id: BucketDataKeyV3;
}

export function serializeBucketDataV3(document: BucketDataDoc): BucketDataDocumentV3 {
  const { bucketKey, o } = document;
  return {
    _id: {
      b: bucketKey.bucket,
      o: o
    },
    // List fields directly, so that we don't accidentally persist any unknown fields
    op: document.op,
    source_table: document.source_table,
    source_key: document.source_key,
    table: document.table,
    row_id: document.row_id,
    checksum: document.checksum,
    data: document.data,
    target_op: document.target_op
  };
}

export function loadBucketDataDocumentV3(
  context: Pick<BucketKey, 'replicationStreamId' | 'definitionId'>,
  doc: BucketDataDocumentV3
): BucketDataDoc {
  const { _id, ...rest } = doc;
  return {
    bucketKey: {
      ...context,
      bucket: _id.b
    },
    o: _id.o,
    ...rest
  };
}

export function taggedBucketParameterDocumentToV3(document: TaggedBucketParameterDocument): BucketParameterDocumentV3 {
  const { index: _index, ...rest } = document;
  return rest as BucketParameterDocumentV3;
}

export interface SourceTableDocumentV3 extends SourceTableDocument {
  bucket_data_source_ids: BucketDefinitionId[];
  parameter_lookup_source_ids: ParameterIndexId[];
  latest_pending_delete?: InternalOpId | undefined;
}

export interface BucketStateDocumentV3 extends BucketStateDocumentBase {
  _id: BucketStateDocumentBase['_id'] & {
    d: BucketDefinitionId;
  };
}
