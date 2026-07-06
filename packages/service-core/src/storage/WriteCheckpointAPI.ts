export enum WriteCheckpointMode {
  /**
   * Raw mappings of `user_id` to `write_checkpoint`s should
   * be supplied for each replication stream.
   */
  CUSTOM = 'custom',
  /**
   * Write checkpoints are stored as a mapping of `user_id` plus
   * replication HEAD (lsn in Postgres) to an automatically generated
   * incrementing `write_checkpoint` (stored as`client_id`).
   */
  MANAGED = 'managed'
}

export interface BaseWriteCheckpointIdentifier {
  /**
   * Identifier for User's account.
   */
  user_id: string;
}

export interface ClientRequestedCheckpointOptions {
  /**
   * Supplied for client-generated checkpoint requests.
   *
   * If omitted, storage creates or stores a regular write checkpoint.
   * If supplied for managed write checkpoints, storage only uses it when it is
   * greater than the stored id for the user_id. Same or lower supplied ids are
   * no-ops, and storage returns the current stored id.
   */
  checkpoint_request_id?: bigint;
}

export interface CustomWriteCheckpointFilters extends BaseWriteCheckpointIdentifier {
  /**
   * Replication stream which was active when this checkpoint was created.
   */
  sync_rules_id: number;
}

export interface BatchedCustomWriteCheckpointOptions extends BaseWriteCheckpointIdentifier {
  /**
   * A supplied incrementing Write Checkpoint number
   */
  checkpoint: bigint;
  /**
   * Set when this custom write checkpoint was created from a client checkpoint
   * request. Regular custom write checkpoints leave this unset so retention
   * cleanup does not delete them.
   */
  checkpoint_requested_at?: Date | null;
}

export interface CustomWriteCheckpointOptions extends BatchedCustomWriteCheckpointOptions {
  /**
   * Replication stream which was active when this checkpoint was created.
   */
  sync_rules_id: number;
}

/**
 * Managed Write Checkpoints are a mapping of User ID to replication HEAD
 */
export interface ManagedWriteCheckpointFilters extends BaseWriteCheckpointIdentifier {
  /**
   * Replication HEAD(s) at the creation of the checkpoint.
   */
  heads: Record<string, string>;
}

export type ManagedWriteCheckpointOptions = ManagedWriteCheckpointFilters & ClientRequestedCheckpointOptions;

export interface CreateManagedWriteCheckpointsResult {
  /**
   * Current managed checkpoint id for each full user_id after applying the batch.
   */
  writeCheckpoints: Map<string, bigint>;
  /**
   * True when the source marker must be forced so replication observes the
   * stored head. Callers use this to advance the source marker once for the
   * whole batch.
   *
   * This must be true whenever a checkpoint in the batch has not yet been
   * processed by replication - both freshly created/advanced checkpoints and
   * existing checkpoints matched by a stale or duplicate request that are still
   * pending. Already-processed checkpoints do not need a new marker.
   *
   * The case this guards against is a lost source marker on a stale retry. The
   * source marker is forced after the checkpoint is persisted, so the two are
   * not atomic (storage and source are usually different databases). Without
   * this flag, the following sequence strands the checkpoint:
   *
   *   1. A client-supplied checkpoint id is persisted (write checkpoint stored,
   *      processed_at_lsn null).
   *   2. Forcing the source marker fails, so the request errors and the client
   *      retries with the same id.
   *   3. The retry is a no-op for the stored id (monotonic, not greater), so if
   *      we only advanced on "a row changed" we would skip the marker.
   *   4. On an idle source nothing else moves replication past the stored head,
   *      so the checkpoint never gets acknowledged and the client waits forever.
   *
   * Reporting true while the checkpoint is still pending makes the retry re-force
   * the marker, which resolves the wait.
   *
   * Backends that do not track a per-row processed indicator may conservatively
   * report true whenever any checkpoint was matched.
   */
  shouldAdvance: boolean;
}

export function uniqueManagedWriteCheckpoints(
  checkpoints: ManagedWriteCheckpointOptions[]
): ManagedWriteCheckpointOptions[] {
  const byUser = new Map<string, ManagedWriteCheckpointOptions>();

  for (const checkpoint of checkpoints) {
    const existing = byUser.get(checkpoint.user_id);
    // A batch can contain entries for many users, and different users may use
    // different request types (generated vs client-supplied). For a single full
    // user_id, though, all entries should be the same type - a batch can still
    // hold multiple entries for that user (e.g. coalesced requests), so we
    // collapse them to one. For client-supplied ids we keep the greatest
    // requested value, so lower stale ids cannot hide the request that should
    // advance storage. For generated ids the entries are equivalent, so any one
    // is kept.
    //
    // Mixing generated and supplied entries for the same user_id is not expected;
    // shouldReplaceManagedWriteCheckpoint still resolves it deterministically
    // (supplied wins) if that invariant is ever violated.
    if (existing == null || shouldReplaceManagedWriteCheckpoint(existing, checkpoint)) {
      byUser.set(checkpoint.user_id, checkpoint);
    }
  }

  return [...byUser.values()];
}

function shouldReplaceManagedWriteCheckpoint(
  existing: ManagedWriteCheckpointOptions,
  candidate: ManagedWriteCheckpointOptions
) {
  const existingRequestId = existing.checkpoint_request_id;
  const candidateRequestId = candidate.checkpoint_request_id;

  if (candidateRequestId == null) {
    return existingRequestId == null;
  }

  if (existingRequestId == null) {
    return true;
  }

  return candidateRequestId > existingRequestId;
}

export type SyncStorageLastWriteCheckpointFilters = BaseWriteCheckpointIdentifier | ManagedWriteCheckpointFilters;
export type LastWriteCheckpointFilters = CustomWriteCheckpointFilters | ManagedWriteCheckpointFilters;

export interface BaseWriteCheckpointAPI {
  readonly writeCheckpointMode: WriteCheckpointMode;
  setWriteCheckpointMode(mode: WriteCheckpointMode): void;
  createManagedWriteCheckpoints(
    checkpoints: ManagedWriteCheckpointOptions[]
  ): Promise<CreateManagedWriteCheckpointsResult>;
}

/**
 * Write Checkpoint API to be used in conjunction with a {@link SyncRulesBucketStorage}.
 * This storage corresponds with a replication stream. These APIs don't require specifying a
 * replication stream id.
 */
export interface SyncStorageWriteCheckpointAPI extends BaseWriteCheckpointAPI {
  lastWriteCheckpoint(filters: SyncStorageLastWriteCheckpointFilters): Promise<bigint | null>;
}

/**
 * Write Checkpoint API which is interfaced directly with the storage layer. This requires
 * replication stream identifiers for custom write checkpoints.
 */
export interface WriteCheckpointAPI extends BaseWriteCheckpointAPI {
  lastWriteCheckpoint(filters: LastWriteCheckpointFilters): Promise<bigint | null>;
}

export const DEFAULT_WRITE_CHECKPOINT_MODE = WriteCheckpointMode.MANAGED;
