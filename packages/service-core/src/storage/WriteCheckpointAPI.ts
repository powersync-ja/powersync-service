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
}

export interface ClientRequestedCheckpointOptions {
  /**
   * Supplied for client-generated checkpoint requests.
   *
   * If omitted, storage generates the next managed write checkpoint id.
   * If supplied, storage only uses it when it is greater than the stored id for
   * the user_id. Same or lower supplied ids are no-ops, and storage returns the
   * current stored id.
   */
  checkpoint_request_id?: bigint;
}

export interface CustomWriteCheckpointOptions
  extends BatchedCustomWriteCheckpointOptions,
    ClientRequestedCheckpointOptions {
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
   * True when at least one row was created or advanced. Callers use this to
   * advance the source marker once for the whole batch.
   */
  updated: boolean;
}

export function uniqueManagedWriteCheckpoints(
  checkpoints: ManagedWriteCheckpointOptions[]
): ManagedWriteCheckpointOptions[] {
  const byUser = new Map<string, ManagedWriteCheckpointOptions>();

  for (const checkpoint of checkpoints) {
    const existing = byUser.get(checkpoint.user_id);
    // A single HTTP batch can contain multiple requests for the same user.
    // If any request supplies a checkpoint id, process only the greatest one so
    // lower stale ids cannot hide the request that should advance storage.
    // In normal routing we should never receive both generated and supplied
    // requests for the same full user_id; this keeps the storage boundary
    // deterministic if that invariant is violated.
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
