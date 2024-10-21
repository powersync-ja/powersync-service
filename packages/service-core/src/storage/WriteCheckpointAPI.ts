export enum WriteCheckpointMode {
  /**
   * Raw mappings of `user_id` to `write_checkpoint`s should
   * be supplied for each set of sync rules.
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
   * Sync rules which were active when this checkpoint was created.
   */
  sync_rules_id: number;
}

export interface BatchedCustomWriteCheckpointOptions extends BaseWriteCheckpointIdentifier {
  /**
   * A supplied incrementing Write Checkpoint number
   */
  checkpoint: bigint;
}

export interface CustomWriteCheckpointOptions extends BatchedCustomWriteCheckpointOptions {
  /**
   * Sync rules which were active when this checkpoint was created.
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

export type ManagedWriteCheckpointOptions = ManagedWriteCheckpointFilters;

export type SyncStorageLastWriteCheckpointFilters = BaseWriteCheckpointIdentifier | ManagedWriteCheckpointFilters;
export type LastWriteCheckpointFilters = CustomWriteCheckpointFilters | ManagedWriteCheckpointFilters;

export interface BaseWriteCheckpointAPI {
  readonly writeCheckpointMode: WriteCheckpointMode;
  setWriteCheckpointMode(mode: WriteCheckpointMode): void;
  createManagedWriteCheckpoint(checkpoint: ManagedWriteCheckpointOptions): Promise<bigint>;
}

/**
 * Write Checkpoint API to be used in conjunction with a {@link SyncRulesBucketStorage}.
 * This storage corresponds with a set of sync rules. These APIs don't require specifying a
 * sync rules id.
 */
export interface SyncStorageWriteCheckpointAPI extends BaseWriteCheckpointAPI {
  batchCreateCustomWriteCheckpoints(checkpoints: BatchedCustomWriteCheckpointOptions[]): Promise<void>;
  createCustomWriteCheckpoint(checkpoint: BatchedCustomWriteCheckpointOptions): Promise<bigint>;
  lastWriteCheckpoint(filters: SyncStorageLastWriteCheckpointFilters): Promise<bigint | null>;
}

/**
 * Write Checkpoint API which is interfaced directly with the storage layer. This requires
 * sync rules identifiers for custom write checkpoints.
 */
export interface WriteCheckpointAPI extends BaseWriteCheckpointAPI {
  batchCreateCustomWriteCheckpoints(checkpoints: CustomWriteCheckpointOptions[]): Promise<void>;
  createCustomWriteCheckpoint(checkpoint: CustomWriteCheckpointOptions): Promise<bigint>;
  lastWriteCheckpoint(filters: LastWriteCheckpointFilters): Promise<bigint | null>;
}

export const DEFAULT_WRITE_CHECKPOINT_MODE = WriteCheckpointMode.MANAGED;
