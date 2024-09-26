export enum WriteCheckpointMode {
  /**
   * Raw mappings of `user_id` to `write_checkpoint`s should
   * be supplied for each set of sync rules.
   */
  CUSTOM = 'manual',
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

export type CustomWriteCheckpointOptions = BatchedCustomWriteCheckpointOptions & CustomWriteCheckpointFilters;

/**
 * Managed Write Checkpoints are a mapping of User ID to replication HEAD
 */
export interface ManagedWriteCheckpointFilters {
  /**
   * Replication HEAD(s) at the creation of the checkpoint.
   */
  heads: Record<string, string>;

  /**
   * Identifier for User's account.
   */
  user_id: string;
}

export type ManagedWriteCheckpointOptions = ManagedWriteCheckpointFilters;

export type LastWriteCheckpointFilters = CustomWriteCheckpointFilters | ManagedWriteCheckpointFilters;

export interface WriteCheckpointAPI {
  batchCreateCustomWriteCheckpoints(checkpoints: CustomWriteCheckpointOptions[]): Promise<void>;

  createCustomWriteCheckpoint(checkpoint: CustomWriteCheckpointOptions): Promise<bigint>;

  createManagedWriteCheckpoint(checkpoint: ManagedWriteCheckpointOptions): Promise<bigint>;

  lastWriteCheckpoint(filters: LastWriteCheckpointFilters): Promise<bigint | null>;
}

export const DEFAULT_WRITE_CHECKPOINT_MODE = WriteCheckpointMode.MANAGED;
