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

export interface WriteCheckpointFilters {
  /**
   * Replication HEAD(s) at the creation of the checkpoint.
   */
  heads?: Record<string, string>;

  /**
   * Sync rules which were active when this checkpoint was created.
   */
  sync_rules_id?: number;

  /**
   * Identifier for User's account.
   */
  user_id: string;
}

export interface WriteCheckpointOptions extends WriteCheckpointFilters {
  /**
   * Strictly incrementing write checkpoint number.
   * Defaults to an automatically incrementing operation.
   */
  checkpoint?: bigint;
}

export interface WriteCheckpointAPI {
  batchCreateWriteCheckpoints(checkpoints: WriteCheckpointOptions[]): Promise<void>;

  createWriteCheckpoint(checkpoint: WriteCheckpointOptions): Promise<bigint>;

  lastWriteCheckpoint(filters: WriteCheckpointFilters): Promise<bigint | null>;
}

export const DEFAULT_WRITE_CHECKPOINT_MODE = WriteCheckpointMode.MANAGED;
