export interface WriteCheckpointFilters {
  /**
   * LSN(s) at the creation of the checkpoint.
   */
  lsns?: Record<string, string>;

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
   * Strictly incrementing write checkpoint number
   */
  checkpoint: bigint;
}
