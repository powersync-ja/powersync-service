export interface ReplicationLock {
  sync_rules_id: number;

  release(): Promise<void>;
}
