import { ToastableSqliteRow } from '@powersync/service-sync-rules';

export enum SyncRuleState {
  /**
   * New replication stream - needs to be processed (initial replication).
   *
   * While multiple replication streams _can_ be in PROCESSING,
   * it's generally pointless, so we only keep one in that state.
   */
  PROCESSING = 'PROCESSING',

  /**
   * Intial processing is done, and can be used for sync.
   *
   * Only one replication stream should be in ACTIVE or ERRORED state.
   */
  ACTIVE = 'ACTIVE',
  /**
   * This state is used when the replication stream has been replaced,
   * and replication is or should be stopped.
   */
  STOP = 'STOP',
  /**
   * After replication stream has been stopped, the data needs to be
   * deleted. Once deleted, the state is TERMINATED.
   */
  TERMINATED = 'TERMINATED',

  /**
   * Replication stream has run into a permanent replication error. It
   * is still the "active" replication stram for syncing to users,
   * but should not replicate anymore.
   *
   * It will transition to STOP when a new replication stream is activated.
   */
  ERRORED = 'ERRORED'
}

export const DEFAULT_DOCUMENT_BATCH_LIMIT = 1000;
export const DEFAULT_DOCUMENT_CHUNK_LIMIT_BYTES = 1 * 1024 * 1024;

export function mergeToast<V>(record: ToastableSqliteRow<V>, persisted: ToastableSqliteRow<V>): ToastableSqliteRow<V> {
  const newRecord: ToastableSqliteRow<V> = {};
  for (let key in record) {
    if (typeof record[key] == 'undefined') {
      newRecord[key] = persisted[key];
    } else {
      newRecord[key] = record[key];
    }
  }
  return newRecord;
}
