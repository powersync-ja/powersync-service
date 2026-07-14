import { locks } from '@powersync/lib-services-framework';
import * as uuid from 'uuid';
import type { SqliteDrizzleRuntime } from './sqlite-config.js';

const DEFAULT_LOCK_TIMEOUT = 60_000;

export class SqliteMigrationLockManager extends locks.AbstractLockManager {
  constructor(private readonly options: locks.LockManagerParams & { runtime: SqliteDrizzleRuntime }) {
    super(options);
  }

  private get timeout(): number {
    return this.options.timeout ?? DEFAULT_LOCK_TIMEOUT;
  }

  async init(): Promise<void> {
    this.options.runtime.client.exec(`
      CREATE TABLE IF NOT EXISTS powersync_migration_locks (
        name TEXT PRIMARY KEY,
        lock_id TEXT,
        expires_at INTEGER NOT NULL
      )
    `);
    this.options.runtime.client
      .prepare(
        `
      INSERT OR IGNORE INTO powersync_migration_locks (name, lock_id, expires_at)
      VALUES (?, NULL, 0)
    `
      )
      .run(this.options.name);
  }

  protected async acquireHandle(): Promise<locks.LockHandle | null> {
    const lockId = uuid.v4();
    const now = Date.now();
    const result = this.options.runtime.client
      .prepare(
        `
      UPDATE powersync_migration_locks
      SET lock_id = ?, expires_at = ?
      WHERE name = ? AND (lock_id IS NULL OR expires_at <= ?)
    `
      )
      .run(lockId, now + this.timeout, this.options.name, now);
    if (result.changes != 1) {
      return null;
    }
    return {
      refresh: async () => {
        const refreshed = this.options.runtime.client
          .prepare(
            `
          UPDATE powersync_migration_locks SET expires_at = ? WHERE name = ? AND lock_id = ?
        `
          )
          .run(Date.now() + this.timeout, this.options.name, lockId);
        if (refreshed.changes != 1) {
          throw new Error('Lock not found, could not refresh');
        }
      },
      release: async () => {
        const released = this.options.runtime.client
          .prepare(
            `
          UPDATE powersync_migration_locks SET lock_id = NULL, expires_at = 0 WHERE name = ? AND lock_id = ?
        `
          )
          .run(this.options.name, lockId);
        if (released.changes != 1) {
          throw new Error('Lock not found, could not release');
        }
      }
    };
  }
}
