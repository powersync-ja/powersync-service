import { AbstractMikroOrmMigrationLockManager } from '../../migrations/AbstractMikroOrmMigrationLockManager.js';

/**
 * SQLite implementation of the migration lock.
 *
 * This intentionally uses raw SQLite statements instead of MikroORM entities. Migration locking has a classic
 * chicken-and-egg problem: the service must take a DB-backed lock before it asks MikroORM to run the migrations
 * that normally create and evolve application tables. The lock table therefore has to bootstrap itself with
 * `CREATE TABLE IF NOT EXISTS`, outside the generated MikroORM migration model.
 */
export class SqliteMigrationLockManager extends AbstractMikroOrmMigrationLockManager {
  protected async initLockStore(): Promise<void> {
    await this.execute(
      `
      CREATE TABLE IF NOT EXISTS powersync_migration_locks (
        name TEXT PRIMARY KEY,
        lock_id TEXT,
        expires_at INTEGER NOT NULL
      )
      `,
      []
    );

    await this.execute(
      `
      INSERT OR IGNORE INTO powersync_migration_locks (name, lock_id, expires_at)
      VALUES (?, NULL, 0)
      `,
      [this.name]
    );
  }

  protected async tryAcquireLock(options: {
    name: string;
    lockId: string;
    now: Date;
    expiresAt: Date;
  }): Promise<boolean> {
    const result = await this.execute(
      `
      UPDATE powersync_migration_locks
      SET lock_id = ?, expires_at = ?
      WHERE name = ?
        AND (lock_id IS NULL OR expires_at <= ?)
      `,
      [options.lockId, options.expiresAt.getTime(), options.name, options.now.getTime()]
    );

    return result.affectedRows == 1;
  }

  protected async refreshLock(lockId: string): Promise<void> {
    const result = await this.execute(
      `
      UPDATE powersync_migration_locks
      SET expires_at = ?
      WHERE name = ? AND lock_id = ?
      `,
      [new Date(Date.now() + this.timeout).getTime(), this.name, lockId]
    );

    if (result.affectedRows != 1) {
      throw new Error('Lock not found, could not refresh');
    }
  }

  protected async releaseLock(lockId: string): Promise<void> {
    const result = await this.execute(
      `
      UPDATE powersync_migration_locks
      SET lock_id = NULL, expires_at = 0
      WHERE name = ? AND lock_id = ?
      `,
      [this.name, lockId]
    );

    if (result.affectedRows != 1) {
      throw new Error('Lock not found, could not release');
    }
  }

  /**
   * Execute raw SQLite against MikroORM's connection so the lock can exist before entity-backed schema exists.
   */
  private async execute(sql: string, params: unknown[]): Promise<{ affectedRows?: number }> {
    const orm = await this.getOrm();
    return orm.em.getConnection().execute(sql, params, 'run');
  }
}
