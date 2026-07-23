import { AbstractMikroOrmMigrationLockManager } from '../../migrations/AbstractMikroOrmMigrationLockManager.js';

/**
 * MySQL-backed migration lock manager.
 *
 * The lock table is created with raw SQL because migration locking has to work before MikroORM-created storage tables
 * exist. This avoids the classic chicken-and-egg problem where migrations need a lock, but the lock would otherwise
 * need a migration-created table.
 */
export class MySqlMigrationLockManager extends AbstractMikroOrmMigrationLockManager {
  protected async initLockStore(): Promise<void> {
    await this.execute(
      `
      CREATE TABLE IF NOT EXISTS powersync_mikroorm_migration_locks (
        name VARCHAR(191) PRIMARY KEY,
        lock_id VARCHAR(36) NOT NULL,
        expires_at DATETIME(3) NOT NULL,
        updated_at DATETIME(3) NOT NULL
      )
      `,
      []
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
      INSERT INTO powersync_mikroorm_migration_locks (name, lock_id, expires_at, updated_at)
      VALUES (?, ?, ?, ?)
      ON DUPLICATE KEY UPDATE
        lock_id = IF(expires_at <= ?, VALUES(lock_id), lock_id),
        expires_at = IF(expires_at <= ?, VALUES(expires_at), expires_at),
        updated_at = IF(expires_at <= ?, VALUES(updated_at), updated_at)
      `,
      [
        options.name,
        options.lockId,
        options.expiresAt,
        options.now,
        options.now,
        options.now,
        options.now
      ]
    );

    return (result.affectedRows ?? 0) > 0;
  }

  protected async refreshLock(lockId: string): Promise<void> {
    await this.execute(
      `
      UPDATE powersync_mikroorm_migration_locks
      SET expires_at = ?, updated_at = ?
      WHERE name = ? AND lock_id = ?
      `,
      [new Date(Date.now() + this.timeout), new Date(), this.name, lockId]
    );
  }

  protected async releaseLock(lockId: string): Promise<void> {
    await this.execute(
      `
      DELETE FROM powersync_mikroorm_migration_locks
      WHERE name = ? AND lock_id = ?
      `,
      [this.name, lockId]
    );
  }

  private async execute(sql: string, params: unknown[]): Promise<{ affectedRows?: number }> {
    const orm = await this.getOrm();
    return orm.em.getConnection().execute(sql, params, 'run');
  }
}
