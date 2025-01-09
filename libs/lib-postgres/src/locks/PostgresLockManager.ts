import * as framework from '@powersync/lib-services-framework';
import { v4 as uuidv4 } from 'uuid';
import { DatabaseClient, sql } from '../db/db-index.js';

const DEFAULT_LOCK_TIMEOUT = 60_000; // 1 minute

export interface PostgresLockManagerParams extends framework.locks.LockManagerParams {
  db: DatabaseClient;
}

export class PostgresLockManager extends framework.locks.AbstractLockManager {
  constructor(protected params: PostgresLockManagerParams) {
    super(params);
  }

  protected get db() {
    return this.params.db;
  }

  get timeout() {
    return this.params.timeout ?? DEFAULT_LOCK_TIMEOUT;
  }

  get name() {
    return this.params.name;
  }

  async init() {
    /**
     * Locks are required for migrations, which means this table can't be
     * created inside a migration. This ensures the locks table is present.
     */
    await this.db.query(sql`
      CREATE TABLE IF NOT EXISTS locks (
        name TEXT PRIMARY KEY,
        lock_id UUID NOT NULL,
        ts TIMESTAMPTZ NOT NULL
      );
    `);
  }

  protected async acquireHandle(): Promise<framework.LockHandle | null> {
    const id = await this._acquireId();
    if (!id) {
      return null;
    }
    return {
      refresh: () => this.refreshHandle(id),
      release: () => this.releaseHandle(id)
    };
  }

  protected async _acquireId(): Promise<string | null> {
    const now = new Date();
    const expiredTs = new Date(now.getTime() - this.timeout);
    const lockId = uuidv4();

    try {
      // Attempt to acquire or refresh the lock
      const res = await this.db.query(sql`
        INSERT INTO
          locks (name, lock_id, ts)
        VALUES
          (
            ${{ type: 'varchar', value: this.name }},
            ${{ type: 'uuid', value: lockId }},
            ${{ type: 1184, value: now.toISOString() }}
          )
        ON CONFLICT (name) DO UPDATE
        SET
          lock_id = CASE
            WHEN locks.ts <= $4 THEN $2
            ELSE locks.lock_id
          END,
          ts = CASE
            WHEN locks.ts <= $4 THEN $3
            ELSE locks.ts
          END
        WHERE
          locks.ts <= ${{ type: 1184, value: expiredTs.toISOString() }}
        RETURNING
          lock_id;
      `);

      if (res.rows.length === 0) {
        // Lock is active and could not be acquired
        return null;
      }

      return lockId;
    } catch (err) {
      console.error('Error acquiring lock:', err);
      throw err;
    }
  }

  protected async refreshHandle(lockId: string) {
    const res = await this.db.query(sql`
      UPDATE locks
      SET
        ts = ${{ type: 1184, value: new Date().toISOString() }}
      WHERE
        lock_id = ${{ type: 'uuid', value: lockId }}
      RETURNING
        lock_id;
    `);

    if (res.rows.length === 0) {
      throw new Error('Lock not found, could not refresh');
    }
  }

  protected async releaseHandle(lockId: string) {
    const res = await this.db.query(sql`
      DELETE FROM locks
      WHERE
        lock_id = ${{ type: 'uuid', value: lockId }}
      RETURNING
        lock_id;
    `);

    if (res.rows.length == 0) {
      throw new Error('Lock not found, could not release');
    }
  }
}
