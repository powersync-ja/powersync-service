import { MikroORM } from '@mikro-orm/core';
import { locks } from '@powersync/lib-services-framework';
import * as uuid from 'uuid';

const DEFAULT_LOCK_TIMEOUT = 60_000;

/**
 * Construction parameters for a database-backed MikroORM migration lock manager.
 *
 * The ORM instance is supplied so dialect implementations can reuse MikroORM's configured connection while still
 * issuing the small amount of raw SQL required to bootstrap the lock table before migrations have run.
 */
export interface MikroOrmMigrationLockManagerParams extends locks.LockManagerParams {
  orm: MikroORM | Promise<MikroORM>;
}

/**
 * Base lock manager for MikroORM-backed storage migrations.
 *
 * The common layer owns lock lifecycle behavior and expiry handling. Dialects provide the actual persistence
 * operations because lock bootstrap is database-specific and must work before the normal MikroORM migration schema
 * exists.
 */
export abstract class AbstractMikroOrmMigrationLockManager extends locks.AbstractLockManager {
  private ormInstance: MikroORM | undefined;

  constructor(protected params: MikroOrmMigrationLockManagerParams) {
    super(params);
  }

  protected get timeout() {
    return this.params.timeout ?? DEFAULT_LOCK_TIMEOUT;
  }

  protected get name() {
    return this.params.name;
  }

  protected async getOrm(): Promise<MikroORM> {
    this.ormInstance ??= await this.params.orm;
    return this.ormInstance;
  }

  async init(): Promise<void> {
    await this.initLockStore();
  }

  protected async acquireHandle(): Promise<locks.LockHandle | null> {
    const lockId = await this.acquireLockId();
    if (lockId == null) {
      return null;
    }

    return {
      refresh: () => this.refreshLock(lockId),
      release: () => this.releaseLock(lockId)
    };
  }

  protected async acquireLockId(): Promise<string | null> {
    const lockId = uuid.v4();
    const now = new Date();
    const expiresAt = new Date(now.getTime() + this.timeout);

    return (await this.tryAcquireLock({
      name: this.name,
      lockId,
      now,
      expiresAt
    }))
      ? lockId
      : null;
  }

  /**
   * Create or prepare the dialect-specific lock storage.
   *
   * Implementations must be safe to call before any generated MikroORM migration has run.
   */
  protected abstract initLockStore(): Promise<void>;

  /**
   * Try to acquire the named lock if it is free or expired.
   */
  protected abstract tryAcquireLock(options: {
    name: string;
    lockId: string;
    now: Date;
    expiresAt: Date;
  }): Promise<boolean>;

  /**
   * Extend ownership of the currently-held lock.
   */
  protected abstract refreshLock(lockId: string): Promise<void>;

  /**
   * Release ownership of the currently-held lock.
   */
  protected abstract releaseLock(lockId: string): Promise<void>;
}
