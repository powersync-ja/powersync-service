import { LockAcquireOptions, LockActiveError, LockCallback, LockHandle, LockManager } from './LockManager.js';

export type LockManagerParams = {
  /**
   * Name of the process/user trying to acquire the lock.
   */
  name: string;
  /**
   * The TTL of the lock (ms). Default: 60000 ms (1 min)
   */
  timeout?: number;
};

export abstract class AbstractLockManager implements LockManager {
  constructor(protected params: LockManagerParams) {}

  /**
   * Implementation specific method for acquiring a lock handle.
   */
  protected abstract acquireHandle(): Promise<LockHandle | null>;

  async acquire(options?: LockAcquireOptions): Promise<LockHandle | null> {
    const { max_wait_ms = 0 } = options ?? {};
    let handle: LockHandle | null = null;
    const start = new Date();
    do {
      handle = await this.acquireHandle();
      if (handle) {
        return handle;
      } else if (max_wait_ms) {
        await new Promise((r) => setTimeout(r, max_wait_ms / 10));
      }
    } while (new Date().getTime() - start.getTime() < max_wait_ms);

    return handle;
  }

  async lock(handler: LockCallback, options?: LockAcquireOptions): Promise<void> {
    const handle = await this.acquire(options);
    if (!handle) {
      throw new LockActiveError();
    }
    try {
      await handler(() => handle.refresh());
    } finally {
      await handle.release();
    }
  }
}
