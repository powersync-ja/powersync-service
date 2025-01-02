export class LockActiveError extends Error {
  constructor() {
    super('Lock is already active');
    this.name = this.constructor.name;
  }
}

export type LockAcquireOptions = {
  /**
   * Optionally retry and wait for the lock to be acquired
   */
  max_wait_ms?: number;
};

export type LockHandle = {
  refresh(): Promise<void>;
  release(): Promise<void>;
};

export type LockCallback = (refresh: () => Promise<void>) => Promise<void>;

export type LockManager = {
  init?(): Promise<void>;
  /**
   * Attempts to acquire a lock handle.
   * @returns null if the lock is in use and either no `max_wait_ms` was provided or the timeout has elapsed.
   */
  acquire(options?: LockAcquireOptions): Promise<LockHandle | null>;
  /**
   * Acquires a lock, executes the given handler callback then automatically releases the lock.
   */
  lock(handler: LockCallback, options?: LockAcquireOptions): Promise<void>;
};
