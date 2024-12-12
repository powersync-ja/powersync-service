export class LockActiveError extends Error {
  constructor() {
    super('Lock is already active');
    this.name = this.constructor.name;
  }
}

export type LockAcquireOptions = {
  max_wait_ms?: number;
};

export type LockManager = {
  init?: () => Promise<void>;
  acquire: (options?: LockAcquireOptions) => Promise<string | null>;
  refresh: (lock_id: string) => Promise<void>;
  release: (lock_id: string) => Promise<void>;

  lock: (handler: (refresh: () => Promise<void>) => Promise<void>) => Promise<void>;
};
