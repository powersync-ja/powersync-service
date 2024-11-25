export class LockActiveError extends Error {
  constructor() {
    super('Lock is already active');
    this.name = this.constructor.name;
  }
}

export type LockManager = {
  acquire: () => Promise<string | null>;
  refresh: (lock_id: string) => Promise<void>;
  release: (lock_id: string) => Promise<void>;

  lock: (handler: (refresh: () => Promise<void>) => Promise<void>) => Promise<void>;
};
