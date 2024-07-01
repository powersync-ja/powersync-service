import * as bson from 'bson';

export class LockActiveError extends Error {
  constructor() {
    super('Lock is already active');
    this.name = this.constructor.name;
  }
}

export type LockManager = {
  acquire: () => Promise<bson.ObjectId | null>;
  refresh: (lock_id: bson.ObjectId) => Promise<void>;
  release: (lock_id: bson.ObjectId) => Promise<void>;

  lock: (handler: (refresh: () => Promise<void>) => Promise<void>) => Promise<void>;
};
