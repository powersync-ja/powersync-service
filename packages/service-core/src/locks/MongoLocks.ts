import * as bson from 'bson';
import * as mongo from 'mongodb';
import { LockActiveError, LockManager } from './LockManager.js';

/**
 * Lock Document Schema
 */
export type Lock = {
  name: string;
  active_lock?: {
    lock_id: bson.ObjectId;
    ts: Date;
  };
};

export type Collection = mongo.Collection<Lock>;

export type AcquireLockParams = {
  /**
   * Name of the process/user trying to acquire the lock.
   */
  name: string;
  /**
   * The TTL of the lock (ms). Default: 60000 ms (1 min)
   */
  timeout?: number;
};

const DEFAULT_LOCK_TIMEOUT = 60 * 1000; // 1 minute

const acquireLock = async (collection: Collection, params: AcquireLockParams) => {
  const now = new Date();
  const lock_timeout = params.timeout ?? DEFAULT_LOCK_TIMEOUT;
  const lock_id = new bson.ObjectId();

  await collection.updateOne(
    {
      name: params.name
    },
    {
      $setOnInsert: {
        name: params.name
      }
    },
    {
      upsert: true
    }
  );

  const expired_ts = now.getTime() - lock_timeout;

  const res = await collection.updateOne(
    {
      $and: [
        { name: params.name },
        {
          $or: [{ active_lock: { $exists: false } }, { 'active_lock.ts': { $lte: new Date(expired_ts) } }]
        }
      ]
    },
    {
      $set: {
        active_lock: {
          lock_id: lock_id,
          ts: now
        }
      }
    }
  );

  if (res.modifiedCount === 0) {
    return null;
  }

  return lock_id.toString();
};

const refreshLock = async (collection: Collection, lock_id: string) => {
  const lockId = new bson.ObjectId(lock_id);
  const res = await collection.updateOne(
    {
      'active_lock.lock_id': lockId
    },
    {
      $set: {
        'active_lock.ts': new Date()
      }
    }
  );

  if (res.modifiedCount === 0) {
    throw new Error('Lock not found, could not refresh');
  }
};

export const releaseLock = async (collection: Collection, lock_id: string) => {
  const lockId = new bson.ObjectId(lock_id);
  const res = await collection.updateOne(
    {
      'active_lock.lock_id': lockId
    },
    {
      $unset: {
        active_lock: true
      }
    }
  );

  if (res.modifiedCount === 0) {
    throw new Error('Lock not found, could not release');
  }
};

export type CreateLockManagerParams = {
  /**
   * Name of the process/user trying to acquire the lock.
   */
  name: string;
  /**
   * The TTL for each lock (ms). Default: 60000 ms (1 min)
   */
  timeout?: number;
};

export const createMongoLockManager = (collection: Collection, params: CreateLockManagerParams): LockManager => {
  return {
    acquire: () => acquireLock(collection, params),
    refresh: (lock_id: string) => refreshLock(collection, lock_id),
    release: (lock_id: string) => releaseLock(collection, lock_id),

    lock: async (handler) => {
      const lock_id = await acquireLock(collection, params);
      if (!lock_id) {
        throw new LockActiveError();
      }

      try {
        await handler(() => refreshLock(collection, lock_id));
      } finally {
        await releaseLock(collection, lock_id);
      }
    }
  };
};
