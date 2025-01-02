import * as framework from '@powersync/lib-services-framework';
import * as bson from 'bson';
import * as mongo from 'mongodb';

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

export type MongoLockManagerParams = framework.locks.LockManagerParams & {
  collection: Collection;
};

const DEFAULT_LOCK_TIMEOUT = 60 * 1000; // 1 minute

export class MongoLockManager extends framework.locks.AbstractLockManager {
  collection: Collection;
  constructor(params: MongoLockManagerParams) {
    super(params);
    this.collection = params.collection;
  }

  protected async acquireHandle(options?: framework.LockAcquireOptions): Promise<framework.LockHandle | null> {
    const lock_id = await this.getHandle();
    if (!lock_id) {
      return null;
    }
    return {
      refresh: () => this.refreshHandle(lock_id),
      release: () => this.releaseHandle(lock_id)
    };
  }

  protected async refreshHandle(lock_id: bson.ObjectId) {
    const res = await this.collection.updateOne(
      {
        'active_lock.lock_id': lock_id
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
  }

  protected async getHandle() {
    const now = new Date();
    const lock_timeout = this.params.timeout ?? DEFAULT_LOCK_TIMEOUT;
    const lock_id = new bson.ObjectId();

    const { name } = this.params;
    await this.collection.updateOne(
      {
        name
      },
      {
        $setOnInsert: {
          name
        }
      },
      {
        upsert: true
      }
    );

    const expired_ts = now.getTime() - lock_timeout;

    const res = await this.collection.updateOne(
      {
        $and: [
          { name: name },
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

    return lock_id;
  }

  protected async releaseHandle(lock_id: bson.ObjectId) {
    const res = await this.collection.updateOne(
      {
        'active_lock.lock_id': lock_id
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
  }
}
