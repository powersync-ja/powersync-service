import { mongo } from '@powersync/lib-service-mongodb';
import { SerializedSyncPlan, storage } from '@powersync/service-core';
import { MongoSyncRulesLock } from './MongoSyncRulesLock.js';
import { PowerSyncMongo } from './db.js';
import { SyncRuleDocument } from './models.js';

export class MongoPersistedSyncRulesContent extends storage.PersistedSyncRulesContent {
  public current_lock: MongoSyncRulesLock | null = null;

  constructor(
    private db: PowerSyncMongo,
    doc: mongo.WithId<SyncRuleDocument>
  ) {
    super({
      id: doc._id,
      sync_rules_content: doc.content,
      sync_plan: doc.plan,
      last_checkpoint_lsn: doc.last_checkpoint_lsn,
      // Handle legacy values
      slot_name: doc.slot_name ?? `powersync_${doc._id}`,
      last_fatal_error: doc.last_fatal_error,
      last_fatal_error_ts: doc.last_fatal_error_ts,
      last_checkpoint_ts: doc.last_checkpoint_ts,
      last_keepalive_ts: doc.last_keepalive_ts,
      active: doc.state == 'ACTIVE'
    });
  }

  async lock() {
    const lock = await MongoSyncRulesLock.createLock(this.db, this);
    this.current_lock = lock;
    return lock;
  }
}
