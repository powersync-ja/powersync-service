import { SqlSyncRules } from '@powersync/service-sync-rules';
import * as mongo from 'mongodb';

import { ParseSyncRulesOptions, PersistedSyncRulesContent } from '../BucketStorage.js';
import { MongoPersistedSyncRules } from './MongoPersistedSyncRules.js';
import { MongoSyncRulesLock } from './MongoSyncRulesLock.js';
import { PowerSyncMongo } from './db.js';
import { SyncRuleDocument } from './models.js';

export class MongoPersistedSyncRulesContent implements PersistedSyncRulesContent {
  public readonly slot_name: string;

  public readonly id: number;
  public readonly sync_rules_content: string;
  public readonly last_checkpoint_lsn: string | null;
  public readonly last_fatal_error: string | null;
  public readonly last_keepalive_ts: Date | null;
  public readonly last_checkpoint_ts: Date | null;

  public current_lock: MongoSyncRulesLock | null = null;

  constructor(
    private db: PowerSyncMongo,
    doc: mongo.WithId<SyncRuleDocument>
  ) {
    this.id = doc._id;
    this.sync_rules_content = doc.content;
    this.last_checkpoint_lsn = doc.last_checkpoint_lsn;
    // Handle legacy values
    this.slot_name = doc.slot_name ?? `powersync_${this.id}`;
    this.last_fatal_error = doc.last_fatal_error;
    this.last_checkpoint_ts = doc.last_checkpoint_ts;
    this.last_keepalive_ts = doc.last_keepalive_ts;
  }

  parsed(options: ParseSyncRulesOptions) {
    return new MongoPersistedSyncRules(
      this.id,
      SqlSyncRules.fromYaml(this.sync_rules_content, options),
      this.last_checkpoint_lsn,
      this.slot_name
    );
  }

  async lock() {
    const lock = await MongoSyncRulesLock.createLock(this.db, this);
    this.current_lock = lock;
    return lock;
  }
}
