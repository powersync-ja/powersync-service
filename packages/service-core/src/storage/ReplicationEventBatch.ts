import * as sync_rules from '@powersync/service-sync-rules';
import * as storage from '../storage/storage-index.js';
import { EventData, ReplicationEventData, ReplicationEventManager } from './ReplicationEventManager.js';

export type ReplicationEventBatchOptions = {
  manager: ReplicationEventManager;
  storage: storage.SyncRulesBucketStorage;
  max_batch_size?: number;
};

export type ReplicationEventWriteParams = {
  event: sync_rules.SqlEventDescriptor;
  table: storage.SourceTable;
  data: EventData;
};

const MAX_BATCH_SIZE = 1000;

export class ReplicationEventBatch {
  readonly manager: ReplicationEventManager;
  readonly maxBatchSize: number;
  readonly storage: storage.SyncRulesBucketStorage;

  protected event_cache: Map<sync_rules.SqlEventDescriptor, ReplicationEventData>;

  /**
   * Keeps track of the number of rows in the cache.
   * This avoids having to calculate the size on demand.
   */
  private _eventCacheRowCount: number;

  constructor(options: ReplicationEventBatchOptions) {
    this.event_cache = new Map();
    this.manager = options.manager;
    this.maxBatchSize = options.max_batch_size || MAX_BATCH_SIZE;
    this.storage = options.storage;
    this._eventCacheRowCount = 0;
  }

  /**
   * Returns the number of rows/events in the cache
   */
  get cacheSize() {
    return this._eventCacheRowCount;
  }

  dispose() {
    this.event_cache.clear();
    // This is not required, but cleans things up a bit.
    this._eventCacheRowCount = 0;
  }

  /**
   * Queues a replication event. The cache is automatically flushed
   * if it exceeds {@link ReplicationEventBatchOptions['max_batch_size']}.
   */
  async save(params: ReplicationEventWriteParams) {
    const { data, event, table } = params;
    if (!this.event_cache.has(event)) {
      this.event_cache.set(event, new Map());
    }

    const eventEntry = this.event_cache.get(event)!;

    if (!eventEntry.has(table)) {
      eventEntry.set(table, []);
    }

    const tableEntry = eventEntry.get(table)!;
    tableEntry.push(data);
    this._eventCacheRowCount++;

    if (this.cacheSize >= this.maxBatchSize) {
      await this.flush();
    }
  }

  /**
   * Flushes cached changes. Events will be emitted to the
   * {@link ReplicationEventManager}.
   */
  async flush() {
    await this.manager.fireEvents({
      batch_data: this.event_cache,
      storage: this.storage
    });

    this.event_cache.clear();
    this._eventCacheRowCount = 0;
  }
}
