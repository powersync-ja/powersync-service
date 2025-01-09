import { readExecutedGtid } from '@module/common/read-executed-gtid.js';
import { BinLogStream, BinLogStreamOptions } from '@module/replication/BinLogStream.js';
import { MySQLConnectionManager } from '@module/replication/MySQLConnectionManager.js';
import { logger } from '@powersync/lib-services-framework';
import {
  ActiveCheckpoint,
  BucketStorageFactory,
  OpId,
  OplogEntry,
  storage,
  SyncRulesBucketStorage
} from '@powersync/service-core';
import { test_utils } from '@powersync/service-core-tests';
import mysqlPromise from 'mysql2/promise';
import { clearTestDb, TEST_CONNECTION_OPTIONS } from './util.js';

/**
 * Tests operating on the binlog stream need to configure the stream and manage asynchronous
 * replication, which gets a little tricky.
 *
 * This wraps all the context required for testing, and tears it down afterward
 * by using `await using`.
 */
export class BinlogStreamTestContext {
  private _binlogStream?: BinLogStream;
  private abortController = new AbortController();
  private streamPromise?: Promise<void>;
  public storage?: SyncRulesBucketStorage;
  private replicationDone = false;

  static async open(factory: storage.TestStorageFactory, options?: { doNotClear?: boolean }) {
    const f = await factory({ doNotClear: options?.doNotClear });
    const connectionManager = new MySQLConnectionManager(TEST_CONNECTION_OPTIONS, {});

    if (!options?.doNotClear) {
      const connection = await connectionManager.getConnection();
      await clearTestDb(connection);
      connection.release();
    }
    return new BinlogStreamTestContext(f, connectionManager);
  }

  constructor(
    public factory: BucketStorageFactory,
    public connectionManager: MySQLConnectionManager
  ) {}

  async dispose() {
    this.abortController.abort();
    await this.streamPromise;
    await this.connectionManager.end();
    this.factory[Symbol.dispose]();
  }

  [Symbol.asyncDispose]() {
    return this.dispose();
  }

  get connectionTag() {
    return this.connectionManager.connectionTag;
  }

  async updateSyncRules(content: string): Promise<SyncRulesBucketStorage> {
    const syncRules = await this.factory.updateSyncRules({ content: content });
    this.storage = this.factory.getInstance(syncRules);
    return this.storage!;
  }

  async loadNextSyncRules() {
    const syncRules = await this.factory.getNextSyncRulesContent();
    if (syncRules == null) {
      throw new Error(`Next sync rules not available`);
    }

    this.storage = this.factory.getInstance(syncRules);
    return this.storage!;
  }

  async loadActiveSyncRules() {
    const syncRules = await this.factory.getActiveSyncRulesContent();
    if (syncRules == null) {
      throw new Error(`Active sync rules not available`);
    }

    this.storage = this.factory.getInstance(syncRules);
    this.replicationDone = true;
    return this.storage!;
  }

  get binlogStream(): BinLogStream {
    if (this.storage == null) {
      throw new Error('updateSyncRules() first');
    }
    if (this._binlogStream) {
      return this._binlogStream;
    }
    const options: BinLogStreamOptions = {
      storage: this.storage,
      connections: this.connectionManager,
      abortSignal: this.abortController.signal
    };
    this._binlogStream = new BinLogStream(options);
    return this._binlogStream!;
  }

  async replicateSnapshot() {
    await this.binlogStream.initReplication();
    this.replicationDone = true;
  }

  startStreaming() {
    if (!this.replicationDone) {
      throw new Error('Call replicateSnapshot() before startStreaming()');
    }
    this.streamPromise = this.binlogStream.streamChanges();
  }

  async getCheckpoint(options?: { timeout?: number }): Promise<string> {
    const connection = await this.connectionManager.getConnection();
    let checkpoint = await Promise.race([
      getClientCheckpoint(connection, this.factory, { timeout: options?.timeout ?? 60_000 }),
      this.streamPromise
    ]);
    connection.release();
    if (typeof checkpoint == undefined) {
      // This indicates an issue with the test setup - streamingPromise completed instead
      // of getClientCheckpoint()
      throw new Error('Test failure - streamingPromise completed');
    }
    return checkpoint as string;
  }

  async getBucketsDataBatch(buckets: Record<string, string>, options?: { timeout?: number }) {
    const checkpoint = await this.getCheckpoint(options);
    const map = new Map<string, string>(Object.entries(buckets));
    return test_utils.fromAsync(this.storage!.getBucketDataBatch(checkpoint, map));
  }

  async getBucketData(bucket: string, start = '0', options?: { timeout?: number }): Promise<OplogEntry[]> {
    const checkpoint = await this.getCheckpoint(options);
    const map = new Map<string, string>([[bucket, start]]);
    const batch = this.storage!.getBucketDataBatch(checkpoint, map);
    const batches = await test_utils.fromAsync(batch);
    return batches[0]?.batch.data ?? [];
  }
}

export async function getClientCheckpoint(
  connection: mysqlPromise.Connection,
  bucketStorage: BucketStorageFactory,
  options?: { timeout?: number }
): Promise<OpId> {
  const start = Date.now();
  const gtid = await readExecutedGtid(connection);
  // This old API needs a persisted checkpoint id.
  // Since we don't use LSNs anymore, the only way to get that is to wait.

  const timeout = options?.timeout ?? 50_000;
  let lastCp: ActiveCheckpoint | null = null;

  logger.info('Expected Checkpoint: ' + gtid.comparable);
  while (Date.now() - start < timeout) {
    const cp = await bucketStorage.getActiveCheckpoint();
    lastCp = cp;
    //logger.info('Last Checkpoint: ' + lastCp.lsn);
    if (!cp.hasSyncRules()) {
      throw new Error('No sync rules available');
    }
    if (cp.lsn && cp.lsn >= gtid.comparable) {
      return cp.checkpoint;
    }

    await new Promise((resolve) => setTimeout(resolve, 30));
  }

  throw new Error(`Timeout while waiting for checkpoint ${gtid.comparable}. Last checkpoint: ${lastCp?.lsn}`);
}
