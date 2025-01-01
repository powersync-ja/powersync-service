import {
  ActiveCheckpoint,
  BucketStorageFactory,
  OpId,
  OplogEntry,
  SyncRulesBucketStorage
} from '@powersync/service-core';
import { TEST_CONNECTION_OPTIONS, clearTestDb } from './util.js';
import { fromAsync } from '@core-tests/stream_utils.js';
import { BinLogStream, BinLogStreamOptions } from '@module/replication/BinLogStream.js';
import { MySQLConnectionManager } from '@module/replication/MySQLConnectionManager.js';
import mysqlPromise from 'mysql2/promise';
import { readExecutedGtid } from '@module/common/read-executed-gtid.js';
import { logger } from '@powersync/lib-services-framework';

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

  static async create(factory: () => Promise<BucketStorageFactory>) {
    const f = await factory();
    const connectionManager = new MySQLConnectionManager(TEST_CONNECTION_OPTIONS, {});

    const connection = await connectionManager.getConnection();
    await clearTestDb(connection);
    connection.release();
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
    return fromAsync(this.storage!.getBucketDataBatch(checkpoint, map));
  }

  async getBucketData(bucket: string, start = '0', options?: { timeout?: number }): Promise<OplogEntry[]> {
    const checkpoint = await this.getCheckpoint(options);
    const map = new Map<string, string>([[bucket, start]]);
    const batch = this.storage!.getBucketDataBatch(checkpoint, map);
    const batches = await fromAsync(batch);
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
