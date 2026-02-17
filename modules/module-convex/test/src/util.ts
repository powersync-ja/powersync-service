import * as mongo_storage from '@powersync/service-module-mongodb-storage';
import {
  BucketStorageFactory,
  createCoreReplicationMetrics,
  initializeCoreReplicationMetrics,
  InternalOpId,
  OplogEntry,
  storage,
  SyncRulesBucketStorage
} from '@powersync/service-core';
import { METRICS_HELPER, test_utils } from '@powersync/service-core-tests';

import { ConvexConnectionManager } from '@module/replication/ConvexConnectionManager.js';
import { ConvexStream, ConvexStreamOptions } from '@module/replication/ConvexStream.js';
import { normalizeConnectionConfig } from '@module/types/types.js';
import { ConvexLSN } from '@module/common/ConvexLSN.js';
import { env } from './env.js';

export const INITIALIZED_MONGO_STORAGE_FACTORY = mongo_storage.test_utils.mongoTestStorageFactoryGenerator({
  url: env.MONGO_TEST_URL,
  isCI: env.CI
});

export function makeConvexConnectionManager() {
  if (!env.CONVEX_DEPLOY_KEY) {
    throw new Error('CONVEX_DEPLOY_KEY is required for slow tests');
  }

  const config = normalizeConnectionConfig({
    type: 'convex',
    deployment_url: env.CONVEX_URL,
    deploy_key: env.CONVEX_DEPLOY_KEY,
    polling_interval_ms: 200,
    request_timeout_ms: 15_000
  });

  return new ConvexConnectionManager(config);
}

export class ConvexStreamTestContext {
  private _stream?: ConvexStream;
  private abortController = new AbortController();
  private streamPromise?: Promise<void>;
  public storage?: SyncRulesBucketStorage;

  static async open(factory: storage.TestStorageFactory) {
    const f = await factory({});
    const connectionManager = makeConvexConnectionManager();
    return new ConvexStreamTestContext(f, connectionManager);
  }

  constructor(
    public factory: BucketStorageFactory,
    public connectionManager: ConvexConnectionManager
  ) {
    createCoreReplicationMetrics(METRICS_HELPER.metricsEngine);
    initializeCoreReplicationMetrics(METRICS_HELPER.metricsEngine);
  }

  abort() {
    this.abortController.abort();
  }

  async dispose() {
    this.abort();
    await this.streamPromise?.catch(() => {});
    await this.factory[Symbol.asyncDispose]();
    await this.connectionManager.end();
  }

  async [Symbol.asyncDispose]() {
    await this.dispose();
  }

  get client() {
    return this.connectionManager.client;
  }

  async updateSyncRules(content: string) {
    const syncRules = await this.factory.updateSyncRules({ content, validate: true });
    this.storage = this.factory.getInstance(syncRules);
    return this.storage!;
  }

  get stream(): ConvexStream {
    if (this.storage == null) {
      throw new Error('Call updateSyncRules() first');
    }
    if (this._stream) {
      return this._stream;
    }

    const options: ConvexStreamOptions = {
      storage: this.storage,
      metrics: METRICS_HELPER.metricsEngine,
      connections: this.connectionManager,
      abortSignal: this.abortController.signal
    };
    this._stream = new ConvexStream(options);
    return this._stream;
  }

  async replicateSnapshot() {
    await this.stream.initReplication();
  }

  startStreaming() {
    this.streamPromise = this.stream.streamChanges();
    return this.streamPromise;
  }

  async waitForCheckpoint(options?: { timeout?: number }): Promise<InternalOpId> {
    const timeout = options?.timeout ?? 30_000;
    const start = Date.now();

    while (Date.now() - start < timeout) {
      const activeStorage = await this.factory.getActiveStorage();
      const cp = await activeStorage?.getCheckpoint();
      if (cp != null && cp.lsn) {
        return cp.checkpoint;
      }
      await new Promise((resolve) => setTimeout(resolve, 100));
    }

    throw new Error(`Timeout waiting for checkpoint after ${timeout}ms`);
  }

  async getBucketData(bucket: string, options?: { timeout?: number }): Promise<OplogEntry[]> {
    const checkpoint = await this.waitForCheckpoint(options);
    const map = new Map<string, InternalOpId>([[bucket, 0n]]);
    let data: OplogEntry[] = [];

    while (true) {
      const batch = this.storage!.getBucketDataBatch(checkpoint, map);
      const batches = await test_utils.fromAsync(batch);
      data = data.concat(batches[0]?.chunkData.data ?? []);
      if (batches.length == 0 || !batches[0]!.chunkData.has_more) {
        break;
      }
      map.set(bucket, BigInt(batches[0]!.chunkData.next_after));
    }

    return data;
  }

  async getChecksums(buckets: string[], options?: { timeout?: number }) {
    const checkpoint = await this.waitForCheckpoint(options);
    return this.storage!.getChecksums(checkpoint, buckets);
  }

  async getChecksum(bucket: string, options?: { timeout?: number }) {
    const checkpoint = await this.waitForCheckpoint(options);
    const map = await this.storage!.getChecksums(checkpoint, [bucket]);
    return map.get(bucket);
  }

  /**
   * After snapshot, manually advance the checkpoint so bucket data is queryable
   * without requiring full delta streaming to catch up.
   */
  async markSnapshotQueryable() {
    const status = await this.storage!.getStatus();
    if (!status.checkpoint_lsn) {
      throw new Error('No checkpoint LSN available - run snapshot first');
    }

    await this.storage!.startBatch(
      {
        defaultSchema: 'convex',
        zeroLSN: ConvexLSN.ZERO.comparable,
        storeCurrentData: false,
        skipExistingRows: false
      },
      async (batch) => {
        await batch.keepalive(status.checkpoint_lsn!);
      }
    );
  }
}
