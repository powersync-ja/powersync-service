import { ConvexConnectionManager } from '@module/replication/ConvexConnectionManager.js';
import { ConvexStream, ConvexStreamOptions } from '@module/replication/ConvexStream.js';
import { logger } from '@powersync/lib-services-framework';
import {
  BucketStorageFactory,
  createCoreReplicationMetrics,
  initializeCoreReplicationMetrics,
  LEGACY_STORAGE_VERSION,
  ReplicationCheckpoint,
  storage
} from '@powersync/service-core';
import { AbstractStreamTestContext, METRICS_HELPER } from '@powersync/service-core-tests';
import { clearTestDb, connectConvex, TEST_CONNECTION_OPTIONS, TestConvexConnection } from './util.js';

export class ConvexStreamTestContext extends AbstractStreamTestContext {
  protected _stream?: ConvexStream;

  /**
   * Tests operating on the stream need to configure the stream and manage asynchronous
   * replication, which gets a little tricky.
   *
   * This configures all the context, and tears it down afterwards.
   */
  static async open(
    factory: (options: storage.TestStorageOptions) => Promise<BucketStorageFactory>,
    options?: {
      doNotClear?: boolean;
      storageVersion?: number;
      streamOptions?: Partial<ConvexStreamOptions>;
      clearSource?: boolean;
    }
  ) {
    const f = await factory({ doNotClear: options?.doNotClear });
    const connectionManager = new ConvexConnectionManager(TEST_CONNECTION_OPTIONS);

    const convexBackend = connectConvex();

    if (options?.clearSource ?? !options?.doNotClear) {
      await clearTestDb(convexBackend);
    }

    const storageVersion = options?.storageVersion ?? LEGACY_STORAGE_VERSION;

    return new ConvexStreamTestContext(f, connectionManager, convexBackend, options?.streamOptions, storageVersion);
  }

  constructor(
    public factory: BucketStorageFactory,
    public connectionManager: ConvexConnectionManager,
    public backend: TestConvexConnection,
    protected streamOptions?: Partial<ConvexStreamOptions>,
    protected storageVersion: number = LEGACY_STORAGE_VERSION
  ) {
    super();
    createCoreReplicationMetrics(METRICS_HELPER.metricsEngine);
    initializeCoreReplicationMetrics(METRICS_HELPER.metricsEngine);
  }

  get connectionTag() {
    return this.connectionManager.connectionTag;
  }

  get stream() {
    if (this.storage == null) {
      throw new Error('updateSyncRules() first');
    }
    if (this._stream) {
      return this._stream;
    }
    const options: ConvexStreamOptions = {
      storage: this.storage,
      metrics: METRICS_HELPER.metricsEngine,
      connections: this.connectionManager,
      abortSignal: this.abortController.signal,
      ...this.streamOptions
    };
    this._stream = new ConvexStream(options);
    return this._stream!;
  }

  protected async _dispose(): Promise<void> {
    await this.connectionManager.end();
  }

  protected triggerReplication(): Promise<void> {
    return this.stream.replicate();
  }

  protected waitForInitialSnapshot(): Promise<void> {
    return this.stream.waitForInitialSnapshot();
  }

  async getClientCheckpoint(options?: { timeout?: number }): Promise<ReplicationCheckpoint> {
    const start = Date.now();

    const lsn = await this.connectionManager.client.getHeadCursor();
    await this.connectionManager.client.createWriteCheckpointMarker();

    // This old API needs a persisted checkpoint id.
    // Since we don't use LSNs anymore, the only way to get that is to wait.

    const timeout = options?.timeout ?? 50_000;

    logger.info(`Waiting for LSN checkpoint: ${lsn}`);
    while (Date.now() - start < timeout) {
      const storage = (await this.factory.getActiveSyncConfig())?.storage;
      const cp = await storage?.getCheckpoint();

      if (cp?.lsn != null && cp.lsn >= lsn) {
        logger.info(`Got write checkpoint: ${lsn} : ${cp.checkpoint}`);
        return cp;
      }

      await new Promise((resolve) => setTimeout(resolve, 5));
    }

    throw new Error('Timeout while waiting for checkpoint');
  }
}
