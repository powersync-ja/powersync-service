import { ErrorCode, ServiceAssertionError, ServiceError } from '@powersync/lib-services-framework';
import { RouteAPI } from '../api/RouteAPI.js';
import { BucketStorageFactory, SyncRulesBucketStorage } from '../storage/storage-index.js';

// Keep up to two source-head/storage batches executing concurrently under load.
// There is intentionally no explicit batch-size cap here: the HTTP request queue
// already bounds how many requests can be waiting in this process.
// Smaller values here have better efficiency, while larger numbers here may give slight improvements in latency.
// We want to limit the number of database connections in use for write checkpoints, so we keep the numbers low here.
export const MAX_IN_FLIGHT_WRITE_CHECKPOINT_BATCHES = 3;

export interface CreateWriteCheckpointResult {
  writeCheckpoint: string;
  replicationHead: string;
}

interface QueuedWriteCheckpoint {
  userId: string;
  resolvers: PromiseWithResolvers<CreateWriteCheckpointResult>;
}

export class WriteCheckpointBatcher {
  private pending: QueuedWriteCheckpoint[] = [];
  private inFlight = 0;

  constructor(
    private readonly getAPI: () => RouteAPI,
    private readonly getStorage: () => BucketStorageFactory
  ) {}

  enqueue(userId: string): Promise<CreateWriteCheckpointResult> {
    const resolvers = Promise.withResolvers<CreateWriteCheckpointResult>();
    this.pending.push({ userId, resolvers });
    this.pump();
    return resolvers.promise;
  }

  private pump() {
    while (this.inFlight < MAX_IN_FLIGHT_WRITE_CHECKPOINT_BATCHES && this.pending.length > 0) {
      // Dispatch immediately while capacity is available. Requests only batch
      // together once all executing slots are saturated and the queue starts
      // accumulating behind them.
      const batch = this.pending;
      this.pending = [];
      this.inFlight++;

      void this.executeBatch(batch).finally(() => {
        this.inFlight--;
        this.pump();
      });
    }
  }

  private async executeBatch(batch: QueuedWriteCheckpoint[]) {
    try {
      const syncBucketStorage = (await this.getStorage().getActiveSyncConfig())?.storage;
      if (!syncBucketStorage) {
        throw new ServiceError(ErrorCode.PSYNC_S2302, `Cannot create Write Checkpoint since no sync config is active.`);
      }

      const { writeCheckpoints, currentCheckpoint } = await this.getAPI().createReplicationHead(
        async (currentCheckpoint) => {
          const writeCheckpoints = await this.createBatchWriteCheckpoints(syncBucketStorage, batch, currentCheckpoint);
          return { writeCheckpoints, currentCheckpoint };
        }
      );

      const results = batch.map((request) => {
        const writeCheckpoint = writeCheckpoints.get(request.userId);
        if (writeCheckpoint == null) {
          throw new ServiceAssertionError(
            `Write checkpoint storage did not return a checkpoint for user ${request.userId}`
          );
        }

        return { request, writeCheckpoint };
      });

      for (const { request, writeCheckpoint } of results) {
        request.resolvers.resolve({
          writeCheckpoint: String(writeCheckpoint),
          replicationHead: currentCheckpoint
        });
      }
    } catch (error) {
      // Do not retry here. The route should pass failures through so clients can
      // apply their normal write-checkpoint retry policy.
      for (const request of batch) {
        request.resolvers.reject(error);
      }
    }
  }

  private async createBatchWriteCheckpoints(
    syncBucketStorage: SyncRulesBucketStorage,
    batch: QueuedWriteCheckpoint[],
    currentCheckpoint: string
  ) {
    return syncBucketStorage.createManagedWriteCheckpoints(
      batch.map((request) => ({
        user_id: request.userId,
        heads: { '1': currentCheckpoint }
      }))
    );
  }
}
