import { ErrorCode, ServiceAssertionError, ServiceError } from '@powersync/lib-services-framework';
import { ReplicationHeadCallback } from '../api/RouteAPI.js';
import {
  BucketStorageFactory,
  ManagedWriteCheckpointOptions,
  SyncRulesBucketStorage
} from '../storage/storage-index.js';

// Keep up to three source-head/storage batches executing concurrently under load.
// There is intentionally no explicit batch-size cap here: the HTTP request queue
// already bounds how many requests can be waiting in this process.
// Smaller values here have better efficiency, while larger numbers here may give slight improvements in latency.
// We want to limit the number of database connections in use for write checkpoints, so we keep the numbers low here.
const MAX_IN_FLIGHT_WRITE_CHECKPOINT_BATCHES = 3;

export interface CreateWriteCheckpointResult {
  writeCheckpoint: string;
  replicationHead: string;
}

export type CreateReplicationHead = <T>(callback: ReplicationHeadCallback<T>) => Promise<T>;

interface QueuedWriteCheckpoint {
  userId: string;
  checkpointRequestId: bigint | undefined;
  resolvers: PromiseWithResolvers<CreateWriteCheckpointResult>;
}

export class WriteCheckpointBatcher {
  private pending: QueuedWriteCheckpoint[] = [];
  private inFlight = 0;
  private scheduledPump: NodeJS.Timeout | undefined;

  constructor(
    private readonly getCreateReplicationHead: () => CreateReplicationHead,
    private readonly getStorage: () => BucketStorageFactory
  ) {}

  enqueue(userId: string, checkpointRequestId?: bigint): Promise<CreateWriteCheckpointResult> {
    const resolvers = Promise.withResolvers<CreateWriteCheckpointResult>();
    this.pending.push({ userId, checkpointRequestId, resolvers });
    this.schedulePump();
    return resolvers.promise;
  }

  private schedulePump() {
    if (this.scheduledPump != null) {
      return;
    }

    // Delay dispatch by one timer turn so requests arriving together can share
    // one source head and one storage write batch.
    // While normally we wouldn't have http requests arriving at the exact same time, this is relevant when
    // requests are queued by fastify: All current requests are typically batched together, and finish at the
    // same time. Then we get many requests from the queue arriving at the same time. Without this delayed dispatch,
    // the first request would form its own batch, and the rest will then wait for a new batch to become available.
    // The delayed dispatch allow all those requests to be batched together.
    this.scheduledPump = setTimeout(() => {
      this.scheduledPump = undefined;
      this.pump();
    }, 0);
  }

  private pump() {
    while (this.inFlight < MAX_IN_FLIGHT_WRITE_CHECKPOINT_BATCHES && this.pending.length > 0) {
      const batch = this.pending;
      this.pending = [];
      this.inFlight++;

      void this.executeBatch(batch).finally(() => {
        this.inFlight--;
        this.schedulePump();
      });
    }
  }

  private async executeBatch(batch: QueuedWriteCheckpoint[]) {
    try {
      const syncBucketStorage = (await this.getStorage().getActiveSyncConfig())?.storage;
      if (!syncBucketStorage) {
        throw new ServiceError(ErrorCode.PSYNC_S2302, `Cannot create Write Checkpoint since no sync config is active.`);
      }

      const { writeCheckpoints, currentCheckpoint } = await this.getCreateReplicationHead()(
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
      batch.map((request) => {
        const checkpoint: ManagedWriteCheckpointOptions = {
          user_id: request.userId,
          heads: { '1': currentCheckpoint }
        };
        if (request.checkpointRequestId != null) {
          checkpoint.checkpoint_request_id = request.checkpointRequestId;
        }
        return checkpoint;
      })
    );
  }
}
