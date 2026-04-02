import { ErrorCode, ServiceError } from '@powersync/lib-services-framework';
import { RouteAPI } from '../api/RouteAPI.js';
import { BucketStorageFactory } from '../storage/storage-index.js';

export interface CreateWriteCheckpointOptions {
  userId: string | undefined;
  clientId: string | undefined;
  api: RouteAPI;
  storage: BucketStorageFactory;
}
export async function createWriteCheckpoint(options: CreateWriteCheckpointOptions) {
  const full_user_id = checkpointUserId(options.userId, options.clientId);

  const syncBucketStorage = await options.storage.getActiveStorage();
  if (!syncBucketStorage) {
    throw new ServiceError(ErrorCode.PSYNC_S2302, `Cannot create Write Checkpoint since no sync rules are active.`);
  }

  const { writeCheckpoint, currentCheckpoint } = await options.api.createReplicationHead(async (currentCheckpoint) => {
    let head = currentCheckpoint;

    if (head == null) {
      // Cosmos DB: HEAD unknown. Poll storage until the streaming loop
      // processes the sentinel and advances the checkpoint LSN.
      const baselineCheckpoint = await syncBucketStorage.getCheckpoint();
      const baselineLsn = baselineCheckpoint?.lsn ?? '';

      const timeout = 30_000;
      const start = Date.now();
      while (Date.now() - start < timeout) {
        const cp = await syncBucketStorage.getCheckpoint();
        if (cp?.lsn && cp.lsn > baselineLsn) {
          head = cp.lsn;
          break;
        }
        await new Promise((r) => setTimeout(r, 50));
      }
      if (!head) {
        throw new ServiceError(ErrorCode.PSYNC_S2302, 'Timeout waiting for sentinel checkpoint');
      }
    }

    const writeCheckpoint = await syncBucketStorage.createManagedWriteCheckpoint({
      user_id: full_user_id,
      heads: { '1': head }
    });
    return { writeCheckpoint, currentCheckpoint: head };
  });

  return {
    writeCheckpoint: String(writeCheckpoint),
    replicationHead: currentCheckpoint
  };
}

export function checkpointUserId(user_id: string | undefined, client_id: string | undefined) {
  if (user_id == null) {
    throw new Error('user_id is required');
  }
  if (client_id == null) {
    return user_id;
  }
  return `${user_id}/${client_id}`;
}
