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
      // Cosmos DB: operationTime / clusterTime not available on regular
      // commands, so createReplicationHead cannot capture the HEAD directly.
      // Instead, use the current storage checkpoint LSN. This is valid because:
      //
      // 1. createReplicationHead already wrote a sentinel to _powersync_checkpoints,
      //    guaranteeing the streaming loop will advance past this point.
      // 2. The sync stream's watchCheckpointChanges resolves the write checkpoint
      //    when replication advances past the stored HEAD.
      // 3. The sentinel ensures forward progress even on an idle system.
      //
      // The HEAD doesn't need to be the exact sentinel position — it just needs
      // to be a valid LSN at or before the sentinel. The current storage LSN
      // satisfies this because it was committed before the sentinel was written.
      const cp = await syncBucketStorage.getCheckpoint();
      if (!cp?.lsn) {
        throw new ServiceError(ErrorCode.PSYNC_S2302, 'Cannot create write checkpoint: no replication checkpoint available');
      }
      head = cp.lsn;
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
