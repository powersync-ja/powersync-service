import { ErrorCode, ServiceError } from '@powersync/lib-services-framework';
import { RouteAPI } from '../api/RouteAPI.js';
import { BucketStorageFactory } from '../storage/storage-index.js';

export interface CreateWriteCheckpointOptions {
  userId: string | undefined;
  clientId: string | undefined;
  api: RouteAPI;
  storage: BucketStorageFactory;
  checkpointRequestId?: bigint;
}
export async function createWriteCheckpoint(options: CreateWriteCheckpointOptions) {
  const full_user_id = checkpointUserId(options.userId, options.clientId);
  const { checkpointRequestId } = options;

  const syncBucketStorage = (await options.storage.getActiveSyncConfig())?.storage;
  if (!syncBucketStorage) {
    throw new ServiceError(ErrorCode.PSYNC_S2302, `Cannot create Write Checkpoint since no sync config is active.`);
  }

  const { writeCheckpoint, currentCheckpoint } = await options.api.createReplicationHead(async (currentCheckpoint) => {
    const writeCheckpoint = await syncBucketStorage.createManagedWriteCheckpoint({
      user_id: full_user_id,
      heads: { '1': currentCheckpoint },
      checkpoint_request_id: checkpointRequestId
    });
    return { writeCheckpoint, currentCheckpoint };
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
