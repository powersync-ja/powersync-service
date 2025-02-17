import { ErrorCode, logger, ServiceError } from '@powersync/lib-services-framework';
import { RouteAPI } from '../api/RouteAPI.js';
import { BucketStorageFactory } from '../storage/BucketStorage.js';

export interface CreateWriteCheckpointOptions {
  userId: string | undefined;
  clientId: string | undefined;
  api: RouteAPI;
  storage: BucketStorageFactory;
}
export async function createWriteCheckpoint(options: CreateWriteCheckpointOptions) {
  const full_user_id = checkpointUserId(options.userId, options.clientId);

  const activeSyncRules = await options.storage.getActiveSyncRulesContent();
  if (!activeSyncRules) {
    throw new ServiceError(ErrorCode.PSYNC_S2302, `Cannot create Write Checkpoint since no sync rules are active.`);
  }

  using syncBucketStorage = options.storage.getInstance(activeSyncRules);

  const { writeCheckpoint, currentCheckpoint } = await options.api.createReplicationHead(async (currentCheckpoint) => {
    const writeCheckpoint = await syncBucketStorage.createManagedWriteCheckpoint({
      user_id: full_user_id,
      heads: { '1': currentCheckpoint }
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
