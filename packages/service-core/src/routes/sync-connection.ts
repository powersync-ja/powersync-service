import { ErrorCode, errors } from '@powersync/lib-services-framework';

import { recordSyncConnection, SyncCloseReason, SyncTransport } from '../metrics/connection-metrics.js';
import type { RouterServiceContext } from './router.js';

/** Resolves stream resources, counting setup failures as rejections before rethrowing. */
export async function resolveSyncConnectionSetup(serviceContext: RouterServiceContext, transport: SyncTransport) {
  const { routerEngine, storageEngine, metricsEngine } = serviceContext;
  let closeReason = SyncCloseReason.ServiceUnavailable;

  try {
    if (routerEngine.closed) {
      throw new errors.ServiceError({
        status: 503,
        code: ErrorCode.PSYNC_S2003,
        description: 'Service temporarily unavailable'
      });
    }

    closeReason = SyncCloseReason.StorageError;
    const bucketStorage = (await storageEngine.activeBucketStorage.getActiveSyncConfig())?.storage;

    closeReason = SyncCloseReason.NoSyncConfig;
    if (bucketStorage == null) {
      throw new errors.ServiceError({
        status: 500,
        code: ErrorCode.PSYNC_S2302,
        description: 'No sync config available'
      });
    }

    closeReason = SyncCloseReason.SyncConfigError;
    const syncRules = bucketStorage.getParsedSyncRules(routerEngine.getAPI().getParseSyncRulesOptions());
    return { bucketStorage, syncRules };
  } catch (error) {
    recordSyncConnection(metricsEngine, { transport, closeReason, error });
    throw error;
  }
}
