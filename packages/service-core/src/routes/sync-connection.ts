import { ErrorCode, errors } from '@powersync/lib-services-framework';
import { HydratedSyncConfig } from '@powersync/service-sync-rules';

import { recordSyncConnection, SyncCloseReason, SyncTransport } from '../metrics/connection-metrics.js';
import type { SyncRulesBucketStorage } from '../storage/storage-index.js';
import type { RouterServiceContext } from './router.js';

/**
 * The connection was rejected before it could be accepted as a sync stream. The rejection has
 * already been counted; the caller is responsible for delivering `error` over its transport.
 */
export type SyncConnectionRejected = { rejected: true; error: errors.ServiceError };

export type SyncConnectionAccepted = {
  rejected: false;
  bucketStorage: SyncRulesBucketStorage;
  syncRules: HydratedSyncConfig;
};

/**
 * Applies the service-state checks an incoming sync connection must pass before it is accepted as
 * a sync stream, and resolves the bucket storage and sync rules it will run against.
 *
 * Router-closed, missing-config, and storage-query failures are counted here before the caller
 * delivers or propagates them. Delivery differs per transport, so a rejection is returned rather
 * than thrown; a failing storage query is counted and rethrown.
 */
export async function resolveSyncConnectionSetup(
  serviceContext: RouterServiceContext,
  transport: SyncTransport
): Promise<SyncConnectionRejected | SyncConnectionAccepted> {
  const { routerEngine, storageEngine, metricsEngine } = serviceContext;

  if (routerEngine.closed) {
    const error = new errors.ServiceError({
      status: 503,
      code: ErrorCode.PSYNC_S2003,
      description: 'Service temporarily unavailable'
    });
    recordSyncConnection(metricsEngine, {
      transport,
      closeReason: SyncCloseReason.ServiceUnavailable,
      error
    });
    return { rejected: true, error };
  }

  let bucketStorage: SyncRulesBucketStorage | undefined;
  try {
    bucketStorage = (await storageEngine.activeBucketStorage.getActiveSyncConfig())?.storage;
  } catch (ex) {
    recordSyncConnection(metricsEngine, {
      transport,
      closeReason: SyncCloseReason.StorageError,
      error: ex
    });
    throw ex;
  }

  if (bucketStorage == null) {
    const error = new errors.ServiceError({
      status: 500,
      code: ErrorCode.PSYNC_S2302,
      description: 'No sync config available'
    });
    recordSyncConnection(metricsEngine, {
      transport,
      closeReason: SyncCloseReason.NoSyncConfig,
      error
    });
    return { rejected: true, error };
  }

  const syncRules = bucketStorage.getParsedSyncRules(routerEngine.getAPI().getParseSyncRulesOptions());
  return { rejected: false, bucketStorage, syncRules };
}
