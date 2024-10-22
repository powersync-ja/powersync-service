import { logger, router, schema } from '@powersync/lib-services-framework';
import * as t from 'ts-codec';

import * as framework from '@powersync/lib-services-framework';
import * as util from '../../util/util-index.js';
import { authUser } from '../auth.js';
import { routeDefinition } from '../router.js';

const WriteCheckpointRequest = t.object({
  client_id: t.string.optional()
});

export const writeCheckpoint = routeDefinition({
  path: '/write-checkpoint.json',
  method: router.HTTPMethod.GET,
  authorize: authUser,
  validator: schema.createTsCodecValidator(WriteCheckpointRequest, { allowAdditional: true }),
  handler: async (payload) => {
    const {
      context: { service_context }
    } = payload;
    const apiHandler = service_context.routerEngine!.getAPI();

    // This old API needs a persisted checkpoint id.
    // Since we don't use LSNs anymore, the only way to get that is to wait.
    const start = Date.now();

    const head = await apiHandler.getReplicationHead();

    const timeout = 50_000;

    logger.info(`Waiting for LSN checkpoint: ${head}`);
    while (Date.now() - start < timeout) {
      const cp = await service_context.storageEngine.activeBucketStorage.getActiveCheckpoint();
      if (!cp.hasSyncRules()) {
        throw new Error('No sync rules available');
      }
      if (cp.lsn && cp.lsn >= head) {
        logger.info(`Got write checkpoint: ${head} : ${cp.checkpoint}`);
        return { checkpoint: cp.checkpoint };
      }

      await new Promise((resolve) => setTimeout(resolve, 30));
    }
    throw new Error('Timeout while waiting for checkpoint');
  }
});

export const writeCheckpoint2 = routeDefinition({
  path: '/write-checkpoint2.json',
  method: router.HTTPMethod.GET,
  authorize: authUser,
  validator: schema.createTsCodecValidator(WriteCheckpointRequest, { allowAdditional: true }),
  handler: async (payload) => {
    const { user_id, service_context } = payload.context;

    const apiHandler = service_context.routerEngine!.getAPI();

    const client_id = payload.params.client_id;
    const full_user_id = util.checkpointUserId(user_id, client_id);

    const currentCheckpoint = await apiHandler.getReplicationHead();
    const {
      storageEngine: { activeBucketStorage }
    } = service_context;

    const activeSyncRules = await activeBucketStorage.getActiveSyncRulesContent();
    if (!activeSyncRules) {
      throw new framework.errors.ValidationError(`Cannot create Write Checkpoint since no sync rules are active.`);
    }

    using syncBucketStorage = activeBucketStorage.getInstance(activeSyncRules);
    const writeCheckpoint = await syncBucketStorage.createManagedWriteCheckpoint({
      user_id: full_user_id,
      heads: { '1': currentCheckpoint }
    });
    logger.info(`Write checkpoint 2: ${JSON.stringify({ currentCheckpoint, id: String(full_user_id) })}`);

    return {
      write_checkpoint: String(writeCheckpoint)
    };
  }
});

export const CHECKPOINT_ROUTES = [writeCheckpoint, writeCheckpoint2];
