import { logger, router, schema } from '@powersync/lib-services-framework';
import * as t from 'ts-codec';

import { authUser } from '../auth.js';
import { routeDefinition } from '../router.js';

const WriteCheckpointRequest = t.object({});

export const writeCheckpoint = routeDefinition({
  path: '/write-checkpoint.json',
  method: router.HTTPMethod.GET,
  authorize: authUser,
  validator: schema.createTsCodecValidator(WriteCheckpointRequest, { allowAdditional: true }),
  handler: async (payload) => {
    const {
      context: { service_context }
    } = payload;
    const api = service_context.routerEngine.getAPI();

    // This old API needs a persisted checkpoint id.
    // Since we don't use LSNs anymore, the only way to get that is to wait.
    const start = Date.now();

    const head = await api.getReplicationHead();

    const timeout = 50_000;

    logger.info(`Waiting for LSN checkpoint: ${head}`);
    while (Date.now() - start < timeout) {
      const cp = await service_context.storage.activeBucketStorage.getActiveCheckpoint();
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

    const api = service_context.routerEngine.getAPI();

    const currentCheckpoint = await api.getReplicationHead();
    const {
      storage: { activeBucketStorage }
    } = service_context;

    const id = await activeBucketStorage.createWriteCheckpoint(user_id!, { '1': currentCheckpoint });
    logger.info(`Write checkpoint 2: ${JSON.stringify({ currentCheckpoint, id: String(id) })}`);

    return {
      write_checkpoint: String(id)
    };
  }
});

export const CHECKPOINT_ROUTES = [writeCheckpoint, writeCheckpoint2];
