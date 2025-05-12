import { logger, router, schema } from '@powersync/lib-services-framework';
import * as t from 'ts-codec';

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
    const apiHandler = service_context.routerEngine.getAPI();

    // This old API needs a persisted checkpoint id.
    // Since we don't use LSNs anymore, the only way to get that is to wait.
    const start = Date.now();

    const head = await apiHandler.createReplicationHead(async (head) => head);

    const timeout = 50_000;

    logger.info(`Waiting for LSN checkpoint: ${head}`);
    while (Date.now() - start < timeout) {
      const bucketStorage = await service_context.storageEngine.activeBucketStorage.getActiveStorage();
      const cp = await bucketStorage?.getCheckpoint();
      if (cp == null) {
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

    const apiHandler = service_context.routerEngine.getAPI();

    const { replicationHead, writeCheckpoint } = await util.createWriteCheckpoint({
      userId: user_id,
      clientId: payload.params.client_id,
      api: apiHandler,
      storage: service_context.storageEngine.activeBucketStorage
    });

    logger.info(`Write checkpoint for ${user_id}/${payload.params.client_id}: ${writeCheckpoint} | ${replicationHead}`);

    return {
      write_checkpoint: String(writeCheckpoint)
    };
  }
});

export const CHECKPOINT_ROUTES = [writeCheckpoint, writeCheckpoint2];
