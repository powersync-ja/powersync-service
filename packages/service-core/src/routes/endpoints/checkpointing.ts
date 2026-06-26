import { codecs, logger, router, schema } from '@powersync/lib-services-framework';
import * as t from 'ts-codec';

import * as util from '../../util/util-index.js';
import { authUser } from '../auth.js';
import { routeDefinition } from '../router.js';

const CheckpointRequestPayload = t.object({
  client_id: t.string,
  checkpoint_request_id: codecs.bigint // bigint here might be overkill, but we use it for other checkpoint inputs
});

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
      const bucketStorage = (await service_context.storageEngine.activeBucketStorage.getActiveSyncConfig())?.storage;
      const cp = await bucketStorage?.getCheckpoint();
      if (cp == null) {
        throw new Error('No sync config available');
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
    const { token_payload, service_context } = payload.context;

    const { replicationHead, writeCheckpoint } = await util.createWriteCheckpoint({
      userId: token_payload!.userIdString,
      clientId: payload.params.client_id,
      batcher: service_context.storageEngine.writeCheckpointBatcher
    });

    logger.info(
      `Write checkpoint for ${token_payload!.userIdString}/${payload.params.client_id}: ${writeCheckpoint} | ${replicationHead}`
    );

    return {
      write_checkpoint: String(writeCheckpoint)
    };
  }
});

export const checkpointRequest = routeDefinition({
  path: '/sync/checkpoint-request',
  method: router.HTTPMethod.POST,
  authorize: authUser,
  validator: schema.createTsCodecValidator(CheckpointRequestPayload, { allowAdditional: true }),
  handler: async (request) => {
    const { token_payload, service_context } = request.context;
    const { params } = request;

    const decodedParams = CheckpointRequestPayload.decode(params);

    // Duplicate checkpoint requests still go through createReplicationHead, so the source may receive a marker/keepalive.
    // Storage preserves the original heads for same-id retries; this keeps the retry path simple for now.
    const { replicationHead, writeCheckpoint } = await util.createWriteCheckpoint({
      userId: token_payload!.userIdString,
      clientId: decodedParams.client_id,
      batcher: service_context.storageEngine.writeCheckpointBatcher,
      checkpointRequestId: decodedParams.checkpoint_request_id
    });

    logger.info(
      `Requested checkpoint for ${token_payload!.userIdString}/${params.client_id}: ${writeCheckpoint} | ${replicationHead}`
    );

    // TODO, we don't actually need to return this here, what should we return :?
    return {
      write_checkpoint: String(writeCheckpoint)
    };
  }
});

export const CHECKPOINT_ROUTES = [checkpointRequest, writeCheckpoint, writeCheckpoint2];
