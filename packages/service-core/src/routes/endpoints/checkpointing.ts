import { codecs, logger, router, schema } from '@powersync/lib-services-framework';
import * as t from 'ts-codec';

import * as util from '../../util/util-index.js';
import { authUser } from '../auth.js';
import { routeDefinition } from '../router.js';

const CHECKPOINT_REQUEST_ID_MAX = 9_223_372_036_854_775_807n;

function isValidCheckpointRequestId(value: bigint) {
  return value > 0n && value <= CHECKPOINT_REQUEST_ID_MAX;
}

export const CheckpointRequestPayload = t.object({
  client_id: t.string,
  // Positive int64, matching the managed write checkpoint id. Clients should
  // send values larger than Number.MAX_SAFE_INTEGER as strings, since JSON
  // numbers are only validated up to the safe-integer range.
  checkpoint_request_id: codecs.bigint
});

const CheckpointRequestPayloadShapeValidator = schema.createTsCodecValidator(CheckpointRequestPayload, {
  allowAdditional: true
});

const CheckpointRequestPayloadValidator = {
  validate(params: t.Encoded<typeof CheckpointRequestPayload>) {
    const shapeValidation = CheckpointRequestPayloadShapeValidator.validate(params);
    if (!shapeValidation.valid) {
      return shapeValidation;
    }

    const decodedParams = CheckpointRequestPayload.decode(params);
    if (!isValidCheckpointRequestId(decodedParams.checkpoint_request_id)) {
      return {
        valid: false as const,
        errors: [`Expected checkpoint_request_id between 1 and ${CHECKPOINT_REQUEST_ID_MAX}`]
      };
    }

    return {
      valid: true as const
    };
  }
};

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

    const head = await apiHandler.createReplicationHead(async (head) => ({ response: head, shouldAdvance: true }));

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
  validator: CheckpointRequestPayloadValidator,
  handler: async (request) => {
    const { token_payload, service_context } = request.context;
    const { params } = request;

    const decodedParams = CheckpointRequestPayload.decode(params);

    // Storage only applies supplied request ids that advance the stored managed checkpoint.
    // Stale or duplicate ids return the stored checkpoint without forcing a source marker.
    const { replicationHead, writeCheckpoint } = await util.createWriteCheckpoint({
      userId: token_payload!.userIdString,
      clientId: decodedParams.client_id,
      batcher: service_context.storageEngine.writeCheckpointBatcher,
      checkpointRequestId: decodedParams.checkpoint_request_id
    });

    logger.info(
      `Requested checkpoint for ${token_payload!.userIdString}/${decodedParams.client_id}: ${writeCheckpoint} | ${replicationHead}`
    );

    // Return the checkpoint value storage is actually at after this request.
    // When an earlier request has already advanced the stored value beyond the
    // supplied checkpoint_request_id, this returns that larger previous value so
    // the client can treat the response as a stale-request acknowledgement.
    return {
      write_checkpoint: String(writeCheckpoint)
    };
  }
});

export const CHECKPOINT_ROUTES = [checkpointRequest, writeCheckpoint, writeCheckpoint2];
