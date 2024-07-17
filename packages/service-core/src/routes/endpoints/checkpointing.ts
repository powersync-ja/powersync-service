import * as t from 'ts-codec';
import { router, schema } from '@powersync/lib-services-framework';

import * as util from '../../util/util-index.js';
import { authUser } from '../auth.js';
import { routeDefinition } from '../router.js';

const WriteCheckpointRequest = t.object({});

export const writeCheckpoint = routeDefinition({
  path: '/write-checkpoint.json',
  method: router.HTTPMethod.GET,
  authorize: authUser,
  validator: schema.createTsCodecValidator(WriteCheckpointRequest, { allowAdditional: true }),
  handler: async (payload) => {
    const system = payload.context.system;
    const storage = system.storage;

    const checkpoint = await util.getClientCheckpoint(system.requirePgPool(), storage);
    return {
      checkpoint
    };
  }
});

export const writeCheckpoint2 = routeDefinition({
  path: '/write-checkpoint2.json',
  method: router.HTTPMethod.GET,
  authorize: authUser,
  validator: schema.createTsCodecValidator(WriteCheckpointRequest, { allowAdditional: true }),
  handler: async (payload) => {
    const { user_id, system } = payload.context;
    const storage = system.storage;
    const write_checkpoint = await util.createWriteCheckpoint(system.requirePgPool(), storage, user_id!);
    return {
      write_checkpoint: String(write_checkpoint)
    };
  }
});

export const CHECKPOINT_ROUTES = [writeCheckpoint, writeCheckpoint2];
