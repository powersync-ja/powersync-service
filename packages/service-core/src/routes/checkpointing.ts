import * as t from 'ts-codec';
import * as micro from '@journeyapps-platform/micro';

import * as util from '../util/util-index.js';

import { authUser } from './auth.js';
import { RouteGenerator } from './router.js';

const WriteCheckpointRequest = t.object({});

export const writeCheckpoint: RouteGenerator = (router) =>
  router.get('/write-checkpoint.json', {
    authorize: authUser,
    validator: micro.schema.createTsCodecValidator(WriteCheckpointRequest, { allowAdditional: true }),
    handler: async (payload) => {
      const system = payload.context.system;
      const storage = system.storage;

      const checkpoint = await util.getClientCheckpoint(system.requirePgPool(), storage);
      return {
        checkpoint
      };
    }
  });

export const writeCheckpoint2: RouteGenerator = (router) =>
  router.get('/write-checkpoint2.json', {
    authorize: authUser,
    validator: micro.schema.createTsCodecValidator(WriteCheckpointRequest, { allowAdditional: true }),
    handler: async (payload) => {
      const { user_id, system } = payload.context;
      const storage = system.storage;
      const write_checkpoint = await util.createWriteCheckpoint(system.requirePgPool(), storage, user_id!);
      return {
        write_checkpoint: String(write_checkpoint)
      };
    }
  });
