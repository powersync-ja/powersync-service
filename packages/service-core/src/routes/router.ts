import * as micro from '@journeyapps-platform/micro';

import * as auth from '../auth/auth-index.js';
import { CorePowerSyncSystem } from '../system/CorePowerSyncSystem.js';

export type Context = {
  user_id?: string;
  system: CorePowerSyncSystem;

  token_payload?: auth.JwtPayload;
  token_errors?: string[];
};

/**
 * Creates a route handler given a router instance
 * TODO move away from Fastify specific types
 */
export type RouteGenerator = (router: micro.fastify.FastifyRouter<Context>) => micro.router.Route;
