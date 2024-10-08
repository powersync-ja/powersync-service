import { deserialize } from 'bson';
import * as http from 'http';

import { errors, logger } from '@powersync/lib-services-framework';
import { ReactiveSocketRouter, RequestData, RSocketRequestMeta } from '@powersync/service-rsocket-router';

import { CorePowerSyncSystem } from '../system/CorePowerSyncSystem.js';
import { generateContext, getTokenFromHeader } from './auth.js';
import { syncStreamReactive } from './endpoints/socket-route.js';
import { RSocketContextMeta, SocketRouteGenerator } from './router-socket.js';
import { Context } from './router.js';

export type RSockerRouterConfig = {
  system: CorePowerSyncSystem;
  server: http.Server;
  routeGenerators?: SocketRouteGenerator[];
};

export const DEFAULT_SOCKET_ROUTES = [syncStreamReactive];

export function configureRSocket(router: ReactiveSocketRouter<Context>, options: RSockerRouterConfig) {
  const { routeGenerators = DEFAULT_SOCKET_ROUTES, server, system } = options;

  router.applyWebSocketEndpoints(server, {
    contextProvider: async (data: Buffer, request: RequestData) => {
      const { token, user_agent } = RSocketContextMeta.decode(deserialize(data) as any);

      // Concatenate client and browser user agents
      const combinedUserAgent = [user_agent, request.userAgent].filter((r) => r != null).join(' ');

      if (!token) {
        throw new errors.AuthorizationError('No token provided');
      }

      try {
        const extracted_token = getTokenFromHeader(token);
        if (extracted_token != null) {
          const { context, errors: token_errors } = await generateContext(system, extracted_token);
          if (context?.token_payload == null) {
            throw new errors.AuthorizationError(token_errors ?? 'Authentication required');
          }
          return {
            token,
            user_agent: combinedUserAgent,
            ...context,
            token_errors: token_errors,
            system
          };
        } else {
          throw new errors.AuthorizationError('No token provided');
        }
      } catch (ex) {
        logger.error(ex);
        throw ex;
      }
    },
    endpoints: routeGenerators.map((generator) => generator(router)),
    metaDecoder: async (meta: Buffer) => {
      return RSocketRequestMeta.decode(deserialize(meta) as any);
    },
    payloadDecoder: async (rawData?: Buffer) => rawData && deserialize(rawData)
  });
}
