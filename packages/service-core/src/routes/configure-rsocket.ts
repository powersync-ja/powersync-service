import { deserialize } from 'bson';
import * as http from 'http';

import { errors, logger } from '@powersync/lib-services-framework';
import { ReactiveSocketRouter, RSocketRequestMeta } from '@powersync/service-rsocket-router';

import { ServiceContext } from '../system/ServiceContext.js';
import { generateContext, getTokenFromHeader } from './auth.js';
import { syncStreamReactive } from './endpoints/socket-route.js';
import { RSocketContextMeta, SocketRouteGenerator } from './router-socket.js';
import { Context, RouterServiceContext } from './router.js';

export type RSockerRouterConfig = {
  service_context: ServiceContext;
  server: http.Server;
  route_generators?: SocketRouteGenerator[];
};

export const DEFAULT_SOCKET_ROUTES = [syncStreamReactive];

export function configureRSocket(router: ReactiveSocketRouter<Context>, options: RSockerRouterConfig) {
  const { route_generators = DEFAULT_SOCKET_ROUTES, server, service_context } = options;

  router.applyWebSocketEndpoints(server, {
    contextProvider: async (data: Buffer) => {
      const { token, user_agent } = RSocketContextMeta.decode(deserialize(data) as any);

      if (!token) {
        throw new errors.AuthorizationError('No token provided');
      }

      try {
        const extracted_token = getTokenFromHeader(token);
        if (extracted_token != null) {
          const { context, errors: token_errors } = await generateContext(options.service_context, extracted_token);
          if (context?.token_payload == null) {
            throw new errors.AuthorizationError(token_errors ?? 'Authentication required');
          }

          if (!service_context.routerEngine) {
            throw new Error(`RouterEngine has not been registered`);
          }

          return {
            token,
            user_agent,
            ...context,
            token_errors: token_errors,
            service_context: service_context as RouterServiceContext
          };
        } else {
          throw new errors.AuthorizationError('No token provided');
        }
      } catch (ex) {
        logger.error(ex);
        throw ex;
      }
    },
    endpoints: route_generators.map((generator) => generator(router)),
    metaDecoder: async (meta: Buffer) => {
      return RSocketRequestMeta.decode(deserialize(meta) as any);
    },
    payloadDecoder: async (rawData?: Buffer) => rawData && deserialize(rawData)
  });
}
