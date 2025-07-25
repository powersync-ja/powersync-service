import { deserialize } from 'bson';
import * as http from 'http';
import * as uuid from 'uuid';

import { ErrorCode, errors, logger } from '@powersync/lib-services-framework';
import { ReactiveSocketRouter, RSocketRequestMeta, TypedBuffer } from '@powersync/service-rsocket-router';

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
    contextProvider: async (data: TypedBuffer): Promise<Context & { token: string }> => {
      const connectionLogger = logger.child({
        // timestamp-based uuid - useful for requests
        rid: `s/${uuid.v7()}`
      });
      const { token, user_agent } = RSocketContextMeta.decode(decodeTyped(data) as any);

      if (!token) {
        throw new errors.AuthorizationError(ErrorCode.PSYNC_S2106, 'No token provided');
      }

      try {
        const extracted_token = getTokenFromHeader(token);
        if (extracted_token != null) {
          const { context, tokenError } = await generateContext(options.service_context, extracted_token);
          if (tokenError != null) {
            throw tokenError;
          } else if (context?.token_payload == null) {
            throw new errors.AuthorizationError(ErrorCode.PSYNC_S2106, 'Authentication required');
          }

          return {
            token,
            user_agent,
            ...context,
            service_context: service_context as RouterServiceContext,
            logger: connectionLogger
          };
        } else {
          // Token field is present, but did not contain a token.
          throw new errors.AuthorizationError(ErrorCode.PSYNC_S2106, 'No valid token provided');
        }
      } catch (ex) {
        connectionLogger.error(ex);
        throw ex;
      }
    },
    endpoints: route_generators.map((generator) => generator(router)),
    metaDecoder: async (meta: TypedBuffer) => {
      return RSocketRequestMeta.decode(decodeTyped(meta) as any);
    },
    payloadDecoder: async (rawData?: TypedBuffer) => rawData && decodeTyped(rawData)
  });
}

function decodeTyped(data: TypedBuffer) {
  switch (data.mimeType) {
    case 'application/json':
      const decoder = new TextDecoder();
      return JSON.parse(decoder.decode(data.contents));
    case 'application/bson':
      return deserialize(data.contents);
  }

  throw new errors.UnsupportedMediaType(`Expected JSON or BSON request, got ${data.mimeType}`);
}
