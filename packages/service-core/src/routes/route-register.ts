import type fastify from 'fastify';
import * as uuid from 'uuid';

import {
  ErrorCode,
  errors,
  HTTPMethod,
  logger,
  RouteNotFound,
  router,
  ServiceError
} from '@powersync/lib-services-framework';
import { Context, ContextProvider, RequestEndpoint, RequestEndpointHandlerPayload } from './router.js';
import { FastifyReply } from 'fastify';

export type FastifyEndpoint<I, O, C> = RequestEndpoint<I, O, C> & {
  parse?: boolean;
  plugins?: fastify.FastifyPluginAsync[];
};

/**
 * Registers endpoint definitions as routes on a Fastify app instance.
 */
export function registerFastifyRoutes(
  app: fastify.FastifyInstance,
  contextProvider: ContextProvider,
  endpoints: FastifyEndpoint<any, any, Context>[]
) {
  for (const e of endpoints) {
    // Create a new context for each route
    app.register(async function (fastify) {
      fastify.route({
        url: e.path,
        method: e.method as HTTPMethod,
        handler: async (request, reply) => {
          const startTime = new Date();
          let response: router.RouterResponse;
          const requestLogger = logger.child({
            route: e.path,
            rid: `h/${uuid.v7()}`
          });
          try {
            const context = await contextProvider(request, { logger: requestLogger });
            let combined = {
              ...(request.params as any),
              ...(request.query as any)
            };

            if (typeof request.body === 'object' && !Buffer.isBuffer(request.body) && !Array.isArray(request.body)) {
              combined = {
                ...combined,
                ...request.body
              };
            }

            const payload: RequestEndpointHandlerPayload = {
              context: context,
              params: combined,
              request
            };

            const endpointResponse = await router.executeEndpoint(e, payload);

            if (router.RouterResponse.isRouterResponse(endpointResponse)) {
              response = endpointResponse;
            } else if (router.isAsyncIterable(endpointResponse) || Buffer.isBuffer(endpointResponse)) {
              response = new router.RouterResponse({
                status: 200,
                data: endpointResponse
              });
            } else {
              response = new router.RouterResponse({
                status: 200,
                data: { data: endpointResponse }
              });
            }
          } catch (ex) {
            const serviceError = errors.asServiceError(ex);
            requestLogger.error(`Request failed`, serviceError);

            response = serviceErrorToResponse(serviceError);
          }

          try {
            await respond(reply, response);
          } finally {
            await response.afterSend?.({ clientClosed: request.socket.closed });
            requestLogger.info(`${e.method} ${request.url}`, {
              duration_ms: Math.round(new Date().valueOf() - startTime.valueOf() + Number.EPSILON),
              status: response.status,
              method: e.method,
              path: request.url
            });
          }
        }
      });

      if (!(e.parse ?? true)) {
        fastify.removeAllContentTypeParsers();
      }

      e.plugins?.forEach((plugin) => fastify.register(plugin));
    });
  }
}

/**
 * Registers a custom not-found handler to ensure 404 error responses have the same schema as other service errors.
 */
export function registerFastifyNotFoundHandler(app: fastify.FastifyInstance) {
  app.setNotFoundHandler(async (request, reply) => {
    await respond(reply, serviceErrorToResponse(new RouteNotFound(request.originalUrl, request.method)));
  });
}

function serviceErrorToResponse(error: ServiceError): router.RouterResponse {
  return new router.RouterResponse({
    status: error.errorData.status || 500,
    headers: {
      'Content-Type': 'application/json'
    },
    data: {
      error: error.errorData
    }
  });
}

async function respond(reply: FastifyReply, response: router.RouterResponse) {
  Object.keys(response.headers).forEach((key) => {
    reply.header(key, response.headers[key]);
  });
  reply.status(response.status);
  await reply.send(response.data);
}
