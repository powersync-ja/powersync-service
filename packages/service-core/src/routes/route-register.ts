import fastify from 'fastify';

import { container, errors, router, HTTPMethod } from '@powersync/service-framework';
import { Context, ContextProvider, RequestEndpoint, RequestEndpointHandlerPayload } from './router.js';

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
          try {
            const context = await contextProvider(request);

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
            const journeyError = errors.JourneyError.isJourneyError(ex) ? ex : new errors.InternalServerError(ex);

            response = new router.RouterResponse({
              status: journeyError.errorData.status || 500,
              headers: {
                'Content-Type': 'application/json'
              },
              data: {
                error: journeyError.errorData
              }
            });
          }

          Object.keys(response.headers).forEach((key) => {
            reply.header(key, response.headers[key]);
          });
          reply.status(response.status);
          try {
            await reply.send(response.data);
          } finally {
            await response.afterSend?.();
            container.logger.info(`${e.method} ${request.url}`, {
              duration_ms: Math.round(new Date().valueOf() - startTime.valueOf() + Number.EPSILON),
              status: response.status,
              method: e.method,
              path: request.url,
              route: e.path
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
