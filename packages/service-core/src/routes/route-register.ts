import type fastify from 'fastify';
import * as zlib from 'node:zlib';
import * as uuid from 'uuid';

import { errors, HTTPMethod, logger, RouteNotFound, router } from '@powersync/lib-services-framework';
import { FastifyReply } from 'fastify';
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

/** Registers a custom error handler that emits service-error JSON honouring any negotiated `Content-Encoding`. */
export function registerFastifyErrorHandler(app: fastify.FastifyInstance) {
  app.setErrorHandler<Error>(async (error, _request, reply) => {
    if (reply.raw.headersSent) {
      // Headers already flushed - reply.code/header would throw ERR_HTTP_HEADERS_SENT.
      reply.raw.destroy(error);
      return;
    }

    const { status, data } = fastifyErrorToResponse(error);
    if (status >= 500) {
      logger.error('Request failed', error);
    } else {
      logger.warn(`Request failed: ${error.message}`, {
        status,
        code: data.error.code
      });
    }

    const json = JSON.stringify(data);

    // Fastify's default handler leaves `content-encoding` intact when it rewrites the body.
    const encoding = reply.getHeader('content-encoding');
    let body: string | Buffer = json;
    if (encoding === 'gzip') {
      body = zlib.gzipSync(json);
    } else if (encoding === 'zstd') {
      body = zlib.zstdCompressSync(json);
    } else {
      reply.removeHeader('content-encoding');
    }

    reply
      .code(status)
      .header('content-type', 'application/json; charset=utf-8')
      .header('content-length', Buffer.byteLength(body))
      .send(body);
  });
}

type ErrorResponseData = { error: errors.ErrorData };

function serviceErrorToResponse(serviceError: errors.ServiceError): router.RouterResponse<ErrorResponseData> {
  return new router.RouterResponse({
    status: serviceError.errorData.status || 500,
    headers: {
      'Content-Type': 'application/json'
    },
    data: {
      error: serviceError.errorData
    }
  });
}

function fastifyErrorToResponse(error: any): router.RouterResponse<ErrorResponseData> {
  // Preserve 4xx status from Fastify built-ins (validation, invalid JSON body, etc.) instead of collapsing to 500.
  if (
    typeof error?.statusCode === 'number' &&
    error.statusCode >= 400 &&
    error.statusCode < 500 &&
    typeof error.code === 'string'
  ) {
    return new router.RouterResponse({
      status: error.statusCode,
      headers: {
        'Content-Type': 'application/json'
      },
      data: {
        error: {
          name: error.name,
          code: error.code,
          status: error.statusCode,
          description: error.message
        }
      }
    });
  }
  return serviceErrorToResponse(errors.asServiceError(error));
}

async function respond(reply: FastifyReply, response: router.RouterResponse) {
  Object.keys(response.headers).forEach((key) => {
    reply.header(key, response.headers[key]);
  });
  reply.status(response.status);
  await reply.send(response.data);
}
