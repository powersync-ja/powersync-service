import type fastify from 'fastify';
import a from 'async';
import { logger } from '@powersync/lib-services-framework';

export type CreateRequestQueueParams = {
  max_queue_depth: number;
  concurrency: number;
};

/**
 * Creates a request queue which limits the amount of concurrent connections which
 * are active at any time.
 */
export const createRequestQueueHook = (params: CreateRequestQueueParams): fastify.onRequestHookHandler => {
  const request_queue = a.queue<() => Promise<void>>((event, done) => {
    event().finally(done);
  }, params.concurrency);

  return (request, reply, next) => {
    if (
      (params.max_queue_depth == 0 && request_queue.running() == params.concurrency) ||
      (params.max_queue_depth > 0 && request_queue.length() >= params.max_queue_depth)
    ) {
      logger.warn(`${request.method} ${request.url}`, {
        status: 429,
        method: request.method,
        path: request.url,
        route: request.routerPath,
        queue_overflow: true
      });
      return reply.status(429).send();
    }

    const finished = new Promise<void>((resolve) => {
      reply.then(
        () => resolve(),
        () => resolve()
      );
    });

    request_queue.push(() => {
      next();
      return finished;
    });
  };
};
