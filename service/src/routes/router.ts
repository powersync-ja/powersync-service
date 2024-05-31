import * as micro from '@journeyapps-platform/micro';
import { ReactiveSocketRouter } from '@powersync/service-rsocket-router';
import { routes } from '@powersync/service-core';

export const Router = new micro.fastify.FastifyRouter<routes.Context>({
  concurrency: {
    max_queue_depth: 20,
    // TODO: Test max concurrency.
    max_concurrent_requests: 10
  },
  tags: () => {
    return {};
  },
  cors: false // CORS is manually registered
});

// Separate router so that we have a separate queue for streaming requests.
export const StreamingRouter = new micro.fastify.FastifyRouter<routes.Context>({
  concurrency: {
    // Don't queue more requests, since it could wait a very long time.
    // KLUDGE: the queue doesn't support a max depth of 0 currently.
    max_queue_depth: 1,
    max_concurrent_requests: 200
  },
  tags: () => {
    return {};
  },
  cors: false // CORS is manually registered
});

export const SocketRouter = new ReactiveSocketRouter<routes.Context>();

export default Router;
