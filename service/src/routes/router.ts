import { ReactiveSocketRouter } from '@powersync/service-rsocket-router';
import { routes } from '@powersync/service-core';

export const SocketRouter = new ReactiveSocketRouter<routes.Context>({
  max_concurrent_connections: 200
});
