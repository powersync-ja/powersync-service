import { router } from '@powersync/lib-services-framework';
import type { JwtPayload } from '../auth/auth-index.js';
import type { CorePowerSyncSystem } from '../system/CorePowerSyncSystem.js';

/**
 * Common context for routes
 */
export type Context = {
  user_id?: string;
  system: CorePowerSyncSystem;

  token_payload?: JwtPayload;
  token_errors?: string[];
  /**
   * Only on websocket endpoints.
   */
  user_agent?: string;
};

export type BasicRouterRequest = {
  headers: Record<string, string | string[] | undefined>;
  protocol: string;
  hostname: string;
};

export type ContextProvider = (request: BasicRouterRequest) => Promise<Context>;

export type RequestEndpoint<
  I,
  O,
  C = Context,
  Payload = RequestEndpointHandlerPayload<I, C, BasicRouterRequest>
> = router.Endpoint<I, O, C, Payload> & {};

export type RequestEndpointHandlerPayload<
  I = any,
  C = Context,
  Request = BasicRouterRequest
> = router.EndpointHandlerPayload<I, C> & {
  request: Request;
};

export type RouteDefinition<I = any, O = any> = RequestEndpoint<I, O>;

/**
 * Helper function for making generics work well when defining routes
 */
export function routeDefinition<I, O, C = Context, Extension = {}>(
  params: RequestEndpoint<I, O, C> & Extension
): RequestEndpoint<I, O, C> & Extension {
  return params;
}
