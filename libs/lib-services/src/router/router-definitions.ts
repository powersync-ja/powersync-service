import { ServiceError } from '@powersync/service-errors';
import { MicroValidator } from '../schema/definitions.js';

/**
 * Subset of HTTP methods used in route definitions
 */
export enum HTTPMethod {
  'DELETE' = 'DELETE',
  'GET' = 'GET',
  'HEAD' = 'HEAD',
  'PATCH' = 'PATCH',
  'POST' = 'POST',
  'PUT' = 'PUT',
  'OPTIONS' = 'OPTIONS'
}

/**
 * Response for authorization checks in route definitions
 */
export type AuthorizationResponse =
  | {
      authorized: true;
    }
  | {
      authorized: false;
      error?: ServiceError | undefined;
    };

/**
 * Payload which is passed to route endpoint handler functions
 */
export type EndpointHandlerPayload<P, C> = {
  params: P;
  context: C;
};

/**
 * A route endpoint handler function
 */
export type EndpointHandler<P, O> = (payload: P) => O | Promise<O>;

/**
 * The definition of a route endpoint
 */
export type Endpoint<I, O, C, P = EndpointHandlerPayload<I, C>, H = EndpointHandler<P, O>> = {
  path: string;
  method: HTTPMethod;
  tags?: EndpointHandler<P, Record<string, string>>;
  authorize?: EndpointHandler<P, AuthorizationResponse>;
  validator?: MicroValidator<I>;
  handler: H;
};
