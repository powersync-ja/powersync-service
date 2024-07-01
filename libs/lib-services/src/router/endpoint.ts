import * as errors from '../errors/errors-index.js';
import { Endpoint, EndpointHandlerPayload } from './router-definitions.js';

/**
 * Executes an endpoint's definition in the correct lifecycle order:
 *  Validations are checked. A {@link ValidationError} is thrown if validations fail.
 *  Authorization is checked. A {@link AuthorizationError} is thrown if checks fail.
 */
export const executeEndpoint = async <I, O, C, P extends EndpointHandlerPayload<I, C>>(
  endpoint: Endpoint<I, O, C, P>,
  payload: P
) => {
  const validation_response = await endpoint.validator?.validate(payload.params);
  if (validation_response && !validation_response.valid) {
    throw new errors.ValidationError(validation_response.errors);
  }
  const authorizer_response = await endpoint.authorize?.(payload);
  if (authorizer_response && !authorizer_response.authorized) {
    throw new errors.AuthorizationError(authorizer_response.errors);
  }

  return endpoint.handler(payload);
};
