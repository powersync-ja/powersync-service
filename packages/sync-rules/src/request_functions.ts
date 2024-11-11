import { ExpressionType } from './ExpressionType.js';
import { RequestParameters, SqliteValue, ParameterValueSet } from './types.js';

export interface SqlParameterFunction {
  readonly debugName: string;
  call: (parameters: ParameterValueSet) => SqliteValue;
  getReturnType(): ExpressionType;
  /** request.user_id(), request.jwt(), token_parameters.* */
  usesAuthenticatedRequestParameters: boolean;
  /** request.parameters(), user_parameters.* */
  usesUnauthenticatedRequestParameters: boolean;
  detail: string;
  documentation: string;
}

const request_parameters: SqlParameterFunction = {
  debugName: 'request.parameters',
  call(parameters: ParameterValueSet) {
    return parameters.raw_user_parameters;
  },
  getReturnType() {
    return ExpressionType.TEXT;
  },
  detail: 'Unauthenticated request parameters as JSON',
  documentation:
    'Returns parameters passed by the client as a JSON string. These parameters are not authenticated - any value can be passed in by the client.',
  usesAuthenticatedRequestParameters: false,
  usesUnauthenticatedRequestParameters: true
};

const request_jwt: SqlParameterFunction = {
  debugName: 'request.jwt',
  call(parameters: ParameterValueSet) {
    return parameters.raw_token_payload;
  },
  getReturnType() {
    return ExpressionType.TEXT;
  },
  detail: 'JWT payload as JSON',
  documentation: 'The JWT payload as a JSON string. This is always validated against trusted keys.',
  usesAuthenticatedRequestParameters: true,
  usesUnauthenticatedRequestParameters: false
};

const request_user_id: SqlParameterFunction = {
  debugName: 'request.user_id',
  call(parameters: ParameterValueSet) {
    return parameters.user_id;
  },
  getReturnType() {
    return ExpressionType.TEXT;
  },
  detail: 'Authenticated user id',
  documentation: "The id of the authenticated user.\nSame as `request.jwt() ->> 'sub'`.",
  usesAuthenticatedRequestParameters: true,
  usesUnauthenticatedRequestParameters: false
};

export const REQUEST_FUNCTIONS_NAMED = {
  parameters: request_parameters,
  jwt: request_jwt,
  user_id: request_user_id
};

export const REQUEST_FUNCTIONS: Record<string, SqlParameterFunction> = REQUEST_FUNCTIONS_NAMED;
