import { ExpressionType } from './ExpressionType.js';
import { ParameterValueSet, SqliteValue } from './types.js';

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

const parametersFunction = (name: string): SqlParameterFunction => {
  return {
    debugName: name,
    call(parameters: ParameterValueSet) {
      return parameters.rawUserParameters;
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
};

const request_jwt: SqlParameterFunction = {
  debugName: 'request.jwt',
  call(parameters: ParameterValueSet) {
    return parameters.rawTokenPayload;
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
    return parameters.userId;
  },
  getReturnType() {
    return ExpressionType.TEXT;
  },
  detail: 'Authenticated user id',
  documentation: "The id of the authenticated user.\nSame as `request.jwt() ->> 'sub'`.",
  usesAuthenticatedRequestParameters: true,
  usesUnauthenticatedRequestParameters: false
};

export const REQUEST_FUNCTIONS_WITHOUT_PARAMETERS: Record<string, SqlParameterFunction> = {
  jwt: request_jwt,
  user_id: request_user_id
};

export const REQUEST_FUNCTIONS: Record<string, SqlParameterFunction> = {
  parameters: parametersFunction('request.parameters'),
  ...REQUEST_FUNCTIONS_WITHOUT_PARAMETERS
};

export const QUERY_FUNCTIONS: Record<string, SqlParameterFunction> = {
  params: parametersFunction('query.params')
};
