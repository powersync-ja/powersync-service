import { ExpressionType } from '../ExpressionType.js';
import { request_jwt, request_user_id, SqlParameterFunction } from '../request_functions.js';
import { ParameterValueSet } from '../types.js';

const subscription_parameters: SqlParameterFunction = {
  debugName: 'subscription.parameters',
  call(parameters: ParameterValueSet) {
    return parameters.rawStreamParameters;
  },
  getReturnType() {
    return ExpressionType.TEXT;
  },
  detail: 'Unauthenticated subscription parameters as JSON',
  documentation:
    'Returns parameters passed by the client for this stream as a JSON string. These parameters are not authenticated - any value can be passed in by the client.',
  usesAuthenticatedRequestParameters: false,
  usesUnauthenticatedRequestParameters: true
};

const connection_parameters: SqlParameterFunction = {
  debugName: 'connection.parameters',
  call(parameters: ParameterValueSet) {
    return parameters.rawUserParameters;
  },
  getReturnType() {
    return ExpressionType.TEXT;
  },
  detail: 'Unauthenticated connection parameters as JSON',
  documentation:
    'Returns parameters passed by the client as a JSON string. These parameters are not authenticated - any value can be passed in by the client.',
  usesAuthenticatedRequestParameters: false,
  usesUnauthenticatedRequestParameters: true
};

export const STREAM_FUNCTIONS: Record<string, Record<string, SqlParameterFunction>> = {
  subscription: {
    parameters: subscription_parameters
  },
  connection: {
    parameters: connection_parameters
  },
  token: {
    user_id: request_user_id,
    jwt: request_jwt
  }
};
