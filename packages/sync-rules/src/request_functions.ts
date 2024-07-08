import { ExpressionType } from './ExpressionType.js';
import { RequestParameters, SqliteValue } from './types.js';

export interface SqlParameterFunction {
  readonly debugName: string;
  call: (parameters: RequestParameters) => SqliteValue;
  getReturnType(): ExpressionType;
}

const request_parameters: SqlParameterFunction = {
  debugName: 'request.parameters',
  call(parameters: RequestParameters) {
    return parameters.raw_user_parameters;
  },
  getReturnType() {
    return ExpressionType.TEXT;
  }
};

const request_jwt: SqlParameterFunction = {
  debugName: 'request.jwt',
  call(parameters: RequestParameters) {
    return parameters.raw_token_payload;
  },
  getReturnType() {
    return ExpressionType.TEXT;
  }
};

const request_user_id: SqlParameterFunction = {
  debugName: 'request.user_id',
  call(parameters: RequestParameters) {
    return parameters.user_id;
  },
  getReturnType() {
    return ExpressionType.TEXT;
  }
};

export const REQUEST_FUNCTIONS_NAMED = {
  parameters: request_parameters,
  jwt: request_jwt,
  user_id: request_user_id
};

export const REQUEST_FUNCTIONS: Record<string, SqlParameterFunction> = REQUEST_FUNCTIONS_NAMED;
