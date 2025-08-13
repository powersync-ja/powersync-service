import {
  globalRequestParameterFunctions,
  parameterFunctions,
  request_user_id,
  SqlParameterFunction
} from '../request_functions.js';
import { ParameterValueSet } from '../types.js';

export const STREAM_FUNCTIONS: Record<string, Record<string, SqlParameterFunction>> = {
  subscription: {
    ...parameterFunctions({
      schema: 'subscription',
      extractJsonString: function (v: ParameterValueSet): string {
        return v.rawStreamParameters ?? '{}';
      },
      extractJsonParsed: function (v: ParameterValueSet) {
        return v.streamParameters ?? {};
      },
      sourceDescription: 'Unauthenticated subscription parameters as JSON',
      sourceDocumentation:
        'parameters passed by the client for this stream as a JSON string. These parameters are not authenticated - any value can be passed in by the client.',
      usesAuthenticatedRequestParameters: false,
      usesUnauthenticatedRequestParameters: true
    })
  },
  connection: {
    ...globalRequestParameterFunctions('connection')
  },
  auth: {
    user_id: request_user_id,
    ...parameterFunctions({
      schema: 'auth',
      extractJsonString: function (v: ParameterValueSet): string {
        return v.rawTokenPayload;
      },
      extractJsonParsed: function (v: ParameterValueSet) {
        return v.tokenParameters;
      },
      sourceDescription: 'JWT payload as JSON',
      sourceDocumentation: 'JWT payload as a JSON string. This is always validated against trusted keys',
      usesAuthenticatedRequestParameters: true,
      usesUnauthenticatedRequestParameters: false
    })
  }
};
