import {
  generateUserIdFunction,
  globalRequestParameterFunctions,
  parameterFunctions,
  SqlParameterFunction
} from '../../request_functions.js';
import { ParameterValueSet } from '../../types.js';

/**
 * @deprecated Alpha Sync Streams implementation for `config.edition` 1 and 2, kept for backwards compatibility until the next major version. See `src/legacy/README.md`.
 */
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
      parameterUsage: 'subscription'
    })
  },
  connection: {
    ...globalRequestParameterFunctions('connection')
  },
  auth: {
    user_id: generateUserIdFunction('auth.user_id', 'auth.parameters('),
    ...parameterFunctions({
      schema: 'auth',
      extractJsonString: function (v: ParameterValueSet): string {
        return v.rawTokenPayload;
      },
      extractJsonParsed: function (v: ParameterValueSet) {
        return v.parsedTokenPayload;
      },
      sourceDescription: 'JWT payload as JSON',
      sourceDocumentation: 'JWT payload as a JSON string. This is always validated against trusted keys',
      parameterUsage: 'authenticated'
    })
  }
};
