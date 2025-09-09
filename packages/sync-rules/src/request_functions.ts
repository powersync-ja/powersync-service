import { ExpressionType } from './ExpressionType.js';
import { CompatibilityContext, CompatibilityEdition, CompatibilityOption } from './compatibility.js';
import { generateSqlFunctions } from './sql_functions.js';
import { ParameterValueSet, SqliteValue } from './types.js';

export interface SqlParameterFunction {
  readonly debugName: string;
  call: (parameters: ParameterValueSet, ...args: SqliteValue[]) => SqliteValue;
  getReturnType(): ExpressionType;
  parameterCount: number;
  /** request.user_id(), request.jwt(), token_parameters.* */
  usesAuthenticatedRequestParameters: boolean;
  /** request.parameters(), user_parameters.* */
  usesUnauthenticatedRequestParameters: boolean;
  detail: string;
  documentation: string;
}

const jsonExtractFromRecord = generateSqlFunctions(
  new CompatibilityContext(CompatibilityEdition.SYNC_STREAMS)
).jsonExtractFromRecord;
/**
 * Defines a `parameters` function and a `parameter` function.
 *
 * The `parameters` function extracts a JSON object from the {@link ParameterValueSet} while the `parameter` function
 * takes a second argument (a JSON path or a single key) to extract.
 */
export function parameterFunctions(options: {
  schema: string;
  extractJsonString: (v: ParameterValueSet) => string;
  extractJsonParsed: (v: ParameterValueSet) => any;
  sourceDescription: string;
  sourceDocumentation: string;
  usesAuthenticatedRequestParameters: boolean;
  usesUnauthenticatedRequestParameters: boolean;
}) {
  const allParameters: SqlParameterFunction = {
    debugName: `${options.schema}.parameters`,
    parameterCount: 0,
    call(parameters: ParameterValueSet) {
      return options.extractJsonString(parameters);
    },
    getReturnType() {
      return ExpressionType.TEXT;
    },
    detail: options.sourceDescription,
    documentation: `Returns ${options.sourceDocumentation}`,
    usesAuthenticatedRequestParameters: options.usesAuthenticatedRequestParameters,
    usesUnauthenticatedRequestParameters: options.usesUnauthenticatedRequestParameters
  };

  const extractParameter: SqlParameterFunction = {
    debugName: `${options.schema}.parameter`,
    parameterCount: 1,
    call(parameters: ParameterValueSet, path) {
      const parsed = options.extractJsonParsed(parameters);
      // jsonExtractFromRecord uses the correct behavior of only splitting the path if it starts with $.
      // This particular JSON extract function always had that behavior, so we don't need to take backwards
      // compatibility into account.
      if (typeof path == 'string') {
        return jsonExtractFromRecord(parsed, path, '->>');
      }

      return null;
    },
    getReturnType() {
      return ExpressionType.ANY;
    },
    detail: `Extract value from ${options.sourceDescription}`,
    documentation: `Returns an extracted value (via the key as the second argument) from ${options.sourceDocumentation}`,
    usesAuthenticatedRequestParameters: options.usesAuthenticatedRequestParameters,
    usesUnauthenticatedRequestParameters: options.usesUnauthenticatedRequestParameters
  };

  return { parameters: allParameters, parameter: extractParameter };
}

export function globalRequestParameterFunctions(schema: string) {
  return parameterFunctions({
    schema,
    extractJsonString: function (v: ParameterValueSet): string {
      return v.rawUserParameters;
    },
    extractJsonParsed: function (v: ParameterValueSet) {
      return v.userParameters;
    },
    sourceDescription: 'Unauthenticated request parameters as JSON',
    sourceDocumentation:
      'parameters passed by the client as a JSON string. These parameters are not authenticated - any value can be passed in by the client.',
    usesAuthenticatedRequestParameters: false,
    usesUnauthenticatedRequestParameters: true
  });
}

export const request_jwt: SqlParameterFunction = {
  debugName: 'request.jwt',
  parameterCount: 0,
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

export function generateUserIdFunction(debugName: string, sameAsDesc: string): SqlParameterFunction {
  return {
    debugName,
    parameterCount: 0,
    call(parameters: ParameterValueSet) {
      return parameters.userId;
    },
    getReturnType() {
      return ExpressionType.TEXT;
    },
    detail: 'Authenticated user id',
    documentation: `The id of the authenticated user.\nSame as \`${sameAsDesc} ->> 'sub'\`.`,
    usesAuthenticatedRequestParameters: true,
    usesUnauthenticatedRequestParameters: false
  };
}

const REQUEST_FUNCTIONS_NAMED = {
  ...globalRequestParameterFunctions('request'),
  jwt: request_jwt,
  user_id: generateUserIdFunction('request.user_id', 'request.jwt()')
};

export const REQUEST_FUNCTIONS: Record<string, SqlParameterFunction> = REQUEST_FUNCTIONS_NAMED;
