import { ExpressionType } from './ExpressionType.js';
import { CompatibilityContext, CompatibilityEdition, CompatibilityOption } from './compatibility.js';
import { generateSqlFunctions } from './sql_functions.js';
import { CompiledClause, ParameterValueClause, ParameterValueSet, SqliteValue } from './types.js';

export interface SqlParameterFunction {
  readonly debugName: string;
  call: (parameters: ParameterValueSet, ...args: SqliteValue[]) => SqliteValue;
  getReturnType(): ExpressionType;
  parameterCount: number;
  /**
   * Whether this function returns data derived from usage parameters.
   *
   * This can be:
   *
   *   1. `subscription`, for unauthenticated subscription parameters like `subscription.parameters()`.
   *   2. `authenticated`, for parameters authenticated by a trusted backend (like `request.user_id()`).
   *   3. `unauthenticated`, for global unauthenticated request parameters like (like `request.parameters()`).
   */
  parameterUsage: 'subscription' | 'authenticated' | 'unauthenticated' | null;
  detail: string;
  documentation: string;
}

const jsonExtractFromRecord = generateSqlFunctions(
  new CompatibilityContext({ edition: CompatibilityEdition.SYNC_STREAMS })
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
  parameterUsage: SqlParameterFunction['parameterUsage'];
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
    parameterUsage: options.parameterUsage
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
    parameterUsage: options.parameterUsage
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
    parameterUsage: 'unauthenticated'
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
  parameterUsage: 'authenticated'
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
    parameterUsage: 'authenticated'
  };
}

const REQUEST_FUNCTIONS_NAMED = {
  ...globalRequestParameterFunctions('request'),
  jwt: request_jwt,
  user_id: generateUserIdFunction('request.user_id', 'request.jwt()')
};

export const REQUEST_FUNCTIONS: Record<string, SqlParameterFunction> = REQUEST_FUNCTIONS_NAMED;

/**
 * A {@link ParameterValueClause} derived from a call to a {@link SqlParameterFunction}.
 */
export interface RequestFunctionCall extends ParameterValueClause {
  function: SqlParameterFunction;
}

export function isRequestFunctionCall(clause: CompiledClause): clause is RequestFunctionCall {
  return (clause as RequestFunctionCall).function != null;
}
