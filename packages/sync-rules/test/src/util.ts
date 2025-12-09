import {
  CompatibilityContext,
  BucketIdTransformer,
  DEFAULT_TAG,
  GetQuerierOptions,
  RequestedStream,
  RequestJwtPayload,
  RequestParameters,
  SourceTableInterface,
  StaticSchema
} from '../../src/index.js';

export class TestSourceTable implements SourceTableInterface {
  readonly connectionTag = DEFAULT_TAG;
  readonly schema = 'test_schema';

  constructor(public readonly name: string) {}
}

export const PARSE_OPTIONS = {
  defaultSchema: 'test_schema',
  compatibility: CompatibilityContext.FULL_BACKWARDS_COMPATIBILITY
};

export const ASSETS = new TestSourceTable('assets');
export const USERS = new TestSourceTable('users');

export const BASIC_SCHEMA = new StaticSchema([
  {
    tag: DEFAULT_TAG,
    schemas: [
      {
        name: 'test_schema',
        tables: [
          {
            name: 'assets',
            columns: [
              { name: 'id', pg_type: 'uuid' },
              { name: 'name', pg_type: 'text' },
              { name: 'count', pg_type: 'int4' },
              { name: 'owner_id', pg_type: 'uuid' }
            ]
          },
          {
            name: 'other',
            columns: [{ name: 'other_id', pg_type: 'uuid' }]
          }
        ]
      }
    ]
  }
]);

/**
 * For backwards-compatiblity in tests only.
 */
export function normalizeTokenParameters(
  token_parameters: Record<string, any>,
  user_parameters?: Record<string, any>
): RequestParameters {
  const tokenPayload = {
    sub: token_parameters.user_id ?? '',
    parameters: { ...token_parameters }
  } satisfies RequestJwtPayload;
  delete tokenPayload.parameters.user_id;
  return new RequestParameters(tokenPayload, user_parameters ?? {});
}

export function normalizeQuerierOptions(
  token_parameters: Record<string, any>,
  user_parameters?: Record<string, any>,
  streams?: Record<string, RequestedStream[]>
): GetQuerierOptions {
  const globalParameters = normalizeTokenParameters(token_parameters, user_parameters);
  return {
    globalParameters,
    hasDefaultStreams: true,
    streams: streams ?? {}
  };
}

export function identityBucketTransformer(id: string) {
  return id;
}
