import { expect } from 'vitest';
import {
  BaseJwtPayload,
  BucketDataSource,
  BucketParameterQuerier,
  ColumnDefinition,
  CompatibilityContext,
  CreateSourceParams,
  DEFAULT_TAG,
  GetQuerierOptions,
  RequestedStream,
  RequestJwtPayload,
  RequestParameters,
  ScopedParameterLookup,
  SourceSchema,
  SourceTableInterface,
  StaticSchema,
  TablePattern
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
 * Shortcut to create RequestParameters from a JWT payload and optional request parameters.
 */
export function requestParameters(
  jwtPayload: Record<string, any>,
  clientParameters?: Record<string, any>
): RequestParameters {
  return new RequestParameters(new BaseJwtPayload(jwtPayload), clientParameters ?? {});
}

export function normalizeQuerierOptions(
  jwtPayload: Record<string, any>,
  clientParameters?: Record<string, any>,
  streams?: Record<string, RequestedStream[]>
): GetQuerierOptions {
  const globalParameters = requestParameters(jwtPayload, clientParameters);
  return {
    globalParameters,
    hasDefaultStreams: true,
    streams: streams ?? {}
  };
}

export function identityBucketTransformer(id: string) {
  return id;
}

/**
 * Empty data source that can be used for testing parameter queries, where most of the functionality here is not used.
 */
export const EMPTY_DATA_SOURCE: BucketDataSource = {
  uniqueName: 'mybucket',
  bucketParameters: [],
  // These are not used in the tests.
  getSourceTables: function (): Set<TablePattern> {
    return new Set();
  },
  evaluateRow(options) {
    throw new Error('Function not implemented.');
  },
  tableSyncsData: function (table: SourceTableInterface): boolean {
    throw new Error('Function not implemented.');
  },
  resolveResultSets: function (schema: SourceSchema, tables: Record<string, Record<string, ColumnDefinition>>): void {
    throw new Error('Function not implemented.');
  },
  debugWriteOutputTables: function (result: Record<string, { query: string }[]>): void {
    throw new Error('Function not implemented.');
  }
};

export async function findQuerierLookups(querier: BucketParameterQuerier): Promise<ScopedParameterLookup[]> {
  expect(querier.hasDynamicBuckets).toBe(true);

  let recordedLookups: ScopedParameterLookup[] | null = null;
  await querier.queryDynamicBucketDescriptions({
    async getParameterSets(lookups: ScopedParameterLookup[]) {
      recordedLookups = lookups;
      return [];
    }
  });

  return recordedLookups!;
}
