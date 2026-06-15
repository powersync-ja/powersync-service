import * as sqlite from 'node:sqlite';

import { expect } from 'vitest';
import {
  BaseJwtPayload,
  BucketDataScope,
  BucketDataSource,
  BucketParameterQuerier,
  BucketSource,
  ColumnDefinition,
  CompatibilityContext,
  CompatibilityEdition,
  CreateSourceParams,
  DEFAULT_HYDRATION_STATE,
  DEFAULT_TAG,
  GetQuerierOptions,
  HydratedBucketSource,
  HydrationInput,
  HydrationState,
  mergeDataSources,
  mergeParameterIndexLookupCreators,
  nodeSqlite,
  ParameterIndexLookupCreator,
  ParameterLookupDefinitionId,
  ParameterLookupScope,
  RequestedStream,
  RequestParameters,
  ScopedEvaluateParameterRow,
  ScopedEvaluateRow,
  ScopedParameterLookup,
  SourceSchema,
  SourceTableRef,
  StaticSchema,
  TablePattern
} from '../../src/index.js';
import { createScalarExpressionEngine } from '../../src/sync_plan/engine/factory.js';

export class TestSourceTable implements SourceTableRef {
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
  createEvaluator() {
    throw new Error('Function not implemented.');
  },
  tableSyncsData: function (table: SourceTableRef): boolean {
    throw new Error('Function not implemented.');
  },
  resolveResultSets: function (schema: SourceSchema, tables: Record<string, Record<string, ColumnDefinition>>): void {
    throw new Error('Function not implemented.');
  },
  debugWriteOutputTables: function (result: Record<string, { query: string }[]>): void {
    throw new Error('Function not implemented.');
  }
};

export const EMPTY_PARAMETER_LOOKUP_SOURCE: ParameterIndexLookupCreator = {
  get sourceId(): ParameterLookupDefinitionId {
    return {
      lookupName: 'lookup',
      queryId: '0'
    };
  },
  getSourceTables(): Set<TablePattern> {
    return new Set();
  },
  createEvaluator() {
    return {
      evaluateParameterRow() {
        return [];
      }
    };
  },
  tableSyncsParameters() {
    return false;
  }
};

export function bucketDataScope(bucketPrefix: string, source: BucketDataSource = EMPTY_DATA_SOURCE): BucketDataScope {
  return { bucketPrefix, source };
}

export function lookupScope(
  lookupName: string,
  queryId: string,
  source: ParameterIndexLookupCreator = EMPTY_PARAMETER_LOOKUP_SOURCE
): ParameterLookupScope {
  return { lookupName, queryId, source };
}

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

export interface DebugMergedSource extends HydratedBucketSource {
  evaluateRow: ScopedEvaluateRow;
  evaluateParameterRow: ScopedEvaluateParameterRow;
}

/**
 * For production purposes, we typically need to operate on the different sources separately. However, for debugging,
 * it is useful to have a single merged source that can evaluate everything.
 */
export function debugHydratedMergedSource(bucketSource: BucketSource, params?: CreateSourceParams): DebugMergedSource {
  const hydrationState = params?.hydrationState ?? DEFAULT_HYDRATION_STATE;
  const input = testHydrationInput({ hydrationState });
  const dataSource = mergeDataSources(input, bucketSource.dataSources);
  const parameterLookupSource = mergeParameterIndexLookupCreators(input, bucketSource.parameterIndexLookupCreators);
  const hydratedBucketSource = bucketSource.hydrate(input);
  return {
    definition: bucketSource,
    evaluateParameterRow: parameterLookupSource.evaluateParameterRow.bind(parameterLookupSource),
    evaluateRow: dataSource.evaluateRow.bind(dataSource),
    pushBucketParameterQueriers: hydratedBucketSource.pushBucketParameterQueriers.bind(hydratedBucketSource)
  };
}

export function testHydrationInput(
  options: {
    compatibility?: CompatibilityContext;
    hydrationState?: HydrationState;
  } = {}
): HydrationInput {
  const {
    compatibility = new CompatibilityContext({ edition: CompatibilityEdition.COMPILED_STREAMS }),
    hydrationState = DEFAULT_HYDRATION_STATE
  } = options;

  return {
    scalarExpressions: createScalarExpressionEngine(compatibility, nodeSqlite(sqlite)),
    hydrationState
  };
}
