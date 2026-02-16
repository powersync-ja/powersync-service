import { test } from 'vitest';
import {
  BucketParameterQuerier,
  CompatibilityContext,
  CompatibilityEdition,
  deserializeSyncPlan,
  GetQuerierOptions,
  HydratedSyncRules,
  javaScriptExpressionEngine,
  PrecompiledSyncConfig,
  RequestParameters,
  serializeSyncPlan,
  ScopedParameterLookup,
  SourceTableInterface,
  SqliteRow,
  SqlSyncRules,
  SyncConfig,
  versionedHydrationState,
  RustSyncPlanEvaluator
} from '../../../../src/index.js';
import { ScalarExpressionEngine } from '../../../../src/sync_plan/engine/scalar_expression_engine.js';

export type SyncRuntime = 'javascript' | 'rust';

interface SyncTest {
  runtime: SyncRuntime;
  engine: ScalarExpressionEngine | null;
  prepareWithoutHydration(yaml: string): SyncConfig;
  prepareSyncStreams(yaml: string): HydratedSyncRules;
}

const runtimes: SyncRuntime[] = ['javascript', 'rust'];

export function syncTest(name: string, fn: (context: { sync: SyncTest }) => unknown | Promise<unknown>) {
  for (const runtime of runtimes) {
    test(`${name} [${runtime}]`, async () => {
      const sync = createSyncTest(runtime);
      try {
        await fn({ sync });
      } finally {
        sync.engine?.close();
      }
    });
  }
}

function createSyncTest(runtime: SyncRuntime): SyncTest {
  const compatibility = new CompatibilityContext({ edition: CompatibilityEdition.SYNC_STREAMS });

  if (runtime == 'javascript') {
    const engine = javaScriptExpressionEngine(compatibility);
    return {
      runtime,
      engine,
      prepareWithoutHydration(inputs) {
        const plan = compileJsPlan(inputs);
        return new PrecompiledSyncConfig(plan, { engine, sourceText: '', defaultSchema: 'test_schema' });
      },
      prepareSyncStreams(inputs) {
        return this.prepareWithoutHydration(inputs).hydrate({ hydrationState: versionedHydrationState(1) });
      }
    };
  }

  const engine = javaScriptExpressionEngine(compatibility);
  return {
    runtime,
    engine,
    prepareWithoutHydration(inputs) {
      const serialized = serializeSyncPlan(compileJsPlan(inputs));
      const plan = deserializeSyncPlan(serialized);
      return new PrecompiledSyncConfig(plan, { engine, sourceText: '', defaultSchema: 'test_schema' });
    },
    prepareSyncStreams(inputs) {
      const serialized = serializeSyncPlan(compileJsPlan(inputs));
      return new RustHydratedSyncRulesFacade(serialized) as unknown as HydratedSyncRules;
    }
  };
}

class RustHydratedSyncRulesFacade {
  readonly #plan: any;
  readonly #evaluator: RustSyncPlanEvaluator;

  constructor(plan: unknown) {
    this.#plan = plan as any;
    this.#evaluator = new RustSyncPlanEvaluator(plan, { defaultSchema: 'test_schema' });
  }

  evaluateRow(options: { sourceTable: SourceTableInterface; record: SqliteRow }) {
    return this.#evaluator.evaluateRow({
      sourceTable: toRustSourceTable(options.sourceTable),
      record: options.record
    }) as any[];
  }

  tableSyncsData(table: SourceTableInterface) {
    return this.#plan.dataSources.some((source: any) => tableMatchesPattern(source.table, table));
  }

  tableSyncsParameters(table: SourceTableInterface) {
    return this.#plan.parameterIndexes.some((source: any) => tableMatchesPattern(source.table, table));
  }

  evaluateParameterRow(sourceTable: SourceTableInterface, row: SqliteRow) {
    const rows = this.#evaluator.evaluateParameterRow(toRustSourceTable(sourceTable), row) as any[];
    return rows.map((entry) => ({
      ...entry,
      lookup: rustLookupToScopedLookup(entry.lookup)
    }));
  }

  getBucketParameterQuerier(options: GetQuerierOptions): { querier: BucketParameterQuerier; errors: any[] } {
    try {
      const prepared = this.#evaluator.prepareBucketQueries({
        globalParameters: toRustRequestParameters(options.globalParameters),
        hasDefaultStreams: options.hasDefaultStreams,
        streams: options.streams
      }) as any;
      const staticBuckets = prepared.staticBuckets as any[];
      const staticCount = staticBuckets.length;
      const evaluator = this.#evaluator;
      const plan = this.#plan;

      const querier: BucketParameterQuerier = {
        staticBuckets,
        hasDynamicBuckets: prepared.dynamicQueries.length != 0,
        async queryDynamicBucketDescriptions(source) {
          if (prepared.dynamicQueries.length == 0) {
            return [];
          }

          const lookupResults: any[] = [];
          for (const query of prepared.dynamicQueries) {
            lookupResults.push(
              ...(await resolveDynamicLookupResults({
                query,
                plan,
                source
              }))
            );
          }

          const allResolved = evaluator.resolveBucketQueries(prepared, lookupResults) as any[];
          return allResolved.slice(staticCount);
        }
      };

      return { querier, errors: [] };
    } catch (error) {
      return {
        querier: {
          staticBuckets: [],
          hasDynamicBuckets: false,
          async queryDynamicBucketDescriptions() {
            return [];
          }
        },
        errors: [{ descriptor: 'rust', message: (error as Error).message }]
      };
    }
  }
}

function toRustSourceTable(table: SourceTableInterface) {
  return {
    connectionTag: table.connectionTag,
    schema: table.schema,
    name: table.name
  };
}

function toRustRequestParameters(parameters: RequestParameters) {
  return {
    auth: parameters.parsedTokenPayload,
    connection: parameters.userParameters,
    subscription: parameters.streamParameters ?? {}
  };
}

function rustLookupToScopedLookup(lookup: { values: unknown[] }) {
  return ScopedParameterLookup.direct(
    {
      lookupName: String(lookup.values[0]),
      queryId: String(lookup.values[1])
    },
    lookup.values.slice(2) as any[]
  );
}

function tableMatchesPattern(pattern: any, table: SourceTableInterface) {
  if (pattern.connection != null && pattern.connection != table.connectionTag) {
    return false;
  }

  if ((pattern.schema ?? 'test_schema') != table.schema) {
    return false;
  }

  return wildcardMatch(pattern.table, table.name);
}

function wildcardMatch(pattern: string, value: string) {
  const escaped = pattern.replace(/[.*+?^${}()|[\]\\]/g, '\\$&').replace(/%/g, '.*');
  return new RegExp(`^${escaped}$`).test(value);
}

async function resolveDynamicLookupResults({
  query,
  plan,
  source
}: {
  query: any;
  plan: any;
  source: { getParameterSets: (lookups: ScopedParameterLookup[]) => Promise<Record<string, unknown>[]> };
}) {
  const results: any[] = [];
  const stagedRows = new Map<string, unknown[][]>();
  const requestValues = new Map<string, any[]>();

  for (const request of query.lookupRequests as any[]) {
    requestValues.set(stageKey(request.stageId, request.idInStage), request.values);
  }

  for (let stageId = 0; stageId < query.lookupStages.length; stageId++) {
    const stage = query.lookupStages[stageId] as any[];
    for (let idInStage = 0; idInStage < stage.length; idInStage++) {
      const lookup = stage[idInStage];
      const key = stageKey(stageId, idInStage);

      let values = requestValues.get(key);
      if (values == null) {
        if (lookup.type != 'parameter') {
          throw new Error('Rust test facade only supports parameter lookup stages.');
        }

        const scope = plan.parameterIndexes[lookup.lookup].lookupScope;
        const instantiation = instantiateLookupValues(lookup.instantiation, stagedRows);
        values = instantiation.map((entry) => {
          const scoped = [scope.lookupName, scope.queryId, ...entry];
          return {
            values: scoped,
            serializedRepresentation: JSON.stringify(scoped)
          };
        });
      }

      const lookups = values.map(rustLookupToScopedLookup);
      const rows = await source.getParameterSets(lookups);
      stagedRows.set(key, rows.map(indexedRowToArray));

      for (const rawLookup of values) {
        results.push({ lookup: rawLookup, rows });
      }
    }
  }

  return results;
}

function instantiateLookupValues(values: any[], stagedRows: Map<string, unknown[][]>) {
  const allInputs = values.map((value) => resolveParameterCandidates(value, stagedRows));
  return cartesianProduct(allInputs);
}

function resolveParameterCandidates(value: any, stagedRows: Map<string, unknown[][]>): unknown[] {
  if (value.type == 'lookup') {
    const rows = stagedRows.get(stageKey(value.lookup.stageId, value.lookup.idInStage)) ?? [];
    const result = rows.map((row) => row[value.resultIndex]).filter((entry) => entry !== undefined && entry !== null);
    return result;
  }

  if (value.type == 'intersection') {
    const intersections = value.values.map((nested: any) => resolveParameterCandidates(nested, stagedRows));
    return intersection(intersections);
  }

  // Request references should be handled in prepare_bucket_queries, not in dynamic resolve.
  return [];
}

function intersection(values: unknown[][]) {
  if (values.length == 0) {
    return [];
  }

  const [first, ...rest] = values;
  const byKey = new Map(first.map((value) => [JSON.stringify(value), value]));
  for (const entries of rest) {
    const allowed = new Set(entries.map((value) => JSON.stringify(value)));
    for (const key of [...byKey.keys()]) {
      if (!allowed.has(key)) {
        byKey.delete(key);
      }
    }
  }
  return [...byKey.values()];
}

function cartesianProduct(groups: unknown[][]) {
  if (groups.some((group) => group.length == 0)) {
    return [];
  }

  return groups.reduce<unknown[][]>((acc, group) => {
    if (acc.length == 0) {
      return group.map((value) => [value]);
    }

    const next: unknown[][] = [];
    for (const prefix of acc) {
      for (const suffix of group) {
        next.push([...prefix, suffix]);
      }
    }
    return next;
  }, []);
}

function indexedRowToArray(row: Record<string, unknown>) {
  return Object.entries(row)
    .sort((a, b) => Number(a[0]) - Number(b[0]))
    .map(([, value]) => value);
}

function stageKey(stageId: number, idInStage: number) {
  return `${stageId}:${idInStage}`;
}

function compileJsPlan(source: string) {
  const { config, errors } = SqlSyncRules.fromYaml(source, {
    throwOnError: false,
    defaultSchema: 'test_schema',
    allowNewSyncCompiler: true
  });

  if (errors.length != 0) {
    throw new Error(errors.map((e) => e.message).join('; '));
  }

  return (config as PrecompiledSyncConfig).plan;
}
