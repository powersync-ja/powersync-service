import {
  CompatibilityOption,
  DEFAULT_HYDRATION_STATE,
  PrecompiledSyncConfig,
  resolveRowMetadata,
  scalarStatementToSql,
  TableProcessorToSqlHelper,
  type HydrationState,
  type SourceTableRef,
  type SyncConfig,
  type TableProcessor
} from '@powersync/service-sync-rules';

export interface Processor {
  kind: 'data' | 'parameters';
  source: number;
  sql: string;
  inputs: ({ column: string } | { constant: string })[];
  outputs: ('star' | { index: number; alias: string })[];
  outputCount: number;
  parameterCount: number;
  table: string;
  bucketPrefix: string;
}

/** Compile once per physical table. Parsing and SQL generation remain in the existing compiler. */
export function compileSourcePlan(
  config: SyncConfig,
  table: SourceTableRef,
  hydrationState: HydrationState = DEFAULT_HYDRATION_STATE
) {
  if (!(config instanceof PrecompiledSyncConfig)) {
    throw new Error('The Rust evaluator requires compiled sync streams (edition 3).');
  }
  if (!config.compatibility.isEnabled(CompatibilityOption.sqliteExpressionEngine)) {
    throw new Error('The Rust evaluator requires unstable_sqlite_expression_engine.');
  }
  const processors: Processor[] = [];
  const defaultSchema = config.defaultSchema;
  const bucketSources = config.bucketDataSources;
  const parameterSources = config.bucketParameterLookupSources;
  const parameterScopes = parameterSources.map((source) => hydrationState.getParameterIndexLookupScope(source));
  const seenBuckets = new Set<string>();
  const seenLookups = new Set<string>();

  function prepare(source: TableProcessor) {
    const pattern = source.sourceTable.toTablePattern(defaultSchema);
    if (!pattern.matches(table)) return null;
    const helper = new TableProcessorToSqlHelper(source);
    return {
      helper,
      finish(): Processor['inputs'] {
        return helper.mapper.instantiation.map((input) =>
          'column' in input ? { column: input.column } : { constant: resolveRowMetadata(input, pattern, table) }
        );
      }
    };
  }

  config.plan.buckets.forEach((bucket, index) => {
    const scope = hydrationState.getBucketSourceScope(bucketSources[index]);
    if (seenBuckets.has(scope.bucketPrefix)) return;
    seenBuckets.add(scope.bucketPrefix);
    for (const source of bucket.sources) {
      const prepared = prepare(source);
      if (!prepared) continue;
      const { helper } = prepared;
      const expressions = [];
      const outputs: Processor['outputs'] = [];
      for (const column of source.columns) {
        if (column === 'star') outputs.push('star');
        else {
          outputs.push({ index: expressions.length, alias: column.alias });
          expressions.push(helper.mapper.transform(column.expr));
        }
      }
      const outputCount = expressions.length;
      expressions.push(...source.parameters.map((p) => helper.mapper.transform(p.expr)));
      processors.push({
        kind: 'data',
        source: index,
        sql: scalarStatementToSql({
          outputs: expressions,
          filters: helper.filterExpressions,
          tableValuedFunctions: helper.tableValuedFunctions
        }),
        inputs: prepared.finish(),
        outputs,
        outputCount,
        parameterCount: source.parameters.length,
        table: source.outputTableName ?? table.name,
        bucketPrefix: scope.bucketPrefix
      });
    }
  });

  config.plan.parameterIndexes.forEach((source, index) => {
    const scope = parameterScopes[index];
    const key = JSON.stringify([scope.lookupName, scope.queryId]);
    if (seenLookups.has(key)) return;
    seenLookups.add(key);
    const prepared = prepare(source);
    if (!prepared) return;
    const { helper } = prepared;
    const expressions = source.outputs.map((o) => helper.mapper.transform(o));
    const outputCount = expressions.length;
    expressions.push(...source.parameters.map((p) => helper.mapper.transform(p.expr)));
    processors.push({
      kind: 'parameters',
      source: index,
      sql: scalarStatementToSql({
        outputs: expressions,
        filters: helper.filterExpressions,
        tableValuedFunctions: helper.tableValuedFunctions
      }),
      inputs: prepared.finish(),
      outputs: [],
      outputCount,
      parameterCount: source.parameters.length,
      table: '',
      bucketPrefix: ''
    });
  });
  return { processors, bucketSources, parameterScopes };
}
