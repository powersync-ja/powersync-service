import { JSONBig } from '@powersync/service-jsonbig';
import {
  DEFAULT_HYDRATION_STATE,
  SqlSyncRules,
  nodeSqlite,
  withBucketSource,
  type HydrationState,
  type SourceTableRef,
  type SqliteRow
} from '@powersync/service-sync-rules';
import * as sqlite from 'node:sqlite';
import { RustSourceEvaluator } from '../src/index.js';

export const table: SourceTableRef = { connectionTag: 'default', schema: 'public', name: 'docs' };

export function compile(queries: string[], nativeSqlite = true) {
  const streams = queries
    .map(
      (query, i) => `  stream${i}:\n    accept_potentially_dangerous_queries: true\n    query: ${JSON.stringify(query)}`
    )
    .join('\n');
  const parsed = SqlSyncRules.fromYaml(
    `config:\n  edition: 3\n  unstable_sqlite_expression_engine: ${nativeSqlite}\nstreams:\n${streams}\n`,
    { defaultSchema: 'public', throwOnError: true }
  );
  return parsed.config;
}

export function prepare(
  queries: string[],
  sourceTable = table,
  hydrationState: HydrationState = DEFAULT_HYDRATION_STATE
) {
  const config = compile(queries);
  const reference = config.hydrate({ hydrationState, sqlite: nodeSqlite(sqlite) });
  const rust = new RustSourceEvaluator(config, sourceTable, hydrationState);
  return {
    config,
    reference,
    rust,
    expected(rows: SqliteRow[]) {
      return rows.map((record) => ({
        data: (() => {
          const { results, errors } = reference.evaluateRowWithErrors({ sourceTable, record });
          return {
            results: results.map((row) => withBucketSource({ ...row, data: JSONBig.stringify(row.data) }, row.source)),
            errors
          };
        })(),
        parameters: reference.evaluateParameterRowWithErrors(sourceTable, record)
      }));
    }
  };
}
