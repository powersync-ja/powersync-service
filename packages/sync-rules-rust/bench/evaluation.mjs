import { JSONBig } from '@powersync/service-jsonbig';
import { DEFAULT_HYDRATION_STATE, nodeSqlite, SqlSyncRules, withBucketSource } from '@powersync/service-sync-rules';
import * as sqlite from 'node:sqlite';
import { bufferToSqlite, getDateRenderMode } from '../../../modules/module-mongodb/dist/replication/bufferToSqlite.js';
export function compile(queries, useSqlite) {
  return SqlSyncRules.fromYaml(
    `config:\n  edition: 3\n  unstable_sqlite_expression_engine: ${useSqlite}\nstreams:\n` +
      queries
        .map(
          (query, i) =>
            `  s${i}:\n    accept_potentially_dangerous_queries: true\n    query: ${JSON.stringify(query)}\n`
        )
        .join(''),
    { defaultSchema: 'public', throwOnError: true }
  ).config;
}
export function jsImplementation(config, table, wire = false) {
  const evaluator = config.hydrate({ hydrationState: DEFAULT_HYDRATION_STATE, sqlite: nodeSqlite(sqlite) });
  const mode = getDateRenderMode(config.compatibility);
  return (buffers) =>
    buffers.map((buffer) => {
      const record = bufferToSqlite(buffer, mode);
      const { results, errors } = evaluator.evaluateRowWithErrors({ sourceTable: table, record });
      const parameters = evaluator.evaluateParameterRowWithErrors(table, record);
      return {
        data: {
          results: results.map((row) =>
            wire
              ? { ...row, data: JSONBig.stringify(row.data), source: config.bucketDataSources.indexOf(row.source) }
              : withBucketSource({ ...row, data: JSONBig.stringify(row.data) }, row.source)
          ),
          errors
        },
        parameters: wire
          ? {
              ...parameters,
              results: parameters.results.map(({ lookup, bucketParameters }) => ({
                source: config.bucketParameterLookupSources.indexOf(lookup.source),
                values: lookup.values,
                bucketParameters
              }))
            }
          : parameters
      };
    });
}
