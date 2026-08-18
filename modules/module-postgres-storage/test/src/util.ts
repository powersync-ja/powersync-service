import type { DatabaseClient } from '@powersync/lib-service-postgres';
import { LEGACY_STORAGE_VERSION } from '@powersync/service-core';
import type { Statement } from '@powersync/service-jpgwire';
import path from 'path';
import { fileURLToPath } from 'url';
import { normalizePostgresStorageConfig, PostgresMigrationAgent } from '../../src/index.js';
import { postgresTestSetup } from '../../src/utils/test-utils.js';
import { env } from './env.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

export const TEST_URI = env.PG_STORAGE_TEST_URL;

const BASE_CONFIG = {
  type: 'postgresql' as const,
  uri: TEST_URI,
  sslmode: 'disable' as const
};

export const TEST_CONNECTION_OPTIONS = normalizePostgresStorageConfig(BASE_CONFIG);

let explainedBucketDataQuery = false;
let bucketDataQueryCount = 0;

async function explainBucketDataQuery(db: DatabaseClient, query: Statement) {
  bucketDataQueryCount++;
  const explainQuery = Number(process.env.POWERSYNC_STORAGE_BENCHMARK_EXPLAIN_QUERY ?? 1);
  if (
    process.env.POWERSYNC_STORAGE_BENCHMARK_EXPLAIN !== 'true' ||
    explainedBucketDataQuery ||
    bucketDataQueryCount != explainQuery
  ) {
    return;
  }
  explainedBucketDataQuery = true;

  const rows = await db.queryRows<Record<string, string>>({
    statement: `EXPLAIN (ANALYZE, BUFFERS, SETTINGS, FORMAT TEXT) ${query.statement}`,
    params: query.params
  });
  const plan = rows.map((row) => Object.values(row)[0]).join('\n');
  console.log(`\nPostgres bucket data query plan\n${plan}\n`);
}

/**
 * Vitest tries to load the migrations via .ts files which fails.
 * For tests this links to the relevant .js files correctly
 */
class TestPostgresMigrationAgent extends PostgresMigrationAgent {
  getInternalScriptsDir(): string {
    return path.resolve(__dirname, '../../dist/migrations/scripts');
  }
}

export const POSTGRES_STORAGE_SETUP = postgresTestSetup({
  url: env.PG_STORAGE_TEST_URL,
  migrationAgent: (config) => new TestPostgresMigrationAgent(config),
  bucketDataQueryHook: explainBucketDataQuery
});

export const POSTGRES_STORAGE_FACTORY = POSTGRES_STORAGE_SETUP;
export const POSTGRES_REPORT_STORAGE_FACTORY = POSTGRES_STORAGE_SETUP.reportFactory;

export const TEST_STORAGE_VERSIONS = [LEGACY_STORAGE_VERSION];
