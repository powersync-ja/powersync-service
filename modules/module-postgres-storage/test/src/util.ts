import path from 'path';
import { fileURLToPath } from 'url';
import { normalizePostgresStorageConfig } from '../../src//types/types.js';
import { PostgresMigrationAgent } from '../../src/migrations/PostgresMigrationAgent.js';
import { PostgresTestStorageFactoryGenerator } from '../../src/storage/PostgresTestStorageFactoryGenerator.js';
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

/**
 * Vitest tries to load the migrations via .ts files which fails.
 * For tests this links to the relevant .js files correctly
 */
class TestPostgresMigrationAgent extends PostgresMigrationAgent {
  getInternalScriptsDir(): string {
    return path.resolve(__dirname, '../../dist/migrations/scripts');
  }
}

export const POSTGRES_STORAGE_FACTORY = PostgresTestStorageFactoryGenerator({
  url: env.PG_STORAGE_TEST_URL,
  migrationAgent: (config) => new TestPostgresMigrationAgent(config)
});
