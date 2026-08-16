import {
  PostgresBucketStorageFactory,
  test_utils as postgresTestUtils
} from '@powersync/service-module-postgres-storage';
import {
  PostgresStorageBenchmarkImplementationOptions,
  StorageBenchmarkImplementation,
  StorageBenchmarkRunResource
} from '../../types/StorageBenchmark.js';

export class PostgresStorageBenchmarkImplementation implements StorageBenchmarkImplementation {
  readonly id = 'postgres-storage' as const;

  constructor(private readonly options: PostgresStorageBenchmarkImplementationOptions) {}

  async open(signal: AbortSignal): Promise<StorageBenchmarkRunResource> {
    signal.throwIfAborted();
    const setup = postgresTestUtils.postgresTestSetup({ url: this.options.url });
    const factory = await setup.factory();

    try {
      signal.throwIfAborted();
      const serverVersion = await readServerVersion(factory);
      const systemIdentifier = await factory.getSystemIdentifier();
      return {
        factory,
        tableIdStrings: setup.tableIdStrings,
        environment: {
          implementation: this.id,
          server_version: serverVersion,
          system_identifier: systemIdentifier
        },
        async dispose() {
          await factory[Symbol.asyncDispose]();
        }
      };
    } catch (error) {
      try {
        await factory[Symbol.asyncDispose]();
      } catch (cleanupError) {
        throw new AggregateError([error, cleanupError], 'PostgreSQL benchmark setup and cleanup failed');
      }
      throw error;
    }
  }
}

async function readServerVersion(factory: PostgresBucketStorageFactory): Promise<number> {
  const result = await factory.db.query('SHOW server_version_num');
  const row = result.rows[0];
  if (row == null) {
    throw new Error('PostgreSQL did not return server_version_num');
  }
  return Number(row.decodeWithoutCustomTypes(0));
}
