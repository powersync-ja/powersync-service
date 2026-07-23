import { DrizzleBucketStorageFactory } from '../../storage/DrizzleBucketStorageFactory.js';
import type { NormalizedDrizzleSqliteStorageConfig } from '../../types/types.js';
import { createSqliteDrizzleRuntime, type SqliteDrizzleRuntime } from './sqlite-config.js';
import { createSqliteDrizzleStorageDialect } from './sqlite-dialect.js';

export function createSqliteDrizzleStorageFactory(options: {
  config: NormalizedDrizzleSqliteStorageConfig;
  slotNamePrefix: string;
  runtime?: SqliteDrizzleRuntime;
}): DrizzleBucketStorageFactory {
  const runtime = options.runtime ?? createSqliteDrizzleRuntime(options.config);
  return new DrizzleBucketStorageFactory({
    runtime,
    dialect: createSqliteDrizzleStorageDialect(runtime),
    slotNamePrefix: options.slotNamePrefix
  });
}
