import * as lib_postgres from '@powersync/lib-service-postgres';
import { logger } from '@powersync/lib-services-framework';
import { storage } from '@powersync/service-core';

import { isPostgresStorageConfig, normalizePostgresStorageConfig, PostgresStorageConfig } from '../types/types.js';
import { dropTables } from '../utils/db.js';
import { PostgresBucketStorageFactory } from './PostgresBucketStorageFactory.js';
import { PostgresReportStorageFactory } from './PostgresReportStorageFactory.js';

export class PostgresStorageProvider implements storage.BucketStorageProvider {
  get type() {
    return lib_postgres.POSTGRES_CONNECTION_TYPE;
  }

  async getStorage(options: storage.GetStorageOptions): Promise<storage.ActiveStorage> {
    const { resolvedConfig } = options;

    const { storage } = resolvedConfig;
    if (!isPostgresStorageConfig(storage)) {
      // This should not be reached since the generation should be managed externally.
      throw new Error(
        `Cannot create Postgres bucket storage with provided config ${storage.type} !== ${lib_postgres.POSTGRES_CONNECTION_TYPE}`
      );
    }

    const decodedConfig = PostgresStorageConfig.decode(storage);
    const normalizedConfig = normalizePostgresStorageConfig(decodedConfig);
    const storageFactory = new PostgresBucketStorageFactory({
      config: normalizedConfig,
      slot_name_prefix: options.resolvedConfig.slot_name_prefix
    });

    const reportStorageFactory = new PostgresReportStorageFactory({
      config: normalizedConfig
    });

    return {
      reportStorage: reportStorageFactory,
      storage: storageFactory,
      shutDown: async () => {
        await storageFactory.db[Symbol.asyncDispose]();
        await reportStorageFactory.db[Symbol.asyncDispose]();
      },
      tearDown: async () => {
        logger.info(`Tearing down Postgres storage: ${normalizedConfig.database}...`);
        await dropTables(storageFactory.db);
        await storageFactory.db[Symbol.asyncDispose]();
        await reportStorageFactory.db[Symbol.asyncDispose]();
        return true;
      }
    } satisfies storage.ActiveStorage;
  }
}
