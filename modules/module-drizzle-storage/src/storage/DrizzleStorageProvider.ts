import { logger } from '@powersync/lib-services-framework';
import { storage, system } from '@powersync/service-core';
import { createSqliteDrizzleStorageFactory } from '../drivers/sqlite/SqliteDrizzleStorageFactory.js';
import {
  DRIZZLE_SQLITE_STORAGE_TYPE,
  DrizzleSqliteStorageConfig,
  isDrizzleStorageConfig,
  normalizeDrizzleSqliteStorageConfig
} from '../types/types.js';
import { DrizzleReportStorage } from './DrizzleReportStorage.js';

export class DrizzleStorageProvider implements storage.StorageProvider {
  get type(): typeof DRIZZLE_SQLITE_STORAGE_TYPE {
    return DRIZZLE_SQLITE_STORAGE_TYPE;
  }

  async getStorage(options: storage.GetStorageOptions): Promise<storage.ActiveStorage> {
    const storageConfig = options.resolvedConfig.storage;
    if (!isDrizzleStorageConfig(storageConfig)) {
      throw new Error(`Cannot create Drizzle storage with provided config ${storageConfig.type}`);
    }
    assertSqliteServiceMode(options.serviceMode);
    const normalizedConfig = normalizeDrizzleSqliteStorageConfig(DrizzleSqliteStorageConfig.decode(storageConfig));
    const factory = createSqliteDrizzleStorageFactory({
      config: normalizedConfig,
      slotNamePrefix: options.resolvedConfig.slot_name_prefix
    });

    return {
      reportStorage: new DrizzleReportStorage(),
      storage: factory,
      shutDown: async () => factory[Symbol.asyncDispose](),
      tearDown: async () => {
        logger.info(`Tearing down Drizzle SQLite storage: ${normalizedConfig.filename}...`);
        for (const table of [
          'write_checkpoints',
          'bucket_parameters',
          'current_data',
          'bucket_data',
          'source_tables',
          'sync_rules',
          'instance'
        ]) {
          factory.runtime.client.exec(`DROP TABLE IF EXISTS \`${table}\``);
        }
        await factory[Symbol.asyncDispose]();
        return true;
      }
    };
  }
}

const SQLITE_ALLOWED_COMMAND_MODES = new Set<string>([
  system.ServiceContextMode.COMPACT,
  system.ServiceContextMode.TEARDOWN,
  system.ServiceContextMode.TEST_CONNECTION
]);

function assertSqliteServiceMode(serviceMode: string): void {
  if (serviceMode == system.ServiceContextMode.UNIFIED || SQLITE_ALLOWED_COMMAND_MODES.has(serviceMode)) {
    return;
  }
  throw new Error(
    `Drizzle SQLite storage only supports the unified service runner. ` +
      `SQLite checkpoint notifications are process-local, so split runners cannot safely share this storage.`
  );
}
