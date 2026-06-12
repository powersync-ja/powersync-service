import { logger } from '@powersync/lib-services-framework';
import { storage, system } from '@powersync/service-core';
import { createSqliteMikroOrm } from '../drivers/sqlite/sqlite-config.js';
import { createSqliteMikroOrmStorageFactory } from '../drivers/sqlite/SqliteMikroOrmStorageFactory.js';
import {
  isMikroOrmSqliteStorageConfig,
  MIKRO_ORM_SQLITE_STORAGE_TYPE,
  MikroOrmSqliteStorageConfig,
  normalizeMikroOrmSqliteStorageConfig
} from '../types/types.js';
import { MikroOrmReportStorage } from './MikroOrmReportStorage.js';

export class MikroOrmStorageProvider implements storage.StorageProvider {
  get type() {
    return MIKRO_ORM_SQLITE_STORAGE_TYPE;
  }

  async getStorage(options: storage.GetStorageOptions): Promise<storage.ActiveStorage> {
    const { storage: storageConfig } = options.resolvedConfig;

    if (!isMikroOrmSqliteStorageConfig(storageConfig)) {
      throw new Error(`Cannot create MikroORM SQLite storage with provided config ${storageConfig.type}`);
    }
    assertSqliteServiceMode(options.serviceMode);

    const decodedConfig = MikroOrmSqliteStorageConfig.decode(storageConfig);
    const normalizedConfig = normalizeMikroOrmSqliteStorageConfig(decodedConfig);
    const orm = await createSqliteMikroOrm(normalizedConfig);
    await orm.schema.update();

    const storageFactory = await createSqliteMikroOrmStorageFactory({
      config: normalizedConfig,
      slotNamePrefix: options.resolvedConfig.slot_name_prefix,
      orm
    });
    const reportStorage = new MikroOrmReportStorage();

    return {
      reportStorage,
      storage: storageFactory,
      shutDown: async () => {
        await storageFactory[Symbol.asyncDispose]();
      },
      tearDown: async () => {
        logger.info(`Tearing down MikroORM SQLite storage: ${normalizedConfig.filename}...`);
        await orm.schema.drop();
        await storageFactory[Symbol.asyncDispose]();
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
    `MikroORM SQLite storage only supports the unified service runner. ` +
      `SQLite checkpoint notifications are process-local, so split "${system.ServiceContextMode.API}" and "${system.ServiceContextMode.SYNC}" runners cannot safely share this storage. ` +
      `Start the service with runner type "${system.ServiceContextMode.UNIFIED}" instead of "${serviceMode}".`
  );
}
