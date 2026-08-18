import { logger } from '@powersync/lib-services-framework';
import { storage, system } from '@powersync/service-core';
import { createMySqlMikroOrm } from '../drivers/mysql/mysql-config.js';
import { createMySqlMikroOrmStorageFactory } from '../drivers/mysql/MySqlMikroOrmStorageFactory.js';
import { createSqliteMikroOrm } from '../drivers/sqlite/sqlite-config.js';
import { createSqliteMikroOrmStorageFactory } from '../drivers/sqlite/SqliteMikroOrmStorageFactory.js';
import {
  isMikroOrmMySqlStorageConfig,
  isMikroOrmSqliteStorageConfig,
  MIKRO_ORM_MYSQL_STORAGE_TYPE,
  MIKRO_ORM_SQLITE_STORAGE_TYPE,
  MikroOrmMySqlStorageConfig,
  MikroOrmSqliteStorageConfig,
  normalizeMikroOrmMySqlStorageConfig,
  normalizeMikroOrmSqliteStorageConfig
} from '../types/types.js';
import { MikroOrmReportStorage } from './MikroOrmReportStorage.js';

export class MikroOrmStorageProvider implements storage.StorageProvider {
  constructor(private readonly storageType: typeof MIKRO_ORM_SQLITE_STORAGE_TYPE | typeof MIKRO_ORM_MYSQL_STORAGE_TYPE) {}

  get type() {
    return this.storageType;
  }

  async getStorage(options: storage.GetStorageOptions): Promise<storage.ActiveStorage> {
    const { storage: storageConfig } = options.resolvedConfig;

    if (this.storageType == MIKRO_ORM_SQLITE_STORAGE_TYPE && isMikroOrmSqliteStorageConfig(storageConfig)) {
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

      return activeStorage({
        storageFactory,
        tearDownLabel: `MikroORM SQLite storage: ${normalizedConfig.filename}`,
        tearDown: () => orm.schema.drop()
      });
    }

    if (this.storageType == MIKRO_ORM_MYSQL_STORAGE_TYPE && isMikroOrmMySqlStorageConfig(storageConfig)) {
      const decodedConfig = MikroOrmMySqlStorageConfig.decode(storageConfig);
      const normalizedConfig = normalizeMikroOrmMySqlStorageConfig(decodedConfig);
      const orm = await createMySqlMikroOrm(normalizedConfig);
      await orm.schema.update();

      const storageFactory = await createMySqlMikroOrmStorageFactory({
        config: normalizedConfig,
        slotNamePrefix: options.resolvedConfig.slot_name_prefix,
        orm
      });

      return activeStorage({
        storageFactory,
        tearDownLabel: 'MikroORM MySQL storage',
        tearDown: () => orm.schema.drop()
      });
    }

    throw new Error(`Cannot create ${this.storageType} storage with provided config ${storageConfig.type}`);
  }
}

function activeStorage(options: {
  storageFactory: storage.BucketStorageFactory;
  tearDownLabel: string;
  tearDown: () => Promise<unknown>;
}): storage.ActiveStorage {
  const reportStorage = new MikroOrmReportStorage();
  return {
    reportStorage,
    storage: options.storageFactory,
    shutDown: async () => {
      await options.storageFactory[Symbol.asyncDispose]();
    },
    tearDown: async () => {
      logger.info(`Tearing down ${options.tearDownLabel}...`);
      await options.tearDown();
      await options.storageFactory[Symbol.asyncDispose]();
      return true;
    }
  };
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
