import { storage, system } from '@powersync/service-core';
import { describe, expect, it } from 'vitest';
import { MIKRO_ORM_SQLITE_STORAGE_TYPE, MikroOrmStorageProvider } from '../../src/index.js';

const RESOLVED_CONFIG = {
  storage: {
    type: MIKRO_ORM_SQLITE_STORAGE_TYPE,
    filename: ':memory:'
  },
  slot_name_prefix: 'test_'
} as const;

function getStorageOptions(serviceMode: system.ServiceContextMode): storage.GetStorageOptions {
  return {
    resolvedConfig: RESOLVED_CONFIG as unknown as storage.GetStorageOptions['resolvedConfig'],
    serviceMode
  };
}

describe('MikroORM SQLite storage provider', () => {
  it('rejects split service runners', async () => {
    const provider = new MikroOrmStorageProvider();

    await expect(provider.getStorage(getStorageOptions(system.ServiceContextMode.API))).rejects.toThrow(
      'MikroORM SQLite storage only supports the unified service runner'
    );
    await expect(provider.getStorage(getStorageOptions(system.ServiceContextMode.SYNC))).rejects.toThrow(
      'MikroORM SQLite storage only supports the unified service runner'
    );
  });

  it('allows the unified service runner', async () => {
    const provider = new MikroOrmStorageProvider();
    const activeStorage = await provider.getStorage(getStorageOptions(system.ServiceContextMode.UNIFIED));

    await activeStorage.shutDown();
  });
});
