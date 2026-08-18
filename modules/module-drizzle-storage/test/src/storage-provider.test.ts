import { storage, system } from '@powersync/service-core';
import { describe, expect, it } from 'vitest';
import { DRIZZLE_SQLITE_STORAGE_TYPE, DrizzleStorageProvider } from '../../src/index.js';

const RESOLVED_CONFIG = {
  storage: { type: DRIZZLE_SQLITE_STORAGE_TYPE, filename: ':memory:' },
  slot_name_prefix: 'test_'
} as const;

function getStorageOptions(serviceMode: system.ServiceContextMode): storage.GetStorageOptions {
  return {
    resolvedConfig: RESOLVED_CONFIG as unknown as storage.GetStorageOptions['resolvedConfig'],
    serviceMode
  };
}

describe('Drizzle SQLite storage provider', () => {
  it('rejects split service runners', async () => {
    const provider = new DrizzleStorageProvider();
    await expect(provider.getStorage(getStorageOptions(system.ServiceContextMode.API))).rejects.toThrow(
      'Drizzle SQLite storage only supports the unified service runner'
    );
    await expect(provider.getStorage(getStorageOptions(system.ServiceContextMode.SYNC))).rejects.toThrow(
      'Drizzle SQLite storage only supports the unified service runner'
    );
  });

  it('allows the unified service runner', async () => {
    const activeStorage = await new DrizzleStorageProvider().getStorage(
      getStorageOptions(system.ServiceContextMode.UNIFIED)
    );
    await activeStorage.shutDown();
  });
});
