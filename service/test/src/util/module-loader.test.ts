import { vi, describe, test, expect, it } from 'vitest';

import { logger } from '@powersync/lib-services-framework';

import { MySQLModule } from '@powersync/service-module-mysql';
import { PostgresModule } from '@powersync/service-module-postgres';
import { PostgresStorageModule } from '@powersync/service-module-postgres-storage';
import { loadModules } from '../../../src/util/module-loader.js';

interface MockConfig {
  connections?: MockConnection[];
  storage: { type: string };
}

describe('module loader', () => {
  it('should load all modules defined in connections and storage', async () => {
    const config: MockConfig = {
      connections: [{ type: 'mysql' }, { type: 'postgresql' }],
      storage: { type: 'postgresql' }
    };
    const modules = await loadModules(config);

    expect(modules.length).toBe(3);
    expect(modules[0]).toBeInstanceOf(MySQLModule);
    expect(modules[1]).toBeInstanceOf(PostgresModule);
    expect(modules[2]).toBeInstanceOf(PostgresStorageModule);
  });

  it('should handle duplicate connection types (e.g., mysql used twice)', async () => {
    const config: MockConfig = {
      connections: [{ type: 'mysql' }, { type: 'postgresql' }, { type: 'mysql' }], // mysql duplicated
      storage: { type: 'postgresql' }
    };

    const modules = await loadModules(config);

    // Expect 3 modules: mysql, postgresql, postgresql-storage
    expect(modules.length).toBe(3);
    expect(modules.filter((m) => m instanceof MySQLModule).length).toBe(1);
    expect(modules.filter((m) => m instanceof PostgresModule).length).toBe(1);
    expect(modules.filter((m) => m instanceof PostgresStorageModule).length).toBe(1);
  });

  it('should throw an error if any modules are not found in ModuleMap', async () => {
    const config: MockConfig = {
      connections: [{ type: 'mysql' }, { type: 'redis' }],
      storage: { type: 'postgresql' }
    };

    await expect(loadModules(config)).rejects.toThrowError();
  });

  it('should throw an error if one dynamic connection module import fails', async () => {
    vi.doMock('../../../src/util/module-loader.js', async (importOriginal) => {
      const mod = await importOriginal();
      mod.ConnectionModuleMap.mysql = () =>
        import('@powersync/service-module-mysql').then(() => {
          throw new Error('Failed to load MySQL module');
        });
      return mod;
    });

    const { loadModules } = await import('../../../src/util/module-loader.js');

    const config: MockConfig = {
      connections: [{ type: 'mysql' }],
      storage: { type: 'mongodb' }
    };

    await expect(loadModules(config)).rejects.toThrowError('Failed to load MySQL module');

    vi.doUnmock('@powersync/service-module-mysql');
  });
});
