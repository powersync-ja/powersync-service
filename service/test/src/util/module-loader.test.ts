import { describe, test, expect, it } from 'vitest';

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
      storage: { type: 'postgresql' } // This should result in 'postgresql-storage'
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

  it('should exclude connections starting with "mongo"', async () => {
    const config: MockConfig = {
      connections: [{ type: 'mysql' }, { type: 'mongodb' }], // mongodb should be ignored
      storage: { type: 'postgresql' }
    };

    const modules = await loadModules(config);

    // Expect 2 modules: mysql and postgresql-storage
    expect(modules.length).toBe(2);
    expect(modules[0]).toBeInstanceOf(MySQLModule);
    expect(modules[1]).toBeInstanceOf(PostgresStorageModule);
    expect(modules.filter((m) => m instanceof PostgresModule).length).toBe(0);
  });

  it('should filter out modules not found in ModuleMap', async () => {
    const config: MockConfig = {
      connections: [{ type: 'mysql' }, { type: 'redis' }], // unknown-db is missing
      storage: { type: 'postgresql' }
    };

    const modules = await loadModules(config);

    // Expect 2 modules: mysql and postgresql-storage
    expect(modules.length).toBe(2);
    expect(modules.every((m) => m instanceof MySQLModule || m instanceof PostgresStorageModule)).toBe(true);
  });

  it('should filter out modules that fail to import and log an error', async () => {
    const config: MockConfig = {
      connections: [{ type: 'mysql' }, { type: 'failing-module' }], // failing-module rejects promise
      storage: { type: 'postgresql' }
    };

    const modules = await loadModules(config);

    // Expect 2 modules: mysql and postgresql-storage
    expect(modules.length).toBe(2);
    expect(modules.filter((m) => m instanceof MySQLModule).length).toBe(1);
  });
});
