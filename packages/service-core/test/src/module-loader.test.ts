import { AbstractModule, loadModules, ServiceContextContainer, TearDownOptions } from '@/index.js';
import { describe, expect, it, vi } from 'vitest';

interface MockConfig {
  connections?: { type: string }[];
  storage: { type: string };
}

class MockMySQLModule extends AbstractModule {
  constructor() {
    super({ name: 'MySQLModule' });
  }
  async initialize(context: ServiceContextContainer): Promise<void> {}
  async teardown(options: TearDownOptions): Promise<void> {}
}
class MockPostgresModule extends AbstractModule {
  constructor() {
    super({ name: 'PostgresModule' });
  }
  async initialize(context: ServiceContextContainer): Promise<void> {}
  async teardown(options: TearDownOptions): Promise<void> {}
}
class MockPostgresStorageModule extends AbstractModule {
  constructor() {
    super({ name: 'PostgresStorageModule' });
  }
  async initialize(context: ServiceContextContainer): Promise<void> {}
  async teardown(options: TearDownOptions): Promise<void> {}
}
const mockLoaders = {
  connection: {
    mysql: async () => {
      return new MockMySQLModule();
    },
    postgresql: async () => {
      return new MockPostgresModule();
    }
  },
  storage: {
    postgresql: async () => {
      return new MockPostgresStorageModule();
    }
  }
};

describe('module loader', () => {
  it('should load all modules defined in connections and storage', async () => {
    const config: MockConfig = {
      connections: [{ type: 'mysql' }, { type: 'postgresql' }],
      storage: { type: 'postgresql' }
    };

    const modules = await loadModules(config as any, mockLoaders);

    expect(modules.length).toBe(3);
    expect(modules[0]).toBeInstanceOf(MockMySQLModule);
    expect(modules[1]).toBeInstanceOf(MockPostgresModule);
    expect(modules[2]).toBeInstanceOf(MockPostgresStorageModule);
  });

  it('should handle duplicate connection types (e.g., mysql used twice)', async () => {
    const config: MockConfig = {
      connections: [{ type: 'mysql' }, { type: 'postgresql' }, { type: 'mysql' }], // mysql duplicated
      storage: { type: 'postgresql' }
    };

    const modules = await loadModules(config as any, mockLoaders);

    // Expect 3 modules: mysql, postgresql, postgresql-storage
    expect(modules.length).toBe(3);
    expect(modules.filter((m) => m instanceof MockMySQLModule).length).toBe(1);
    expect(modules.filter((m) => m instanceof MockPostgresModule).length).toBe(1);
    expect(modules.filter((m) => m instanceof MockPostgresStorageModule).length).toBe(1);
  });

  it('should throw an error if any modules are not found in ModuleMap', async () => {
    const config: MockConfig = {
      connections: [{ type: 'mysql' }, { type: 'redis' }],
      storage: { type: 'postgresql' }
    };

    await expect(loadModules(config as any, mockLoaders)).rejects.toThrowError();
  });

  it('should throw an error if one dynamic connection module import fails', async () => {
    const config: MockConfig = {
      connections: [{ type: 'mysql' }],
      storage: { type: 'postgresql' }
    };

    const loaders = {
      connection: {
        mysql: async () => {
          throw new Error('Failed to load MySQL module');
        }
      },
      storage: mockLoaders.storage
    };

    await expect(loadModules(config as any, loaders)).rejects.toThrowError('Failed to load MySQL module');
  });
});
