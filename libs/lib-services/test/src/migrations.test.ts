import { describe, expect, test, vi } from 'vitest';
import { AbstractMigrationAgent } from '../../src/migrations/AbstractMigrationAgent.js';
import { Direction, Migration, MigrationState, MigrationStore } from '../../src/migrations/migration-definitions.js';
import { MigrationManager } from '../../src/migrations/MigrationManager.js';
import { MockLockManager } from './__mocks__/MockLockManager.js';

class MockMigrationAgent extends AbstractMigrationAgent {
  locks = new MockLockManager();

  store: MigrationStore = {
    clear: async () => {},
    load: async () => {
      // No state stored
      return undefined;
    },
    save: async (state: MigrationState) => {}
  };

  async loadInternalMigrations(): Promise<Migration<{} | undefined>[]> {
    return [];
  }

  async [Symbol.asyncDispose](): Promise<void> {}
}

describe('Migrations', () => {
  test('should allow additional registered migrations', async () => {
    const manager = new MigrationManager();
    manager.registerMigrationAgent(new MockMigrationAgent());

    const additionalMigrationName = 'additional';

    const mockMigration = {
      name: additionalMigrationName,
      up: vi.fn(),
      down: vi.fn()
    };
    manager.registerMigrations([mockMigration]);

    await manager.migrate({
      direction: Direction.Up
    });

    expect(mockMigration.up.mock.calls.length).eq(1);
  });

  test('should run internal migrations', async () => {
    const manager = new MigrationManager();
    const agent = new MockMigrationAgent();
    manager.registerMigrationAgent(agent);

    const internalMigrationName = 'internal';

    const mockMigration = {
      name: internalMigrationName,
      up: vi.fn(),
      down: vi.fn()
    };

    vi.spyOn(agent, 'loadInternalMigrations').mockImplementation(async () => {
      return [mockMigration];
    });

    await manager.migrate({
      direction: Direction.Up
    });

    expect(mockMigration.up.mock.calls.length).eq(1);
  });

  test('should log migration state to store', async () => {
    const manager = new MigrationManager();
    const agent = new MockMigrationAgent();
    manager.registerMigrationAgent(agent);

    const internalMigrationName = 'internal';
    const mockMigration = {
      name: internalMigrationName,
      up: vi.fn(),
      down: vi.fn()
    };

    manager.registerMigrations([mockMigration]);

    const spy = vi.spyOn(agent.store, 'save');

    await manager.migrate({
      direction: Direction.Up
    });

    expect(spy.mock.calls[0][0].last_run).eq(internalMigrationName);
  });
});
