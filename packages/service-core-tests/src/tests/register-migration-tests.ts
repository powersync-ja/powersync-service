import { AbstractPowerSyncMigrationAgent, framework, PowerSyncMigrationManager } from '@powersync/service-core';
import { expect, test, vi } from 'vitest';

const generateTestMigrations = (length: number, start: number = 0) => {
  const results: string[] = [];
  return {
    results,
    tests: Array.from({ length }).map((v, index) => {
      const i = index + start;
      return {
        down: vi.fn(async () => {
          results.push(`down - ${i}`);
        }),
        up: vi.fn(async () => {
          results.push(`up - ${i}`);
        }),
        name: i.toString()
      };
    })
  };
};

/**
 * Reset the factory as part of disposal. This helps cleanup after tests.
 */
const managedResetAgent = (factory: () => AbstractPowerSyncMigrationAgent) => {
  const agent = factory();
  return {
    agent,
    // Reset the store for the next tests
    [Symbol.asyncDispose]: () => agent.resetStore()
  };
};

export const registerMigrationTests = (migrationAgentFactory: () => AbstractPowerSyncMigrationAgent) => {
  test('Should run migrations correctly', async () => {
    await using manager = new framework.migrations.MigrationManager() as PowerSyncMigrationManager;
    // Disposal is executed in reverse order. The store will be reset before the manage disposes it.
    await using managedAgent = managedResetAgent(migrationAgentFactory);

    await managedAgent.agent.resetStore();

    manager.registerMigrationAgent(managedAgent.agent);

    const length = 10;
    const { tests, results } = generateTestMigrations(length);
    manager.registerMigrations(tests);

    await manager.migrate({
      direction: framework.migrations.Direction.Up
    });

    const upLogs = Array.from({ length }).map((v, index) => `up - ${index}`);

    expect(results).deep.equals(upLogs);

    // Running up again should not run any migrations
    await manager.migrate({
      direction: framework.migrations.Direction.Up
    });

    expect(results.length).equals(length);

    // Clear the results
    results.splice(0, length);

    await manager.migrate({
      direction: framework.migrations.Direction.Down
    });

    const downLogs = Array.from({ length })
      .map((v, index) => `down - ${index}`)
      .reverse();
    expect(results).deep.equals(downLogs);

    // Running down again should not run any additional migrations
    await manager.migrate({
      direction: framework.migrations.Direction.Down
    });

    expect(results.length).equals(length);

    // Clear the results
    results.splice(0, length);

    // Running up should run the up migrations again
    await manager.migrate({
      direction: framework.migrations.Direction.Up
    });

    expect(results).deep.equals(upLogs);
  });

  test('Should run migrations with additions', async () => {
    await using manager = new framework.migrations.MigrationManager() as PowerSyncMigrationManager;
    // Disposal is executed in reverse order. The store will be reset before the manage disposes it.
    await using managedAgent = managedResetAgent(migrationAgentFactory);

    await managedAgent.agent.resetStore();

    manager.registerMigrationAgent(managedAgent.agent);

    const length = 10;
    const { tests, results } = generateTestMigrations(length);
    manager.registerMigrations(tests);

    await manager.migrate({
      direction: framework.migrations.Direction.Up
    });

    const upLogs = Array.from({ length }).map((v, index) => `up - ${index}`);

    expect(results).deep.equals(upLogs);

    // Add a new migration
    const { results: newResults, tests: newTests } = generateTestMigrations(1, 10);
    manager.registerMigrations(newTests);

    // Running up again should not run any migrations
    await manager.migrate({
      direction: framework.migrations.Direction.Up
    });

    // The original tests should not have been executed again
    expect(results.length).equals(length);

    // The new migration should have been executed
    expect(newResults).deep.equals([`up - ${10}`]);
  });
};
