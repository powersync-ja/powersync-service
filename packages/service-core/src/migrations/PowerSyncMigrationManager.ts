import * as framework from '@powersync/lib-services-framework';
import fs from 'fs/promises';
import path from 'path';
import * as system from '../system/system-index.js';

/**
 * PowerSync service migrations each have this context available to the `up` and `down` methods.
 */
export interface PowerSyncMigrationContext {
  service_context: system.ServiceContext;
}

export interface PowerSyncMigrationGenerics extends framework.MigrationAgentGenerics {
  MIGRATION_CONTEXT: PowerSyncMigrationContext;
}

export type PowerSyncMigrationFunction = framework.MigrationFunction<PowerSyncMigrationContext>;

export abstract class AbstractPowerSyncMigrationAgent extends framework.AbstractMigrationAgent<PowerSyncMigrationGenerics> {
  abstract getInternalScriptsDir(): string;

  async loadInternalMigrations(): Promise<framework.Migration<PowerSyncMigrationContext>[]> {
    const migrationsDir = this.getInternalScriptsDir();
    const files = await fs.readdir(migrationsDir);
    const migrations = files.filter((file) => {
      // Vitest resolves ts files
      return ['.js', '.ts'].includes(path.extname(file));
    });

    return await Promise.all(
      migrations.map(async (migration) => {
        const module = await import(path.resolve(migrationsDir, migration));
        return {
          name: path.basename(migration).replace(path.extname(migration), ''),
          up: module.up,
          down: module.down
        };
      })
    );
  }
}

export type PowerSyncMigrationManager = framework.MigrationManager<PowerSyncMigrationGenerics>;
