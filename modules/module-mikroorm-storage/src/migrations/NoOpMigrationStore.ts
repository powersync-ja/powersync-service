import { migrations } from '@powersync/lib-services-framework';

/**
 * Service migrations only trigger MikroORM's migrator for this module.
 * MikroORM owns the actual migration state in its own migration storage.
 */
export class NoOpMigrationStore implements migrations.MigrationStore {
  async load(): Promise<migrations.MigrationState | undefined> {
    return undefined;
  }

  async save(_state: migrations.MigrationState): Promise<void> {}

  async clear(): Promise<void> {}
}
