import { migrations } from '@powersync/lib-services-framework';

export class NoOpMigrationStore implements migrations.MigrationStore {
  async load(): Promise<migrations.MigrationState | undefined> {
    return undefined;
  }
  async save(_state: migrations.MigrationState): Promise<void> {}
  async clear(): Promise<void> {}
}
