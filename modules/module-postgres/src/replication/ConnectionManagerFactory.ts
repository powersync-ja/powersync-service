import { PgManager } from './PgManager.js';
import { NormalizedPostgresConnectionConfig } from '../types/types.js';
import { PgPoolOptions } from '@powersync/service-jpgwire';
import { logger } from '@powersync/lib-services-framework';
import { CustomTypeRegistry } from '../types/registry.js';

export class ConnectionManagerFactory {
  private readonly connectionManagers = new Set<PgManager>();
  public readonly dbConnectionConfig: NormalizedPostgresConnectionConfig;

  constructor(
    dbConnectionConfig: NormalizedPostgresConnectionConfig,
    private readonly registry: CustomTypeRegistry
  ) {
    this.dbConnectionConfig = dbConnectionConfig;
  }

  create(poolOptions: PgPoolOptions) {
    const manager = new PgManager(this.dbConnectionConfig, { ...poolOptions, registry: this.registry });
    console.log('create ConnectionManagerFactory', this.connectionManagers.size);
    this.connectionManagers.add(manager);

    manager.registerListener({
      onEnded: () => {
        console.log('manager ended');
        this.connectionManagers.delete(manager);
      }
    });
    return manager;
  }

  async shutdown() {
    logger.info('Shutting down Postgres connection Managers...');
    for (const manager of [...this.connectionManagers]) {
      await manager.end();
    }
    logger.info('Postgres connection Managers shutdown completed.');
  }
}
