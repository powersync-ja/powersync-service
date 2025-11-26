import { logger } from '@powersync/lib-services-framework';
import { PgPoolOptions } from '@powersync/service-jpgwire';
import { NormalizedPostgresConnectionConfig } from '../types/types.js';
import { PgManager } from './PgManager.js';

export class ConnectionManagerFactory {
  private readonly connectionManagers = new Set<PgManager>();
  public readonly dbConnectionConfig: NormalizedPostgresConnectionConfig;

  constructor(dbConnectionConfig: NormalizedPostgresConnectionConfig) {
    this.dbConnectionConfig = dbConnectionConfig;
  }

  create(poolOptions: PgPoolOptions) {
    const manager = new PgManager(this.dbConnectionConfig, { ...poolOptions });
    this.connectionManagers.add(manager);

    manager.registerListener({
      onEnded: () => {
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
