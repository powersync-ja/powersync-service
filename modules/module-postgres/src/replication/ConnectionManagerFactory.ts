import { PgManager } from './PgManager.js';
import { NormalizedPostgresConnectionConfig } from '../types/types.js';
import { PgPoolOptions } from '@powersync/service-jpgwire';
import { logger } from '@powersync/lib-services-framework';

export class ConnectionManagerFactory {
  private readonly connectionManagers: PgManager[];
  public readonly dbConnectionConfig: NormalizedPostgresConnectionConfig;

  constructor(dbConnectionConfig: NormalizedPostgresConnectionConfig) {
    this.dbConnectionConfig = dbConnectionConfig;
    this.connectionManagers = [];
  }

  create(poolOptions: PgPoolOptions) {
    const manager = new PgManager(this.dbConnectionConfig, poolOptions);
    this.connectionManagers.push(manager);
    return manager;
  }

  async shutdown() {
    logger.info('Shutting down Postgres connection Managers...');
    for (const manager of this.connectionManagers) {
      await manager.end();
    }
    logger.info('Postgres connection Managers shutdown completed.');
  }
}
