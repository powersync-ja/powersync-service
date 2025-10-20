import { logger } from '@powersync/lib-services-framework';
import { ResolvedConnectionConfig } from '../types/types.js';
import { MSSQLConnectionManager } from './MSSQLConnectionManager.js';
import sql from 'mssql';

export class MSSQLConnectionManagerFactory {
  private readonly connectionManagers: MSSQLConnectionManager[];
  public readonly connectionConfig: ResolvedConnectionConfig;

  constructor(connectionConfig: ResolvedConnectionConfig) {
    this.connectionConfig = connectionConfig;
    this.connectionManagers = [];
  }

  create(poolOptions: sql.PoolOpts<sql.Connection>) {
    const manager = new MSSQLConnectionManager(this.connectionConfig, poolOptions);
    this.connectionManagers.push(manager);
    return manager;
  }

  async shutdown() {
    logger.info('Shutting down MSSQL connection Managers...');
    for (const manager of this.connectionManagers) {
      await manager.end();
    }
    logger.info('MSSQL connection Managers shutdown completed.');
  }
}
