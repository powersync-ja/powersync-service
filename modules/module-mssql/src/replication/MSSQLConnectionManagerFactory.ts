import { logger } from '@powersync/lib-services-framework';
import { ResolvedMSSQLConnectionConfig } from '../types/types.js';
import { MSSQLConnectionManager } from './MSSQLConnectionManager.js';
import sql from 'mssql';

export class MSSQLConnectionManagerFactory {
  private readonly connectionManagers: Set<MSSQLConnectionManager>;
  public readonly connectionConfig: ResolvedMSSQLConnectionConfig;

  constructor(connectionConfig: ResolvedMSSQLConnectionConfig) {
    this.connectionConfig = connectionConfig;
    this.connectionManagers = new Set<MSSQLConnectionManager>();
  }

  create(poolOptions: sql.PoolOpts<sql.Connection>) {
    const manager = new MSSQLConnectionManager(this.connectionConfig, poolOptions);
    manager.registerListener({
      onEnded: () => {
        this.connectionManagers.delete(manager);
      }
    });
    this.connectionManagers.add(manager);
    return manager;
  }

  async shutdown() {
    logger.info('Shutting down MSSQL connection Managers...');
    for (const manager of this.connectionManagers.values()) {
      await manager.end();
    }
    logger.info('MSSQL connection Managers shutdown completed.');
  }
}
