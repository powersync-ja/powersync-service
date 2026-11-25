import { logger } from '@powersync/lib-services-framework';
import sql from 'mssql';
import { ResolvedMSSQLConnectionConfig } from '../types/types.js';
import { MSSQLConnectionManager } from './MSSQLConnectionManager.js';

// Replication requests get a longer timeout than the mssql default of 15s. SQL Server sorts the changes of a large
// transaction before returning the first row, which can take longer than 15s, and every retry reads the same changes.
const REPLICATION_REQUEST_TIMEOUT_MS = 120_000;

export class MSSQLConnectionManagerFactory {
  private readonly connectionManagers: Set<MSSQLConnectionManager>;
  public readonly connectionConfig: ResolvedMSSQLConnectionConfig;

  constructor(connectionConfig: ResolvedMSSQLConnectionConfig) {
    this.connectionConfig = connectionConfig;
    this.connectionManagers = new Set<MSSQLConnectionManager>();
  }

  create(poolOptions: sql.PoolOpts<sql.Connection>) {
    const manager = new MSSQLConnectionManager(this.connectionConfig, poolOptions, REPLICATION_REQUEST_TIMEOUT_MS);
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
