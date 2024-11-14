import { logger } from '@powersync/lib-services-framework';
import mysql from 'mysql2/promise';
import { MySQLConnectionManager } from './MySQLConnectionManager.js';
import { ResolvedConnectionConfig } from '../types/types.js';

export class MySQLConnectionManagerFactory {
  private readonly connectionManagers: MySQLConnectionManager[];
  private readonly connectionConfig: ResolvedConnectionConfig;

  constructor(connectionConfig: ResolvedConnectionConfig) {
    this.connectionConfig = connectionConfig;
    this.connectionManagers = [];
  }

  create(poolOptions: mysql.PoolOptions) {
    const manager = new MySQLConnectionManager(this.connectionConfig, poolOptions);
    this.connectionManagers.push(manager);
    return manager;
  }

  async shutdown() {
    logger.info('Shutting down MySQL connection Managers...');
    for (const manager of this.connectionManagers) {
      await manager.end();
    }
    logger.info('MySQL connection Managers shutdown completed.');
  }
}
