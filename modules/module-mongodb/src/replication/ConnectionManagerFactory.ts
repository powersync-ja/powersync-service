import { logger } from '@powersync/lib-services-framework';
import { NormalizedMongoConnectionConfig } from '../types/types.js';
import { MongoManager } from './MongoManager.js';

export class ConnectionManagerFactory {
  private readonly connectionManagers = new Set<MongoManager>();
  public readonly dbConnectionConfig: NormalizedMongoConnectionConfig;

  constructor(dbConnectionConfig: NormalizedMongoConnectionConfig) {
    this.dbConnectionConfig = dbConnectionConfig;
  }

  create() {
    const manager = new MongoManager(this.dbConnectionConfig);
    this.connectionManagers.add(manager);

    manager.registerListener({
      onEnded: () => {
        this.connectionManagers.delete(manager);
      }
    });
    return manager;
  }

  async shutdown() {
    logger.info('Shutting down MongoDB connection Managers...');
    for (const manager of [...this.connectionManagers]) {
      await manager.end();
    }
    logger.info('MongoDB connection Managers shutdown completed.');
  }
}
