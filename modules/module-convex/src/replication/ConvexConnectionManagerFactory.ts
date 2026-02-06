import { logger } from '@powersync/lib-services-framework';
import { ResolvedConvexConnectionConfig } from '../types/types.js';
import { ConvexConnectionManager } from './ConvexConnectionManager.js';

export class ConvexConnectionManagerFactory {
  private readonly connectionManagers = new Set<ConvexConnectionManager>();

  constructor(public readonly connectionConfig: ResolvedConvexConnectionConfig) {}

  create() {
    const manager = new ConvexConnectionManager(this.connectionConfig);
    manager.registerListener({
      onEnded: () => {
        this.connectionManagers.delete(manager);
      }
    });
    this.connectionManagers.add(manager);
    return manager;
  }

  async shutdown() {
    logger.info('Shutting down Convex connection managers...');
    for (const manager of [...this.connectionManagers]) {
      await manager.end();
    }
    logger.info('Convex connection managers shutdown completed.');
  }
}
