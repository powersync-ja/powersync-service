import { logger } from '@powersync/lib-services-framework';
import { AbstractModule } from './AbstractModule.js';
import { ServiceContext } from '../system/ServiceContext.js';

/**
 *  The module manager is responsible for managing the lifecycle of all modules in the system.
 */
export class ModuleManager {
  private readonly modules: Map<string, AbstractModule> = new Map();

  public register(modules: AbstractModule[]) {
    for (const module of modules) {
      if (this.modules.has(module.name)) {
        logger.warn(`Module ${module.name} already registered, skipping...`);
      } else {
        this.modules.set(module.name, module);
      }
    }
  }

  async initialize(context: ServiceContext) {
    for (const module of this.modules.values()) {
      await module.initialize(context);
    }
  }

  async shutDown() {
    for (const module of this.modules.values()) {
      await module.shutdown();
    }
  }

  async tearDown() {
    for (const module of this.modules.values()) {
      await module.teardown();
    }
  }
}
