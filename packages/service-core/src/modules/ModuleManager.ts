import { logger } from '@powersync/lib-services-framework';
import * as system from '../system/system-index.js';
import { AbstractModule } from './AbstractModule.js';
/**
 *  The module manager is responsible for managing the lifecycle of all modules in the system.
 */
export class ModuleManager {
  private readonly modules: Map<string, AbstractModule> = new Map();

  constructor() {}

  public register(modules: AbstractModule[]) {
    for (const module of modules) {
      if (this.modules.has(module.name)) {
        logger.warn(`Module ${module.name} already registered, skipping...`);
        continue;
      }
      this.modules.set(module.name, module);
    }
  }

  async initialize(serviceContext: system.ServiceContextContainer) {
    for (const module of this.modules.values()) {
      await module.initialize(serviceContext);
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
