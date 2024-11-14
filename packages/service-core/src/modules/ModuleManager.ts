import { logger } from '@powersync/lib-services-framework';
import * as system from '../system/system-index.js';
import { AbstractModule, TearDownOptions } from './AbstractModule.js';
/**
 *  The module manager keeps track of activated modules
 */
export class ModuleManager {
  private readonly modules: Map<string, AbstractModule> = new Map();

  public register(modules: AbstractModule[]) {
    for (const module of modules) {
      if (this.modules.has(module.name)) {
        logger.warn(`Module ${module.name} already registered, skipping.`);
        continue;
      }
      this.modules.set(module.name, module);
      logger.info(`Successfully registered Module ${module.name}.`);
    }
  }

  async initialize(serviceContext: system.ServiceContextContainer) {
    logger.info(`Initializing modules...`);
    for (const module of this.modules.values()) {
      await module.initialize(serviceContext);
    }
    logger.info(`Successfully Initialized modules.`);
  }

  async tearDown(options: TearDownOptions) {
    for (const module of this.modules.values()) {
      await module.teardown(options);
    }
  }
}
