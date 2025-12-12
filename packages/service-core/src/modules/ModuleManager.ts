import { logger } from '@powersync/lib-services-framework';
import * as system from '../system/system-index.js';
import { AbstractModule, TearDownOptions } from './AbstractModule.js';
import { loadModules, ModuleLoaders } from './loader.js';
/**
 *  The module manager keeps track of activated modules
 */
export class ModuleManager {
  private readonly modules: Map<string, AbstractModule> = new Map();
  private moduleLoaders: ModuleLoaders | undefined;

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

  public registerDynamicModules(moduleLoaders: ModuleLoaders) {
    this.moduleLoaders = moduleLoaders;
  }

  async initialize(serviceContext: system.ServiceContextContainer) {
    logger.info(`Loading dynamic modules...`);
    if (this.moduleLoaders) {
      const dynamicModules = await loadModules(serviceContext.configuration, this.moduleLoaders);
      this.register(dynamicModules);
    }
    logger.info(`Successfully loaded dynamic modules.`);

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
