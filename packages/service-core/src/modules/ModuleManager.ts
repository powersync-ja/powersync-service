import { logger } from '@powersync/lib-services-framework';
import { ServiceContext } from '../system/ServiceContext.js';
import { RunnerConfig } from '../util/util-index.js';
import { AbstractModule } from './AbstractModule.js';

/**
 *  The module manager is responsible for managing the lifecycle of all modules in the system.
 */
export class ModuleManager {
  readonly serviceContext: ServiceContext;

  private readonly modules: Map<string, AbstractModule> = new Map();

  constructor(serviceContext?: ServiceContext) {
    this.serviceContext = serviceContext ?? new ServiceContext();
  }

  public register(modules: AbstractModule[]) {
    for (const module of modules) {
      if (this.modules.has(module.name)) {
        logger.warn(`Module ${module.name} already registered, skipping...`);
        return;
      } else {
        this.modules.set(module.name, module);
      }
      // Let the module register functionality on the service context
      module.register(this.serviceContext);
    }
  }

  async initialize(entryConfig: RunnerConfig) {
    await this.serviceContext.initialize(entryConfig);

    for (const module of this.modules.values()) {
      await module.initialize(this.serviceContext);
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
