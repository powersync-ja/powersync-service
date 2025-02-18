import { ServiceContextContainer } from '../system/ServiceContext.js';
import { logger } from '@powersync/lib-services-framework';
import winston from 'winston';
import { PersistedSyncRulesContent } from '../storage/storage-index.js';

export interface TearDownOptions {
  /**
   *  If required, tear down any configuration/state for the specific sync rules
   */
  syncRules?: PersistedSyncRulesContent[];
}

export interface AbstractModuleOptions {
  name: string;
}

export abstract class AbstractModule {
  protected logger: winston.Logger;

  protected constructor(protected options: AbstractModuleOptions) {
    this.logger = logger.child({ name: `Module:${options.name}` });
  }

  /**
   *  Initialize the module using any required services from the ServiceContext
   */
  public abstract initialize(context: ServiceContextContainer): Promise<void>;

  /**
   *  Permanently clean up and dispose of any configuration or state for this module.
   */
  public abstract teardown(options: TearDownOptions): Promise<void>;

  public get name() {
    return this.options.name;
  }
}
