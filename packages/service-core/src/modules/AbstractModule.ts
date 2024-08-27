import { ServiceContextContainer } from '../system/ServiceContext.js';
import { logger } from '@powersync/lib-services-framework';
import winston from 'winston';

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
   *  Terminate and clean up any resources managed by the module right away
   */
  public abstract teardown(): Promise<void>;

  public get name() {
    return this.options.name;
  }
}
