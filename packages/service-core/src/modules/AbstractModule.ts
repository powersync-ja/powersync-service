import { ServiceContext } from '../system/ServiceContext.js';

export interface AbstractModuleOptions {
  name: string;
}

export abstract class AbstractModule {
  public name: string;
  protected options: AbstractModuleOptions;

  protected constructor(options: AbstractModuleOptions) {
    this.options = options;
    this.name = options.name;
  }

  /**
   * Register any functionality on the {@link ServiceContext}.
   * Note this will be executed before the ServiceContext has been initialized.
   * This can be used to register storage providers which will be created during initialization.
   */
  public abstract register(context: ServiceContext): Promise<void>;

  /**
   *  Initialize the module using any required services from the ServiceContext
   */
  public abstract initialize(context: ServiceContext): Promise<void>;

  /**
   *  Finish processing any requests and gracefully shut down any resources managed by the module
   */
  public abstract shutdown(): Promise<void>;

  /**
   *  Terminate and clean up any resources managed by the module right away
   */
  public abstract teardown(context: ServiceContext): Promise<void>;
}
