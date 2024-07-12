import { ServiceContext } from '../system/ServiceContext.js';

export abstract class AbstractModule {
  public name: string;

  protected constructor(name: string) {
    this.name = name;
  }

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
  public abstract teardown(): Promise<void>;
}
