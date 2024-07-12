import { SyncAPI } from './SyncAPI.js';
import { logger } from '@powersync/lib-services-framework';

/**
 *  Serves as a registry from which SyncAPIs can be retrieved based on Replication DataSource type
 *  Initially only one SyncAPI per DataSource type is supported
 */
export class SyncAPIProvider {
  private api: SyncAPI | undefined;

  public register(api: SyncAPI) {
    if (this.api) {
      logger.warn('A SyncAPI has already been registered. Overriding existing implementation');
    }

    this.api = api;
  }

  public getSyncAPI(): SyncAPI {
    if (!this.api) {
      throw new Error('No SyncAPI has been registered yet.');
    }

    return this.api;
  }
}