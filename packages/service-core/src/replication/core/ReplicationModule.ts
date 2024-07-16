import { DataSourceConfig } from '@powersync/service-types/dist/config/PowerSyncConfig.js';
import * as t from 'ts-codec';
import { logger, schema } from '@powersync/lib-services-framework';
import { ReplicationAdapter } from './ReplicationAdapter.js';
import { SyncAPI } from '../../api/SyncAPI.js';
import { AbstractModule, AbstractModuleOptions } from '../../modules/AbstractModule.js';
import { ServiceContext } from '../../system/ServiceContext.js';

export interface ReplicationModuleOptions extends AbstractModuleOptions {
  type: string;
}

/**
 *  A replication module describes all the functionality that PowerSync requires to
 *  replicate data from a DataSource. Whenever a new data source is added to powersync this class should be extended.
 */
export abstract class ReplicationModule extends AbstractModule {
  protected type: string;

  /**
   * @protected
   * @param options
   */
  protected constructor(options: ReplicationModuleOptions) {
    super(options);
    this.type = options.type;
  }

  /**
   *  Create the API adapter for the DataSource required by the sync API
   *  endpoints.
   */
  protected abstract createSyncAPIAdapter(): SyncAPI;

  /**
   *  Create the ReplicationAdapter to be used by PowerSync replicator.
   */
  protected abstract createReplicationAdapter(): ReplicationAdapter<any>;

  /**
   *  Return the TS codec schema describing the required configuration values for this module.
   */
  protected abstract configSchema(): t.ObjectCodec<any>;

  /**
   *  Register this module's replication adapters and sync API providers if the required configuration is present.
   */
  public async initialize(context: ServiceContext): Promise<void> {
    if (!context.configuration.data_sources) {
      // No data source configuration found in the config skip for now
      // TODO: Consider a mechanism to check for config in the ENV variables as well
      return;
    }

    const matchingConfig = context.configuration.data_sources.filter((dataSource) => dataSource.type === this.type);

    if (matchingConfig.length > 1) {
      logger.warning(
        `Multiple data sources of type ${this.type} found in the configuration. Only the first will be used.`
      );
    }

    try {
      // If validation fails, log the error and continue, no replication will happen for this data source
      this.validateConfig(matchingConfig[0]);
      context.replicationEngine.register(this.createReplicationAdapter());
      context.syncAPIProvider.register(this.createSyncAPIAdapter());
    } catch (e) {
      logger.error(e);
    }
  }

  private validateConfig(config: DataSourceConfig): void {
    const validator = schema
      .parseJSONSchema(t.generateJSONSchema(this.configSchema(), { allowAdditional: true }))
      .validator();

    const valid = validator.validate(config);

    if (!valid.valid) {
      throw new Error(`Failed to validate Module ${this.name} configuration: ${valid.errors.join(', ')}`);
    }
  }
}
