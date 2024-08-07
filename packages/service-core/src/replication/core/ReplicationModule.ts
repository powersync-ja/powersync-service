import { logger, schema } from '@powersync/lib-services-framework';
import { DataSourceConfig } from '@powersync/service-types/dist/config/PowerSyncConfig.js';
import * as t from 'ts-codec';

import * as types from '@powersync/service-types';
import * as api from '../../api/api-index.js';
import * as modules from '../../modules/modules-index.js';
import * as system from '../../system/system-index.js';
import { ReplicationAdapter } from './ReplicationAdapter.js';

export interface ReplicationModuleOptions extends modules.AbstractModuleOptions {
  type: string;
}

/**
 *  A replication module describes all the functionality that PowerSync requires to
 *  replicate data from a DataSource. Whenever a new data source is added to powersync this class should be extended.
 */
export abstract class ReplicationModule extends modules.AbstractModule {
  protected type: string;

  protected replicationAdapters: Set<ReplicationAdapter>;

  /**
   * @protected
   * @param options
   */
  protected constructor(options: ReplicationModuleOptions) {
    super(options);
    this.type = options.type;
    this.replicationAdapters = new Set();
  }

  /**
   *  Create the API adapter for the DataSource required by the sync API
   *  endpoints.
   */
  protected abstract createSyncAPIAdapter(config: DataSourceConfig): api.RouteAPI;

  /**
   *  Create the ReplicationAdapter to be used by PowerSync replicator.
   */
  protected abstract createReplicationAdapter(config: DataSourceConfig): ReplicationAdapter;

  /**
   *  Return the TS codec schema describing the required configuration values for this module.
   */
  protected abstract configSchema(): t.AnyCodec;

  /**
   *  Register this module's replication adapters and sync API providers if the required configuration is present.
   */
  public async initialize(context: system.ServiceContext): Promise<void> {
    if (!context.configuration.connections) {
      // No data source configuration found in the config skip for now
      return;
    }

    const matchingConfig = context.configuration.connections.filter((dataSource) => dataSource.type === this.type);

    if (matchingConfig.length > 1) {
      logger.warning(
        `Multiple data sources of type ${this.type} found in the configuration. Only the first will be used.`
      );
    }

    try {
      const baseMatchingConfig = matchingConfig[0];
      const decodedConfig = this.configSchema().decode(baseMatchingConfig);
      // If validation fails, log the error and continue, no replication will happen for this data source
      this.validateConfig(matchingConfig[0]);
      const replicationAdapter = this.createReplicationAdapter(decodedConfig);
      this.replicationAdapters.add(replicationAdapter);
      context.replicationEngine.register(replicationAdapter);

      const apiAdapter = this.createSyncAPIAdapter(decodedConfig);
      context.routerEngine.registerAPI(apiAdapter);
    } catch (e) {
      logger.error(e);
    }
  }

  private validateConfig(config: DataSourceConfig): void {
    const validator = schema
      .parseJSONSchema(
        t.generateJSONSchema(this.configSchema(), { allowAdditional: true, parsers: [types.configFile.portParser] })
      )
      .validator();

    const valid = validator.validate(config);

    if (!valid.valid) {
      throw new Error(`Failed to validate Module ${this.name} configuration: ${valid.errors.join(', ')}`);
    }
  }
}
