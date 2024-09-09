import { DataSourceConfig } from '@powersync/service-types/dist/config/PowerSyncConfig.js';
import * as t from 'ts-codec';

import * as types from '@powersync/service-types';
import * as api from '../api/api-index.js';
import * as modules from '../modules/modules-index.js';
import * as system from '../system/system-index.js';
import { schema } from '@powersync/lib-services-framework';
import { AbstractReplicator } from './AbstractReplicator.js';

export interface ReplicationModuleOptions extends modules.AbstractModuleOptions {
  type: string;
  configSchema: t.AnyCodec;
}

/**
 *  A replication module describes all the functionality that PowerSync requires to
 *  replicate data from a DataSource. Whenever a new data source is added to powersync this class should be extended.
 */
export abstract class ReplicationModule<TConfig extends DataSourceConfig> extends modules.AbstractModule {
  protected type: string;
  protected configSchema: t.AnyCodec;
  protected decodedConfig: TConfig | undefined;

  /**
   * @protected
   * @param options
   */
  protected constructor(options: ReplicationModuleOptions) {
    super(options);
    this.type = options.type;
    this.configSchema = options.configSchema;
  }

  /**
   *  Create the RouteAPI adapter for the DataSource required to service the sync API
   *  endpoints.
   */
  protected abstract createRouteAPIAdapter(): api.RouteAPI;

  /**
   *  Create the Replicator to be used by the ReplicationEngine.
   */
  protected abstract createReplicator(context: system.ServiceContext): AbstractReplicator;

  /**
   *  Register this module's Replicators and RouteAPI adapters if the required configuration is present.
   */
  public async initialize(context: system.ServiceContext): Promise<void> {
    if (!context.configuration.connections) {
      // No data source configuration found in the config skip for now
      return;
    }

    const matchingConfig = context.configuration.connections.filter((dataSource) => dataSource.type === this.type);
    if (!matchingConfig.length) {
      // This module is needed given the config
      return;
    }

    if (!matchingConfig.length) {
      return;
    }

    if (matchingConfig.length > 1) {
      this.logger.warning(
        `Multiple data sources of type ${this.type} found in the configuration. Only the first will be used.`
      );
    }

    try {
      const baseMatchingConfig = matchingConfig[0] as TConfig;
      // If validation fails, log the error and continue, no replication will happen for this data source
      this.validateConfig(baseMatchingConfig);
      this.decodedConfig = this.configSchema.decode(baseMatchingConfig);

      context.replicationEngine?.register(this.createReplicator(context));
      context.routerEngine?.registerAPI(this.createRouteAPIAdapter());
    } catch (e) {
      this.logger.error('Failed to initialize.', e);
    }
  }

  private validateConfig(config: TConfig): void {
    const validator = schema
      .parseJSONSchema(
        // This generates a schema for the encoded form of the codec
        t.generateJSONSchema(this.configSchema, {
          allowAdditional: true,
          parsers: [types.configFile.portParser]
        })
      )
      .validator();

    const valid = validator.validate(config);

    if (!valid.valid) {
      throw new Error(`Failed to validate Module ${this.name} configuration: ${valid.errors.join(', ')}`);
    }
  }

  protected getDefaultId(dataSourceName: string): string {
    return `${this.type}-${dataSourceName}`;
  }
}
