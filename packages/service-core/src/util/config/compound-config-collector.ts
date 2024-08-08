import { logger } from '@powersync/lib-services-framework';
import { configFile } from '@powersync/service-types';
import * as auth from '../../auth/auth-index.js';
import { ConfigCollector } from './collectors/config-collector.js';
import { Base64ConfigCollector } from './collectors/impl/base64-config-collector.js';
import { FallbackConfigCollector } from './collectors/impl/fallback-config-collector.js';
import { FileSystemConfigCollector } from './collectors/impl/filesystem-config-collector.js';
import { Base64SyncRulesCollector } from './sync-rules/impl/base64-sync-rules-collector.js';
import { FileSystemSyncRulesCollector } from './sync-rules/impl/filesystem-sync-rules-collector.js';
import { InlineSyncRulesCollector } from './sync-rules/impl/inline-sync-rules-collector.js';
import { SyncRulesCollector } from './sync-rules/sync-collector.js';
import { ResolvedPowerSyncConfig, RunnerConfig, SyncRulesConfig } from './types.js';

export type CompoundConfigCollectorOptions = {
  /**
   * Collectors for PowerSync configuration content.
   * The configuration from first collector to provide a configuration
   * is used. The order of the collectors specifies precedence
   */
  configCollectors: ConfigCollector[];
  /**
   * Collectors for PowerSync sync rules content.
   * The configuration from first collector to provide a configuration
   * is used. The order of the collectors specifies precedence
   */
  syncRulesCollectors: SyncRulesCollector[];
};

export type ConfigCollectedEvent = {
  base_config: configFile.PowerSyncConfig;
  resolved_config: ResolvedPowerSyncConfig;
};

export type ConfigCollectorListener = {
  configCollected?: (event: ConfigCollectedEvent) => Promise<void>;
};

const POWERSYNC_DEV_KID = 'powersync-dev';

const DEFAULT_COLLECTOR_OPTIONS: CompoundConfigCollectorOptions = {
  configCollectors: [new Base64ConfigCollector(), new FileSystemConfigCollector(), new FallbackConfigCollector()],
  syncRulesCollectors: [
    new Base64SyncRulesCollector(),
    new FileSystemSyncRulesCollector(),
    new InlineSyncRulesCollector()
  ]
};

export class CompoundConfigCollector {
  constructor(protected options: CompoundConfigCollectorOptions = DEFAULT_COLLECTOR_OPTIONS) {}

  /**
   * Collects and resolves base config
   */
  async collectConfig(runnerConfig: RunnerConfig = {}): Promise<ResolvedPowerSyncConfig> {
    const baseConfig = await this.collectBaseConfig(runnerConfig);

    const dataSources = baseConfig.replication?.connections ?? [];
    if (dataSources.length > 1) {
      throw new Error('Only a single replication data source is supported currently');
    }

    const collectors = new auth.CompoundKeyCollector();
    const keyStore = new auth.KeyStore(collectors);

    const inputKeys = baseConfig.client_auth?.jwks?.keys ?? [];
    const staticCollector = await auth.StaticKeyCollector.importKeys(inputKeys);

    collectors.add(staticCollector);

    let jwks_uris = baseConfig.client_auth?.jwks_uri ?? [];
    if (typeof jwks_uris == 'string') {
      jwks_uris = [jwks_uris];
    }

    for (let uri of jwks_uris) {
      collectors.add(
        new auth.CachedKeyCollector(
          new auth.RemoteJWKSCollector(uri, { block_local_ip: !!baseConfig.client_auth?.block_local_jwks })
        )
      );
    }

    const baseDevKey = (baseConfig.client_auth?.jwks?.keys ?? []).find((key) => key.kid == POWERSYNC_DEV_KID);

    let devKey: auth.KeySpec | undefined;
    if (baseConfig.dev?.demo_auth && baseDevKey != null && baseDevKey.kty == 'oct') {
      devKey = await auth.KeySpec.importKey(baseDevKey);
    }

    const sync_rules = await this.collectSyncRules(baseConfig, runnerConfig);

    let jwt_audiences: string[] = baseConfig.client_auth?.audience ?? [];

    let config: ResolvedPowerSyncConfig = {
      base_config: baseConfig,
      connections: baseConfig.replication?.connections || [],
      storage: baseConfig.storage,
      client_keystore: keyStore,
      // Dev tokens only use the static keys, no external key sources
      // We may restrict this even further to only the powersync-dev key.
      dev_client_keystore: new auth.KeyStore(staticCollector),
      api_tokens: baseConfig.api?.tokens ?? [],
      dev: {
        demo_auth: baseConfig.dev?.demo_auth ?? false,
        demo_client: baseConfig.dev?.demo_client ?? false,
        demo_password: baseConfig.dev?.demo_password,
        crud_api: baseConfig.dev?.crud_api ?? false,
        dev_key: devKey
      },
      port: baseConfig.port ?? 8080,
      sync_rules,
      jwt_audiences,

      token_max_expiration: '1d', // 1 day
      metadata: baseConfig.metadata ?? {},
      migrations: baseConfig.migrations,
      telemetry: {
        disable_telemetry_sharing: baseConfig.telemetry?.disable_telemetry_sharing ?? false,
        internal_service_endpoint:
          baseConfig.telemetry?.internal_service_endpoint ?? 'https://pulse.journeyapps.com/v1/metrics'
      },
      // TODO maybe move this out of the connection or something
      // slot_name_prefix: connections[0]?.slot_name_prefix ?? 'powersync_'
      slot_name_prefix: 'powersync_'
    };

    return config;
  }

  /**
   * Collects the base PowerSyncConfig from various registered collectors.
   * @throws if no collector could return a configuration.
   */
  protected async collectBaseConfig(runner_config: RunnerConfig): Promise<configFile.PowerSyncConfig> {
    for (const collector of this.options.configCollectors) {
      try {
        const baseConfig = await collector.collect(runner_config);
        if (baseConfig) {
          return baseConfig;
        }
        logger.debug(
          `Could not collect PowerSync config with ${collector.name} method. Moving on to next method if available.`
        );
      } catch (ex) {
        // An error in a collector is a hard stop
        throw new Error(`Could not collect config using ${collector.name} method. Caught exception: ${ex}`);
      }
    }
    throw new Error('PowerSyncConfig could not be collected using any of the registered config collectors.');
  }

  protected async collectSyncRules(
    baseConfig: configFile.PowerSyncConfig,
    runnerConfig: RunnerConfig
  ): Promise<SyncRulesConfig> {
    for (const collector of this.options.syncRulesCollectors) {
      try {
        const config = await collector.collect(baseConfig, runnerConfig);
        if (config) {
          return config;
        }
        logger.debug(
          `Could not collect sync rules with ${collector.name} method. Moving on to next method if available.`
        );
      } catch (ex) {
        // An error in a collector is a hard stop
        throw new Error(`Could not collect sync rules using ${collector.name} method. Caught exception: ${ex}`);
      }
    }
    return {
      present: false
    };
  }
}
