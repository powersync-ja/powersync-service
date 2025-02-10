import { configFile } from '@powersync/service-types';
import { PowerSyncConfig } from '@powersync/service-types/src/config/PowerSyncConfig.js';
import { CompoundKeyCollector } from '../../auth/CompoundKeyCollector.js';
import { KeySpec } from '../../auth/KeySpec.js';
import { KeyStore } from '../../auth/KeyStore.js';

export enum ServiceRunner {
  UNIFIED = 'unified',
  API = 'api',
  SYNC = 'sync'
}

export type RunnerConfig = {
  config_path?: string;
  config_base64?: string;
  sync_rules_base64?: string;
};

export type MigrationContext = {
  runner_config: RunnerConfig;
};

export type Runner = (config: RunnerConfig) => Promise<void>;

export type SyncRulesConfig = {
  present: boolean;
  content?: string;
  path?: string;
  exit_on_error: boolean;
};

export type ResolvedPowerSyncConfig = {
  base_config: PowerSyncConfig;
  connections?: configFile.GenericDataSourceConfig[];
  storage: configFile.GenericStorageConfig;
  dev: {
    demo_auth: boolean;
    /**
     * Only present when demo_auth == true
     */
    dev_key?: KeySpec;
  };
  client_keystore: KeyStore<CompoundKeyCollector>;
  /**
   * Keystore for development tokens.
   */
  dev_client_keystore: KeyStore;
  port: number;
  sync_rules: SyncRulesConfig;
  api_tokens: string[];
  jwt_audiences: string[];
  token_max_expiration: string;
  metadata: Record<string, string>;
  migrations?: {
    disable_auto_migration?: boolean;
  };

  telemetry: {
    disable_telemetry_sharing: boolean;
    internal_service_endpoint: string;
  };

  /** Prefix for postgres replication slot names. May eventually be connection-specific. */
  slot_name_prefix: string;
  parameters: Record<string, number | string | boolean | null>;
};
