import { logger } from '@powersync/lib-services-framework';
import { CompiledSyncConfig, updateSyncRulesFromYaml } from '../storage/BucketStorageFactory.js';
import { ConfigurationFileSyncRulesProvider } from '../util/config/sync-rules/sync-rules-provider.js';
import { SyncRulesConfig } from '../util/config/types.js';

/**
 * A snapshot loaded by the core module once per service context. Consumers share the same
 * content and validation results until restart. Configured content can come from a file,
 * Base64, or inline YAML; a read failure is distinct from API-managed mode.
 */
export type LoadedSyncConfig =
  | { mode: 'api' }
  | { mode: 'unavailable'; error: unknown }
  | {
      mode: 'configured';
      /**
       * Absent when configured mode is reserved before the first deployment supplies content.
       */
      config?: CompiledSyncConfig;
      versionLabel: string | undefined;
      exitOnError: boolean;
    };

export async function loadSyncConfig(configuration: SyncRulesConfig): Promise<LoadedSyncConfig> {
  // The configured source determines deployment ownership, even when it cannot be loaded.
  const source = configuration.source ?? (configuration.present ? 'config' : 'api');
  if (source == 'api') {
    return { mode: 'api' };
  }
  if (!configuration.present) {
    return { mode: 'configured', versionLabel: configuration.version_label, exitOnError: configuration.exit_on_error };
  }
  let content: string | undefined;
  try {
    content = await new ConfigurationFileSyncRulesProvider(configuration).get();
    if (content == null) {
      throw new Error('Sync config is configured but no content or file path is available');
    }
  } catch (error) {
    logger.warn('Unable to read local sync config', error);
    return { mode: 'unavailable', error };
  }
  // Validate independently of source connectivity, and let replication enforce exit_on_error.
  const { config } = updateSyncRulesFromYaml(content, { validate: false });
  return {
    mode: 'configured',
    config,
    versionLabel: configuration.version_label,
    exitOnError: configuration.exit_on_error
  };
}
