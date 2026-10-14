import { logger } from '@powersync/lib-services-framework';
import { DEFAULT_TAG, SqlSyncRules, SyncRulesErrors, YamlError } from '@powersync/service-sync-rules';
import { CompiledSyncConfig, updateSyncRulesFromConfig } from '../../../storage/BucketStorageFactory.js';
import type { ResolvedPowerSyncConfig, SyncRulesConfig } from '../types.js';
import { ConfigurationFileSyncRulesProvider } from './sync-rules-provider.js';

/**
 * A snapshot loaded once during service configuration resolution. Consumers share the same
 * content and validation results until restart. Configured content can come from a file,
 * Base64, or inline YAML; a read failure is distinct from API-managed mode.
 */
export type LoadedSyncConfig =
  | {
      /**
       * Precompiled with a placeholder base schema so configuration and service consumers can
       * share validation results and storage preparation without parsing the YAML again.
       * Do not use the parsed config directly for replication evaluation: replication jobs load
       * the persisted config with their source-specific default schema, preparing the stored plan
       * or reparsing legacy YAML at that point.
       */
      compiled: CompiledSyncConfig;
    }
  | { error: unknown };

export async function loadSyncConfig(
  configuration: SyncRulesConfig,
  connections?: ResolvedPowerSyncConfig['connections']
): Promise<LoadedSyncConfig | undefined> {
  if (!configuration.present) {
    return undefined;
  }
  let content: string | undefined;
  try {
    content = await new ConfigurationFileSyncRulesProvider(configuration).get();
    if (content == null) {
      throw new Error('Sync config is configured but no content or file path is available');
    }
  } catch (error) {
    logger.warn('Unable to read local sync config', error);
    return { error };
  }
  // Validate independently of source connectivity, and let replication enforce exit_on_error.
  let config: CompiledSyncConfig;
  try {
    config = updateSyncRulesFromConfig(
      SqlSyncRules.fromYaml(content, {
        defaultSchema: 'not_applicable',
        throwOnError: false,
        connections:
          connections &&
          Object.fromEntries(
            connections.map((connection) => [connection.tag ?? DEFAULT_TAG, { type: connection.type }])
          )
      })
    ).config;
  } catch (error) {
    // Some malformed inputs throw even with validation disabled. Preserve the rejected YAML
    // and expose the failure through diagnostics, leaving exit_on_error to the replicator.
    const errors =
      error instanceof SyncRulesErrors
        ? error.errors
        : [new YamlError(error instanceof Error ? error : new Error(String(error)))];
    config = { yaml: content, plan: null, parsed: { config: new SqlSyncRules(content), errors } };
  }
  return { compiled: config };
}
