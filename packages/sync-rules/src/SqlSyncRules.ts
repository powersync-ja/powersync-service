import { YamlError } from './errors.js';
import { parseSyncConfig, validateSyncConfig } from './from_yaml.js';
import { SyncConfig, SyncConfigWithErrors } from './SyncConfig.js';
import { SyncConfigParseOptions } from './types.js';

/**
 * @deprecated Use {@link SyncConfigParseOptions} instead.
 */
export type SyncRulesOptions = SyncConfigParseOptions;

/**
 * The {@link SyncConfig} implementation for legacy Sync Rules (`bucket_definitions`) and the Sync Streams alpha
 * (`config: edition: 2`). The parser also returns an empty instance when the YAML can't be parsed.
 *
 * @deprecated Legacy Sync Rules (`bucket_definitions`) implementation, kept for backwards compatibility until the next
 * major version. To parse or validate a sync config, use {@link parseSyncConfig} or {@link validateSyncConfig}.
 */
export class SqlSyncRules extends SyncConfig {
  /**
   * @deprecated Use {@link validateSyncConfig} instead.
   */
  static validate(yaml: string, options: SyncConfigParseOptions): YamlError[] {
    return validateSyncConfig(yaml, options);
  }

  /**
   * @deprecated Use {@link parseSyncConfig} instead.
   */
  static fromYaml(yaml: string, options: SyncConfigParseOptions): SyncConfigWithErrors {
    return parseSyncConfig(yaml, options);
  }
}
