import { configFile } from '@powersync/service-types';
import { SyncRulesConfig } from '../../types.js';
import { SyncRulesCollector } from '../sync-collector.js';

export class InlineSyncRulesCollector extends SyncRulesCollector {
  get name(): string {
    return 'Inline';
  }

  async collect(baseConfig: configFile.PowerSyncConfig): Promise<SyncRulesConfig | null> {
    const content = baseConfig?.sync_config?.content;
    if (!content) {
      return null;
    }

    return {
      present: true,
      exit_on_error: baseConfig.sync_config?.exit_on_error ?? true,
      ...baseConfig.sync_config
    };
  }
}
