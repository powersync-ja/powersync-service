import { SyncRulesConfig } from '../../types.js';
import { SyncRulesCollector } from '../sync-collector.js';
import { configFile } from '@powersync/service-types';

export class InlineSyncRulesCollector extends SyncRulesCollector {
  get name(): string {
    return 'Inline';
  }

  async collect(baseConfig: configFile.PowerSyncConfig): Promise<SyncRulesConfig | null> {
    const content = baseConfig.sync_rules?.content;
    if (!content) {
      return null;
    }

    return {
      present: true,
      ...baseConfig.sync_rules
    };
  }
}
