import { RunnerConfig, SyncRulesConfig } from '../../types.js';
import { SyncRulesCollector } from '../sync-collector.js';
import { configFile } from '@powersync/service-types';

export class Base64SyncRulesCollector extends SyncRulesCollector {
  get name(): string {
    return 'Base64';
  }

  async collect(baseConfig: configFile.PowerSyncConfig, runnerConfig: RunnerConfig): Promise<SyncRulesConfig | null> {
    const { sync_rules_base64 } = runnerConfig;
    if (!sync_rules_base64) {
      return null;
    }

    return {
      present: true,
      content: Buffer.from(sync_rules_base64, 'base64').toString()
    };
  }
}
