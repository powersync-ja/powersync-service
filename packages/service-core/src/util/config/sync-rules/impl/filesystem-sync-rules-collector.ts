import * as path from 'path';
import { RunnerConfig, SyncRulesConfig } from '../../types.js';
import { SyncRulesCollector } from '../sync-collector.js';
import { configFile } from '@powersync/service-types';

export class FileSystemSyncRulesCollector extends SyncRulesCollector {
  get name(): string {
    return 'FileSystem';
  }

  async collect(baseConfig: configFile.PowerSyncConfig, runnerConfig: RunnerConfig): Promise<SyncRulesConfig | null> {
    const sync_path = baseConfig.sync_rules?.path;
    if (!sync_path) {
      return null;
    }

    const { config_path } = runnerConfig;

    // Depending on the container, the sync rules may not actually be present.
    // Only persist the path here, and load on demand using `loadSyncRules()`.
    return {
      present: true,
      path: config_path ? path.resolve(path.dirname(config_path), sync_path) : sync_path
    };
  }
}
