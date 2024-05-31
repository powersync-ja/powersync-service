import { configFile } from '@powersync/service-types';
import { RunnerConfig, SyncRulesConfig } from '../types.js';

export abstract class SyncRulesCollector {
  abstract get name(): string;

  abstract collect(baseConfig: configFile.PowerSyncConfig, runnerConfig: RunnerConfig): Promise<SyncRulesConfig | null>;
}
