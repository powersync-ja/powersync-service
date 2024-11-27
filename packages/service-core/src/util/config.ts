import * as fs from 'fs/promises';

import { container } from '@powersync/lib-services-framework';
import { ResolvedPowerSyncConfig, RunnerConfig } from './config/types.js';
import { CompoundConfigCollector } from './util-index.js';

/**
 * Loads the resolved config using the registered config collector
 */
export async function loadConfig(runnerConfig: RunnerConfig) {
  const collector = container.getImplementation(CompoundConfigCollector);
  return collector.collectConfig(runnerConfig);
}

export async function loadSyncRules(config: ResolvedPowerSyncConfig): Promise<string | undefined> {
  const sync_rules = config.sync_rules;
  if (sync_rules.content) {
    return sync_rules.content;
  } else if (sync_rules.path) {
    return await fs.readFile(sync_rules.path, 'utf-8');
  }
}
