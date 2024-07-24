import * as fs from 'fs/promises';

import { container } from '@powersync/lib-services-framework';
import { ServiceContext } from '../system/ServiceContext.js';
import { CompoundConfigCollector } from './config/compound-config-collector.js';
import { ResolvedPowerSyncConfig, RunnerConfig } from './config/types.js';

export async function loadConfig(runnerConfig: RunnerConfig = {}) {
  const collector = new CompoundConfigCollector();
  const config = await collector.collectConfig(runnerConfig);
  const serviceContext = new ServiceContext(config);
  container.register(ServiceContext, serviceContext);
  await serviceContext.initialize();
  return config;
}

export async function loadSyncRules(config: ResolvedPowerSyncConfig): Promise<string | undefined> {
  const sync_rules = config.sync_rules;
  if (sync_rules.content) {
    return sync_rules.content;
  } else if (sync_rules.path) {
    return await fs.readFile(sync_rules.path, 'utf-8');
  }
}
