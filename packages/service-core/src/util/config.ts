import * as fs from 'fs/promises';
import { baseUri } from '@powersync/service-types';

import { ResolvedConnection, ResolvedPowerSyncConfig, RunnerConfig } from './config/types.js';
import { CompoundConfigCollector } from './config/compound-config-collector.js';

/**
 * Build a single URI from full postgres credentials.
 */
export function buildDemoPgUri(options: ResolvedConnection): string {
  if (!options.debug_api) {
    throw new Error('Not supported');
  }

  const uri = new URL(baseUri(options));
  uri.username = options.username;
  uri.password = options.password;
  if (options.sslmode != 'disable') {
    // verify-full is tricky to actually use on a client, since they won't have the cert
    // Just use "require" by default
    // uri.searchParams.set('sslmode', options.sslmode);
    uri.searchParams.set('sslmode', 'require');
  }
  return uri.toString();
}

export function loadConfig(runnerConfig: RunnerConfig = {}) {
  const collector = new CompoundConfigCollector();
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
