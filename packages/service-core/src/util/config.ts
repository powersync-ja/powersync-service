import * as fs from 'fs/promises';
import winston from 'winston';

import { container, logger, LogFormat, DEFAULT_LOG_LEVEL, DEFAULT_LOG_FORMAT } from '@powersync/lib-services-framework';
import { configFile } from '@powersync/service-types';
import { ResolvedPowerSyncConfig, RunnerConfig } from './config/types.js';
import { CompoundConfigCollector } from './util-index.js';

export function configureLogger(config?: configFile.LoggingConfig): void {
  const level = process.env.PS_LOG_LEVEL ?? config?.level ?? DEFAULT_LOG_LEVEL;
  const format =
    (process.env.PS_LOG_FORMAT as configFile.LoggingConfig['format']) ?? config?.format ?? DEFAULT_LOG_FORMAT;
  const winstonFormat = format === 'json' ? LogFormat.production : LogFormat.development;

  // We want the user to always be aware that a log level was configured (they might forget they set it in the config and wonder why they aren't seeing logs)
  // We log this using the configured format, but before we configure the level.
  logger.configure({ level: DEFAULT_LOG_LEVEL, format: winstonFormat, transports: [new winston.transports.Console()] });
  logger.info(`Configured logger with level "${level}" and format "${format}"`);

  logger.configure({ level, format: winstonFormat, transports: [new winston.transports.Console()] });
}

/**
 * Loads the resolved config using the registered config collector
 */
export async function loadConfig(runnerConfig: RunnerConfig) {
  const collector = container.getImplementation(CompoundConfigCollector);
  const config = await collector.collectConfig(runnerConfig);
  configureLogger(config.base_config.system?.logging);
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
