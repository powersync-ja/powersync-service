import { Command } from 'commander';

import { logger } from '@powersync/lib-services-framework';
import * as v8 from 'v8';
import { createPowerSyncMongo, MongoBucketStorage } from '../../storage/storage-index.js';
import { loadConfig } from '../../util/config.js';
import { extractRunnerOptions, wrapConfigCommand } from './config-command.js';

const COMMAND_NAME = 'compact';

/**
 * Approximately max-old-space-size + 64MB.
 */
const HEAP_LIMIT = v8.getHeapStatistics().heap_size_limit;

/**
 * Subtract 128MB for process overhead.
 *
 * Limit to 1024MB overall.
 */
const COMPACT_MEMORY_LIMIT_MB = Math.min(HEAP_LIMIT / 1024 / 1024 - 128, 1024);

export function registerCompactAction(program: Command) {
  const compactCommand = program.command(COMMAND_NAME);

  wrapConfigCommand(compactCommand);

  return compactCommand.description('Compact storage').action(async (options) => {
    const runnerConfig = extractRunnerOptions(options);

    const config = await loadConfig(runnerConfig);
    const { storage } = config;
    const psdb = createPowerSyncMongo(storage);
    const client = psdb.client;
    await client.connect();
    try {
      const bucketStorage = new MongoBucketStorage(psdb, { slot_name_prefix: config.slot_name_prefix });
      const active = await bucketStorage.getActiveSyncRules();
      if (active == null) {
        logger.info('No active instance to compact');
        return;
      }
      const p = bucketStorage.getInstance(active);
      await p.compact({ memoryLimitMB: COMPACT_MEMORY_LIMIT_MB });
      logger.info('done');
    } catch (e) {
      logger.error(`Failed to compact: ${e.toString()}`);
      process.exit(1);
    } finally {
      await client.close();
      process.exit(0);
    }
  });
}
