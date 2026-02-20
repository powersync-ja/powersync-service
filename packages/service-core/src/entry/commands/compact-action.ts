import { Command } from 'commander';

import { container, logger } from '@powersync/lib-services-framework';
import * as v8 from 'v8';
import * as system from '../../system/system-index.js';
import * as utils from '../../util/util-index.js';

import { modules } from '../../index.js';
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
  const compactCommand = program
    .command(COMMAND_NAME)
    .option(`-b, --buckets [buckets]`, 'Full bucket names, comma-separated (e.g., "global[],mybucket[\\"user1\\"]")');

  wrapConfigCommand(compactCommand);

  return compactCommand.description('Compact storage').action(async (options) => {
    const buckets = options.buckets
      ?.split(',')
      .map((b: string) => b.trim())
      .filter(Boolean);
    if (buckets) {
      const invalid = buckets.filter((b: string) => !b.includes('['));
      if (invalid.length > 0) {
        logger.error(
          `Invalid bucket names: ${invalid.join(', ')}. ` +
            `Pass full bucket names (e.g., "global[]"), not bucket definition names (e.g., "global").`
        );
        process.exit(1);
      }
    }
    if (buckets == null) {
      logger.info('Compacting storage for all buckets...');
    } else {
      logger.info(`Compacting storage for ${buckets?.join(', ')}...`);
    }
    const config = await utils.loadConfig(extractRunnerOptions(options));
    const serviceContext = new system.ServiceContextContainer({
      serviceMode: system.ServiceContextMode.COMPACT,
      configuration: config
    });

    // Register modules in order to allow custom module compacting
    const moduleManager = container.getImplementation(modules.ModuleManager);
    await moduleManager.initialize(serviceContext);

    logger.info('Connecting to storage...');

    try {
      // Start the storage engine in order to create the appropriate BucketStorage
      await serviceContext.lifeCycleEngine.start();
      const bucketStorage = serviceContext.storageEngine.activeBucketStorage;

      const active = await bucketStorage.getActiveStorage();
      if (active == null) {
        logger.info('No active instance to compact');
        return;
      }
      logger.info('Performing compaction...');
      if (buckets != null) {
        await active.compact({
          memoryLimitMB: COMPACT_MEMORY_LIMIT_MB,
          compactBuckets: buckets,
          compactParameterData: false
        });
      } else {
        await active.compact({ memoryLimitMB: COMPACT_MEMORY_LIMIT_MB, compactParameterData: true });
      }
      logger.info('Successfully compacted storage.');
    } catch (e) {
      logger.error(`Failed to compact: ${e.toString()}`);
      // Indirectly triggers lifeCycleEngine.stop
      process.exit(1);
    } finally {
      // Indirectly triggers lifeCycleEngine.stop
      process.exit(0);
    }
  });
}
