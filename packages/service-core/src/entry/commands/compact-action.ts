import { Command } from 'commander';

import { extractRunnerOptions, wrapConfigCommand } from './config-command.js';
import { logger } from '@powersync/lib-services-framework';
import { loadConfig } from '../../util/config.js';
import { mongo } from '../../db/db-index.js';
import { MongoBucketStorage, MongoSyncBucketStorage, PowerSyncMongo } from '../../storage/storage-index.js';

const COMMAND_NAME = 'compact';

export function registerCompactAction(program: Command) {
  const compactCommand = program.command(COMMAND_NAME);

  wrapConfigCommand(compactCommand);

  return compactCommand.description('Compact storage').action(async (options) => {
    const runnerConfig = extractRunnerOptions(options);

    const config = await loadConfig(runnerConfig);
    const { storage } = config;
    const client = mongo.createMongoClient(storage);
    await client.connect();
    try {
      const psdb = new PowerSyncMongo(client);
      const bucketStorage = new MongoBucketStorage(psdb, { slot_name_prefix: config.slot_name_prefix });
      const active = await bucketStorage.getActiveSyncRules();
      if (active == null) {
        logger.info('No active instance to compact');
        return;
      }
      const p = bucketStorage.getInstance(active);
      await p.compact({});
      logger.info('done');
    } finally {
      await client.close();
    }
  });
}
