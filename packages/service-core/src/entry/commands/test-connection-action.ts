import { Command } from 'commander';

import { container, logger } from '@powersync/lib-services-framework';
import * as system from '../../system/system-index.js';
import * as utils from '../../util/util-index.js';

import { modules, ReplicationEngine } from '../../index.js';
import { extractRunnerOptions, wrapConfigCommand } from './config-command.js';

const COMMAND_NAME = 'test-connection';

export function registerTestConnectionAction(program: Command) {
  const testConnectionCommand = program.command(COMMAND_NAME);

  wrapConfigCommand(testConnectionCommand);

  return testConnectionCommand.description('Test connection').action(async (options) => {
    try {
      const config = await utils.loadConfig(extractRunnerOptions(options));
      const serviceContext = new system.ServiceContextContainer({
        serviceMode: system.ServiceContextMode.TEST_CONNECTION,
        configuration: config
      });

      const replication = new ReplicationEngine();
      serviceContext.register(ReplicationEngine, replication);

      // Register modules in order to load the correct config
      const moduleManager = container.getImplementation(modules.ModuleManager);
      await moduleManager.initialize(serviceContext);

      // Start the storage engine in order to create the appropriate BucketStorage
      await serviceContext.lifeCycleEngine.start();

      logger.info('Testing connection...');
      const results = await replication.testConnection();
      logger.info(`Connection succeeded to ${results.map((r) => r.connectionDescription).join(', ')}`);
      process.exit(0);
    } catch (e) {
      logger.error(`Connection failed: ${e.message}`);
      process.exit(1);
    }
  });
}
