import { container, logger, migrations } from '@powersync/lib-services-framework';
import { Command } from 'commander';

import * as modules from '../../modules/modules-index.js';
import * as system from '../../system/system-index.js';
import * as utils from '../../util/util-index.js';
import { extractRunnerOptions, wrapConfigCommand } from './config-command.js';

const COMMAND_NAME = 'migrate';

export function registerMigrationAction(program: Command) {
  const migrationCommand = program.command(COMMAND_NAME);

  wrapConfigCommand(migrationCommand);

  return migrationCommand
    .description('Run migrations')
    .argument('<direction>', 'Migration direction. `up` or `down`')
    .action(async (direction: migrations.Direction, options) => {
      const config = await utils.loadConfig(extractRunnerOptions(options));
      const serviceContext = new system.ServiceContextContainer({
        mode: system.ServiceContextMode.MIGRATION,
        configuration: config
      });

      // Register modules in order to allow custom module migrations
      const moduleManager = container.getImplementation(modules.ModuleManager);
      await moduleManager.initialize(serviceContext);

      try {
        await serviceContext.migrations.migrate({
          direction,
          // Give the migrations access to the service context
          migrationContext: {
            service_context: serviceContext
          }
        });

        await serviceContext.migrations[Symbol.asyncDispose]();
        process.exit(0);
      } catch (e) {
        logger.error(`Migration failure`, e);
        process.exit(1);
      }
    });
}
