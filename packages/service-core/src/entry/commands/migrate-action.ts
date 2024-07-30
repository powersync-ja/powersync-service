import { logger } from '@powersync/lib-services-framework';
import { Command } from 'commander';

import * as migrations from '../../migrations/migrations-index.js';
import * as modules from '../../modules/modules-index.js';
import { extractRunnerOptions, wrapConfigCommand } from './config-command.js';

const COMMAND_NAME = 'migrate';

export function registerMigrationAction(program: Command, moduleManager: modules.ModuleManager) {
  const migrationCommand = program.command(COMMAND_NAME);

  wrapConfigCommand(migrationCommand);

  return migrationCommand
    .description('Run migrations')
    .argument('<direction>', 'Migration direction. `up` or `down`')
    .action(async (direction: migrations.Direction, options) => {
      await moduleManager.initialize(extractRunnerOptions(options));

      try {
        await migrations.migrate({
          direction,
          service_context: moduleManager.serviceContext
        });

        process.exit(0);
      } catch (e) {
        logger.error(`Migration failure`, e);
        process.exit(1);
      }
    });
}
