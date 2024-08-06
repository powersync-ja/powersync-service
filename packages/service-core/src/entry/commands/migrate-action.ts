import { logger } from '@powersync/lib-services-framework';
import { Command } from 'commander';

import * as migrations from '../../migrations/migrations-index.js';
import { extractRunnerOptions, wrapConfigCommand } from './config-command.js';

const COMMAND_NAME = 'migrate';

export function registerMigrationAction(program: Command) {
  const migrationCommand = program.command(COMMAND_NAME);

  wrapConfigCommand(migrationCommand);

  return migrationCommand
    .description('Run migrations')
    .argument('<direction>', 'Migration direction. `up` or `down`')
    .action(async (direction: migrations.Direction, options) => {
      try {
        await migrations.migrate({
          direction,
          runner_config: extractRunnerOptions(options)
        });

        process.exit(0);
      } catch (e) {
        logger.error(`Migration failure`, e);
        process.exit(1);
      }
    });
}
