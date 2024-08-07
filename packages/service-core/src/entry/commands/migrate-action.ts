import { Command } from 'commander';

import { extractRunnerOptions, wrapConfigCommand } from './config-command.js';
import { migrate } from '../../migrations/migrations.js';
import { Direction } from '../../migrations/definitions.js';
import { logger } from '@powersync/lib-services-framework';

const COMMAND_NAME = 'migrate';

export function registerMigrationAction(program: Command) {
  const migrationCommand = program.command(COMMAND_NAME);

  wrapConfigCommand(migrationCommand);

  return migrationCommand
    .description('Run migrations')
    .argument('<direction>', 'Migration direction. `up` or `down`')
    .action(async (direction: Direction, options) => {
      const runnerConfig = extractRunnerOptions(options);

      try {
        await migrate({
          direction,
          runner_config: runnerConfig
        });

        process.exit(0);
      } catch (e) {
        logger.error(`Migration failure`, e);
        process.exit(1);
      }
    });
}
