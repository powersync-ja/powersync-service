import { Command } from 'commander';
import { Direction } from '@journeyapps-platform/micro-migrate';

import { extractRunnerOptions, wrapConfigCommand } from './config-command.js';
import { migrate } from '../../migrations/migrations.js';

const COMMAND_NAME = 'migrate';

export function registerMigrationAction(program: Command) {
  const migrationCommand = program.command(COMMAND_NAME);

  wrapConfigCommand(migrationCommand);

  return migrationCommand
    .description('Run migrations')
    .argument('<direction>', 'Migration direction. `up` or `down`')
    .action(async (direction: Direction, options) => {
      const runnerConfig = extractRunnerOptions(options);

      await migrate({
        direction,
        runner_config: runnerConfig
      });
    });
}
