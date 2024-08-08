import { Command } from 'commander';

import { teardown } from '../../runner/teardown.js';
import { extractRunnerOptions, wrapConfigCommand } from './config-command.js';

const COMMAND_NAME = 'teardown';

export function registerTearDownAction(program: Command) {
  const teardownCommand = program.command(COMMAND_NAME);

  wrapConfigCommand(teardownCommand);

  return teardownCommand
    .argument('[ack]', 'Type `TEARDOWN` to confirm teardown should occur')
    .description('Terminate all replicating sync rules, deleting the replication slots')
    .action(async (ack, options) => {
      if (ack !== 'TEARDOWN') {
        throw new Error('TEARDOWN was not acknowledged.');
      }

      await teardown(extractRunnerOptions(options));
    });
}
