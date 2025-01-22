import { Command } from 'commander';

import { teardown } from '../../runner/teardown.js';
import { extractRunnerOptions, wrapConfigCommand } from './config-command.js';
import { ErrorCode, ServiceError } from '@powersync/lib-services-framework';

const COMMAND_NAME = 'teardown';

export function registerTearDownAction(program: Command) {
  const teardownCommand = program.command(COMMAND_NAME);

  wrapConfigCommand(teardownCommand);

  return teardownCommand
    .argument('[ack]', 'Type `TEARDOWN` to confirm teardown should occur')
    .description('Terminate all replicating sync rules, clear remote configuration and remove all data')
    .action(async (ack, options) => {
      if (ack !== 'TEARDOWN') {
        throw new ServiceError(ErrorCode.PSYNC_S0102, 'TEARDOWN was not acknowledged.');
      }

      await teardown(extractRunnerOptions(options));
    });
}
