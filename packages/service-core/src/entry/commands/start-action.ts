import { Command } from 'commander';

import * as system from '../../system/system-index.js';
import * as utils from '../../util/util-index.js';
import { extractRunnerOptions, wrapConfigCommand } from './config-command.js';

const COMMAND_NAME = 'start';

export function registerStartAction(
  program: Command,
  serviceContext: system.ServiceContext,
  handlers: Record<utils.ServiceRunner, utils.Runner>
) {
  const startCommand = program.command(COMMAND_NAME);

  wrapConfigCommand(startCommand);

  return startCommand
    .description('Starts a PowerSync service runner.')
    .option(
      `-r, --runner-type [${Object.values(utils.ServiceRunner).join('|')}]`,
      'Type of runner to start. Defaults to unified runner.',
      utils.env.PS_RUNNER_TYPE
    )
    .action(async (options) => {
      // load the config
      await serviceContext.initialize(extractRunnerOptions(options));

      const runner = handlers[options.runnerType as utils.ServiceRunner];
      await runner(serviceContext);
    });
}
