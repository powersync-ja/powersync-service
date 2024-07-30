import { Command } from 'commander';

import { logger } from '@powersync/lib-services-framework';
import * as modules from '../modules/modules-index.js';
import * as utils from '../util/util-index.js';
import { registerMigrationAction } from './commands/migrate-action.js';
import { registerTearDownAction } from './commands/teardown-action.js';
import { registerCompactAction, registerStartAction } from './entry-index.js';

/**
 * Generates a Commander program which serves as the entry point
 * for the PowerSync service.
 * This registers standard actions for teardown and migrations.
 * Optionally registers the start command handlers.
 */
export function generateEntryProgram(
  moduleManager: modules.ModuleManager,
  startHandlers?: Record<utils.ServiceRunner, utils.Runner>
) {
  const entryProgram = new Command();
  entryProgram.name('powersync-runner').description('CLI to initiate a PowerSync service runner');

  registerTearDownAction(entryProgram);
  registerMigrationAction(entryProgram, moduleManager);
  registerCompactAction(entryProgram, moduleManager);

  if (startHandlers) {
    registerStartAction(entryProgram, moduleManager, startHandlers);
  }

  return {
    program: entryProgram,
    /**
     * Executes the main program. Ends the NodeJS process if an exception was caught.
     */
    execute: async function runProgram() {
      try {
        await entryProgram.parseAsync();
      } catch (e) {
        logger.error('Fatal error', e);
        process.exit(1);
      }
    }
  };
}
