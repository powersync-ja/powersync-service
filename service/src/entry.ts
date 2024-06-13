import '@journeyapps-platform/micro/register';
import './util/register-alerting.js';

import winston from 'winston';
import { entry, utils } from '@powersync/service-core';
import * as framework from '@powersync/service-framework';

// Configure logging to console
framework.logger.configure({
  format: utils.env.NODE_ENV == 'production' ? framework.Logger.production_format : framework.Logger.development_format,
  transports: [new winston.transports.Console()]
});

import { startServer } from './runners/server.js';
import { startStreamWorker } from './runners/stream-worker.js';

// Generate Commander CLI entry point program
const { execute } = entry.generateEntryProgram({
  [utils.ServiceRunner.API]: startServer,
  [utils.ServiceRunner.SYNC]: startStreamWorker,
  [utils.ServiceRunner.UNIFIED]: async (config: utils.RunnerConfig) => {
    await startServer(config);
    await startStreamWorker(config);
  }
});

/**
 * Starts the program
 */
execute();
