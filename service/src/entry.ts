import { entry, utils } from '@powersync/service-core';
import { startServer } from './runners/server.js';
import { startStreamWorker } from './runners/stream-worker.js';
import { container, ContainerImplementation } from '@powersync/lib-services-framework';
import { createSentryReporter } from './util/alerting.js';

container.registerDefaults();
container.register(ContainerImplementation.REPORTER, createSentryReporter());

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
