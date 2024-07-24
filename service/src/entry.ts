import { container, ContainerImplementation } from '@powersync/lib-services-framework';
import { entry, modules, utils } from '@powersync/service-core';
import { startServer } from './runners/server.js';
import { startStreamWorker } from './runners/stream-worker.js';
import { createSentryReporter } from './util/alerting.js';

container.registerDefaults();
container.register(ContainerImplementation.REPORTER, createSentryReporter());

const moduleManager = new modules.ModuleManager();
container.register(modules.ModuleManager, moduleManager);

// TODO register Postgres module
moduleManager.register([]);

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
