import { container, ContainerImplementation } from '@powersync/lib-services-framework';
import { entry, modules, utils } from '@powersync/service-core';
import PostgresModule from '@powersync/service-module-postgres';
import { startServer } from './runners/server.js';
import { startStreamWorker } from './runners/stream-worker.js';
import { createSentryReporter } from './util/alerting.js';

// Initialize framework components
container.registerDefaults();
container.register(ContainerImplementation.REPORTER, createSentryReporter());

const moduleManager = new modules.ModuleManager();
moduleManager.register([PostgresModule]);

// Generate Commander CLI entry point program
const { execute } = entry.generateEntryProgram(moduleManager, {
  [utils.ServiceRunner.API]: startServer,
  [utils.ServiceRunner.SYNC]: startStreamWorker,
  [utils.ServiceRunner.UNIFIED]: async (serviceContext) => {
    await startServer(serviceContext);
    await startStreamWorker(serviceContext);
  }
});

/**
 * Starts the program
 */
execute();
