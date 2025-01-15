import { container, ContainerImplementation } from '@powersync/lib-services-framework';
import * as core from '@powersync/service-core';

import { MongoModule } from '@powersync/service-module-mongodb';
import { MongoStorageModule } from '@powersync/service-module-mongodb-storage';
import { MySQLModule } from '@powersync/service-module-mysql';
import { PostgresModule } from '@powersync/service-module-postgres';
import { PostgresStorageModule } from '@powersync/service-module-postgres-storage';
import { startServer } from './runners/server.js';
import { startStreamRunner } from './runners/stream-worker.js';
import { startUnifiedRunner } from './runners/unified-runner.js';
import { createSentryReporter } from './util/alerting.js';

// Initialize framework components
container.registerDefaults();
container.register(ContainerImplementation.REPORTER, createSentryReporter());

const moduleManager = new core.modules.ModuleManager();
moduleManager.register([
  new PostgresModule(),
  new MySQLModule(),
  new MongoModule(),
  new MongoStorageModule(),
  new PostgresStorageModule()
]);
// This is a bit of a hack. Commands such as the teardown command or even migrations might
// want access to the ModuleManager in order to use modules
container.register(core.ModuleManager, moduleManager);

// This is nice to have to avoid passing it around
container.register(core.utils.CompoundConfigCollector, new core.utils.CompoundConfigCollector());

// Generate Commander CLI entry point program
const { execute } = core.entry.generateEntryProgram({
  [core.utils.ServiceRunner.API]: startServer,
  [core.utils.ServiceRunner.SYNC]: startStreamRunner,
  [core.utils.ServiceRunner.UNIFIED]: startUnifiedRunner
});

/**
 * Starts the program
 */
execute();
