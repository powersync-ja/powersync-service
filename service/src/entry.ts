import { container, ContainerImplementation, logger } from '@powersync/lib-services-framework';
import * as core from '@powersync/service-core';

import { CoreModule } from '@powersync/service-module-core';
import { startServer } from './runners/server.js';
import { startStreamRunner } from './runners/stream-worker.js';
import { startUnifiedRunner } from './runners/unified-runner.js';
import { createSentryReporter } from './util/alerting.js';
import { DYNAMIC_MODULES } from './util/modules.js';

// Initialize framework components
container.registerDefaults();
container.register(ContainerImplementation.REPORTER, createSentryReporter());

const moduleManager = new core.modules.ModuleManager();
moduleManager.register([new CoreModule()]);
moduleManager.registerDynamicModules(DYNAMIC_MODULES);
// This is a bit of a hack. Commands such as the teardown command or even migrations might
// want access to the ModuleManager in order to use modules
container.register(core.ModuleManager, moduleManager);

// This is nice to have to avoid passing it around
container.register(core.utils.CompoundConfigCollector, new core.utils.CompoundConfigCollector());

let traceServerPromise: Promise<core.BasicCdpTraceServer> | undefined;

function startTraceServer() {
  traceServerPromise ??= core
    .startBasicCdpTraceServer({
      host: process.env.POWERSYNC_TRACE_CDP_HOST ?? '127.0.0.1',
      port: process.env.POWERSYNC_TRACE_CDP_PORT ? Number(process.env.POWERSYNC_TRACE_CDP_PORT) : 9222
    })
    .then((server) => {
      logger.info(`Trace CDP server listening at ${server.url}`);
      logger.info(`Trace CDP WebSocket URL: ${server.webSocketDebuggerUrl}`);
      return server;
    })
    .catch((error) => {
      traceServerPromise = undefined;
      logger.error('Failed to start trace CDP server', error);
      throw error;
    });

  return traceServerPromise;
}

function withTraceServer(runner: core.utils.Runner): core.utils.Runner {
  return async (runnerConfig) => {
    await startTraceServer();
    await runner(runnerConfig);
  };
}

// Generate Commander CLI entry point program
const { execute } = core.entry.generateEntryProgram({
  [core.utils.ServiceRunner.API]: withTraceServer(startServer),
  [core.utils.ServiceRunner.SYNC]: withTraceServer(startStreamRunner),
  [core.utils.ServiceRunner.UNIFIED]: withTraceServer(startUnifiedRunner)
});

/**
 * Starts the program
 */
execute();
