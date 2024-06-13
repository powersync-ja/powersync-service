import { Direction } from '@journeyapps-platform/micro-migrate';
import { migrations, replication, utils, Metrics, logger } from '@powersync/service-core';

import { PowerSyncSystem } from '../system/PowerSyncSystem.js';

export async function startStreamWorker(runnerConfig: utils.RunnerConfig) {
  logger.info('Booting');

  const config = await utils.loadConfig(runnerConfig);

  // Self hosted version allows for automatic migrations
  if (!config.migrations?.disable_auto_migration) {
    await migrations.migrate({
      direction: Direction.Up,
      runner_config: runnerConfig
    });
  }

  const system = new PowerSyncSystem(config);

  logger.info('Starting system');
  await system.start();
  logger.info('System started');

  Metrics.getInstance().configureReplicationMetrics(system);

  const mngr = new replication.WalStreamManager(system);
  mngr.start();

  // MUST be after startServer.
  // This is so that the handler is run before the server's handler, allowing streams to be interrupted on exit
  system.addTerminationHandler();

  system.terminationHandler.handleTerminationSignal(async () => {
    await mngr.stop();
  });

  await system.probe.ready();
}
