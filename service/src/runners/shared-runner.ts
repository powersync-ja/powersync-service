import * as core from '@powersync/service-core';
import { registerMetrics } from '../metrics.js';

/**
 * Configures the server portion on a {@link serviceContext}
 */
export const registerSharedRunnerServices = async (serviceContext: core.system.ServiceContextContainer) => {
  const { storage, disposer: storageDisposer } = await serviceContext.storageFactory.generateStorage(
    serviceContext.configuration
  );

  // TODO better identifier
  serviceContext.register(core.system.ServiceIdentifiers.STORAGE, storage);
  serviceContext.lifeCycleEngine.withLifecycle(storage, {
    stop: () => storageDisposer()
  });

  // Needs to be done after storage
  await registerMetrics(serviceContext);
};
