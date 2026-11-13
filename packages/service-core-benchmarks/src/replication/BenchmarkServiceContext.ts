import { system, utils } from '@powersync/service-core';
import { StorageBenchmarkRunResource } from '../types/StorageBenchmark.js';

export interface BenchmarkServiceContextOptions {
  readonly serviceMode: system.ServiceContextMode;
  readonly configuration: utils.ResolvedPowerSyncConfig;
  readonly storageResource: StorageBenchmarkRunResource;
}

export function createBenchmarkServiceContext(options: BenchmarkServiceContextOptions): system.ServiceContextContainer {
  const context = new system.ServiceContextContainer({
    serviceMode: options.serviceMode,
    configuration: options.configuration
  });
  context.storageEngine.registerProvider({
    type: 'benchmark',
    async getStorage() {
      return {
        storage: options.storageResource.factory,
        reportStorage: {} as never,
        async shutDown() {},
        async tearDown() {
          return false;
        }
      };
    }
  });
  return context;
}
