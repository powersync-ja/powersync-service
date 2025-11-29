import { container, logger } from '@powersync/lib-services-framework';
import * as core from '@powersync/service-core';

interface DynamicModuleMap {
  [key: string]: () => Promise<any>;
}

export const ConnectionModuleMap: DynamicModuleMap = {
  mysql: () => import('@powersync/service-module-mysql').then((module) => module.MySQLModule),
  postgresql: () => import('@powersync/service-module-postgres').then((module) => module.PostgresModule)
};

const StorageModuleMap: DynamicModuleMap = {
  postgresql: () => import('@powersync/service-module-postgres-storage').then((module) => module.PostgresStorageModule)
};

/**
 * Utility function to dynamically load and instantiate modules.
 */
export async function loadModules(config: core.ResolvedPowerSyncConfig) {
  const requiredConnections = [
    ...new Set(
      config.connections
        ?.map((connection) => connection.type)
        .filter((connection) => !connection.startsWith('mongo')) || []
    )
  ];
  const missingConnectionModules: string[] = [];
  const modulePromises: Promise<any>[] = [];

  // 1. Map connection types to their module loading promises making note of any
  // missing connection types.
  requiredConnections.forEach((connectionType) => {
    const modulePromise = ConnectionModuleMap[connectionType];
    if (modulePromise !== undefined) {
      modulePromises.push(modulePromise());
    } else {
      missingConnectionModules.push(connectionType);
    }
  });

  // Fail if any connection types are not found.
  if (missingConnectionModules.length > 0) {
    throw new Error(`Invalid connection types: "${[...missingConnectionModules].join(', ')}"`);
  }

  if (config.storage.type !== 'mongo' && StorageModuleMap[config.storage.type] !== undefined) {
    modulePromises.push(StorageModuleMap[config.storage.type]());
  }

  // 2. Dynamically import and instantiate module classes and resolve all promises
  // raising errors if any modules could not be imported.
  const moduleInstances = await Promise.all(
    modulePromises.map(async (modulePromise) => {
      const ModuleClass = await modulePromise;
      return new ModuleClass();
    })
  );

  return moduleInstances;
}
