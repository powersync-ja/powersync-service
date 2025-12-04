import * as core from '@powersync/service-core';

interface DynamicModuleMap {
  [key: string]: () => Promise<core.AbstractModule>;
}

export const CONNECTION_MODULE_MAP: DynamicModuleMap = {
  mongodb: () => import('@powersync/service-module-mongodb').then((module) => new module.MongoModule()),
  mysql: () => import('@powersync/service-module-mysql').then((module) => new module.MySQLModule()),
  postgresql: () => import('@powersync/service-module-postgres').then((module) => new module.PostgresModule())
};

const STORAGE_MODULE_MAP: DynamicModuleMap = {
  mongodb: () => import('@powersync/service-module-mongodb-storage').then((module) => new module.MongoStorageModule()),
  postgresql: () =>
    import('@powersync/service-module-postgres-storage').then((module) => new module.PostgresStorageModule())
};

/**
 * Utility function to dynamically load and instantiate modules.
 */
export async function loadModules(config: core.ResolvedPowerSyncConfig) {
  const requiredConnections = [...new Set(config.connections?.map((connection) => connection.type) || [])];
  const missingConnectionModules: string[] = [];
  const modulePromises: Promise<core.AbstractModule>[] = [];

  // 1. Map connection types to their module loading promises making note of any
  // missing connection types.
  requiredConnections.forEach((connectionType) => {
    const modulePromise = CONNECTION_MODULE_MAP[connectionType];
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

  if (STORAGE_MODULE_MAP[config.storage.type] !== undefined) {
    modulePromises.push(STORAGE_MODULE_MAP[config.storage.type]());
  } else {
    throw new Error(`Invalid storage type: "${config.storage.type}"`);
  }

  // 2. Dynamically import and instantiate module classes and resolve all promises
  // raising errors if any modules could not be imported.
  const moduleInstances = await Promise.all(modulePromises);

  return moduleInstances;
}
