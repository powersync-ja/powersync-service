import { ErrorCode, ServiceError } from '@powersync/lib-services-framework';
import { ResolvedPowerSyncConfig } from '../util/util-index.js';
import { AbstractModule } from './AbstractModule.js';

interface DynamicModuleMap {
  [key: string]: () => Promise<AbstractModule>;
}

export interface ModuleLoaders {
  storage: DynamicModuleMap;
  connection: DynamicModuleMap;
}

function quoteList(values: string[]): string {
  return values.map((value) => `"${value}"`).join(', ');
}

/**
 * Utility function to dynamically load and instantiate modules.
 */
export async function loadModules(config: ResolvedPowerSyncConfig, loaders: ModuleLoaders) {
  const requiredConnections = [...new Set(config.connections?.map((connection) => connection.type) || [])];
  const missingConnectionModules: string[] = [];
  const modulePromises: Promise<AbstractModule>[] = [];

  // 1. Map connection types to their module loading promises making note of any
  // missing connection types.
  requiredConnections.forEach((connectionType) => {
    const modulePromise = loaders.connection[connectionType];
    if (modulePromise !== undefined) {
      modulePromises.push(modulePromise());
    } else {
      missingConnectionModules.push(connectionType);
    }
  });

  // Fail if any connection types are not found.
  if (missingConnectionModules.length > 0) {
    const validConnections = Object.keys(loaders.connection);
    throw new ServiceError(
      ErrorCode.PSYNC_S3201,
      `Invalid connection types: ${quoteList(missingConnectionModules)}${validConnections.length > 0 ? `. Supported types: ${quoteList(validConnections)}.` : ''}`
    );
  }

  if (loaders.storage[config.storage.type] !== undefined) {
    modulePromises.push(loaders.storage[config.storage.type]());
  } else {
    const validStorage = Object.keys(loaders.storage);
    throw new ServiceError(
      ErrorCode.PSYNC_S3201,
      `Invalid storage type: "${config.storage.type}"${validStorage.length > 0 ? `. Supported types: ${quoteList(validStorage)}.` : ''}`
    );
  }

  // 2. Dynamically import and instantiate module classes and resolve all promises
  // raising errors if any modules could not be imported.
  const moduleInstances = await Promise.all(modulePromises);

  return moduleInstances;
}
