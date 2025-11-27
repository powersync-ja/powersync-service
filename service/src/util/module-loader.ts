import { container, logger } from '@powersync/lib-services-framework';
import * as core from '@powersync/service-core';

interface DynamicModuleMap {
  [key: string]: () => Promise<any>;
}

const ModuleMap: DynamicModuleMap = {
  mysql: () => import('@powersync/service-module-mysql').then((module) => module.MySQLModule),
  postgresql: () => import('@powersync/service-module-postgres').then((module) => module.PostgresModule),
  'postgresql-storage': () =>
    import('@powersync/service-module-postgres-storage').then((module) => module.PostgresStorageModule)
};

/**
 * Utility function to dynamically load and instantiate modules.
 * This function can optionally be moved to its own file (e.g., module-loader.ts)
 * for better separation of concerns if the file gets much larger.
 */
export async function loadModules(config: core.ResolvedPowerSyncConfig) {
  // 1. Determine required connections: Unique types from connections + storage type
  const requiredConnections = [
    ...new Set(config.connections?.map((connection) => connection.type) || []),
    `${config.storage.type}-storage` // Using template literal is clear
  ];

  // 2. Map connection types to their module loading promises
  const modulePromises = requiredConnections.map(async (connectionType) => {
    // Exclude 'mongo' connections explicitly early
    if (connectionType.startsWith('mongo')) {
      return null; // Return null for filtering later
    }

    const modulePromise = ModuleMap[connectionType];

    // Check if a module is defined for the connection type
    if (!modulePromise) {
      logger.warn(`No module defined in ModuleMap for connection type: ${connectionType}`);
      return null;
    }

    try {
      // Dynamically import and instantiate the class
      const ModuleClass = await modulePromise();
      return new ModuleClass();
    } catch (error) {
      // Log an error if the dynamic import fails (e.g., module not installed)
      logger.error(`Failed to load module for ${connectionType}:`, error);
      return null;
    }
  });

  // 3. Resolve all promises and filter out nulls/undefineds
  const moduleInstances = await Promise.all(modulePromises);
  return moduleInstances.filter((instance) => instance !== null); // Filter out nulls from excluded or failed imports
}
