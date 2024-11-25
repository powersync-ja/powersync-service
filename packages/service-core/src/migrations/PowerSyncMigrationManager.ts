import * as framework from '@powersync/lib-services-framework';
import * as system from '../system/system-index.js';

/**
 * PowerSync service migrations each have this context available to the `up` and `down` methods.
 */
export interface PowerSyncMigrationContext {
  service_context: system.ServiceContext;
}

export interface PowerSyncMigrationGenerics extends framework.MigrationAgentGenerics {
  MIGRATION_CONTEXT: PowerSyncMigrationContext;
}

export type PowerSyncMigrationFunction = framework.MigrationFunction<PowerSyncMigrationContext>;

export abstract class AbstractPowerSyncMigrationAgent extends framework.AbstractMigrationAgent<PowerSyncMigrationGenerics> {}

export type PowerSyncMigrationManager = framework.MigrationManager<PowerSyncMigrationGenerics>;
