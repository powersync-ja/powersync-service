import { migrations } from '@powersync/service-core';

export const up: migrations.PowerSyncMigrationFunction = async (_context) => {
  // No-op.
  // Pending-delete support is now storage-version specific and initialized when v3 sync rules are deployed.
};

export const down: migrations.PowerSyncMigrationFunction = async (_context) => {
  // No-op.
};
