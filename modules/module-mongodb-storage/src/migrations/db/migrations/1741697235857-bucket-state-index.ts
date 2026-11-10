import { migrations } from '@powersync/service-core';

// Indexes for v1/v2 data collections are managed by stream initialization.
export const up: migrations.PowerSyncMigrationFunction = async () => {};

export const down: migrations.PowerSyncMigrationFunction = async () => {};
