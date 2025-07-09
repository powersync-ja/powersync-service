import { POWERSYNC_VERSION } from '@powersync/service-core';

/**
 * application_name for PostgreSQL connections to the source database
 */
export function getApplicationName() {
  return `powersync/${POWERSYNC_VERSION}`;
}
