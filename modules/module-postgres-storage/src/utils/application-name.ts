import { POWERSYNC_VERSION } from '@powersync/service-core';

/**
 * Name for postgres application_name, for bucket storage connections.
 */
export function getStorageApplicationName() {
  return `powersync-storage/${POWERSYNC_VERSION}`;
}
