import { ServiceAssertionError } from '@powersync/lib-services-framework';
import { storage } from '@powersync/service-core';

export const V1_CURRENT_DATA_TABLE = 'current_data';
export const V3_CURRENT_DATA_TABLE = 'v3_current_data';

/**
 * The table used by a specific storage version for general current_data access.
 */
export function getCommonCurrentDataTable(storageConfig: storage.StorageVersionConfig) {
  return storageConfig.softDeleteCurrentData ? V3_CURRENT_DATA_TABLE : V1_CURRENT_DATA_TABLE;
}

export function getV1CurrentDataTable(storageConfig: storage.StorageVersionConfig) {
  if (storageConfig.softDeleteCurrentData) {
    throw new ServiceAssertionError('current_data table cannot be used when softDeleteCurrentData is enabled');
  }
  return V1_CURRENT_DATA_TABLE;
}

export function getV3CurrentDataTable(storageConfig: storage.StorageVersionConfig) {
  if (!storageConfig.softDeleteCurrentData) {
    throw new ServiceAssertionError('v3_current_data table cannot be used when softDeleteCurrentData is disabled');
  }
  return V3_CURRENT_DATA_TABLE;
}
