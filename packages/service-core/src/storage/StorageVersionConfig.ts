export interface StorageVersionConfig {
  version: number;

  /**
   * Whether versioned bucket names are automatically enabled.
   *
   * If this is false, bucket names may still be versioned depending on the sync config.
   */
  versionedBuckets: boolean;

  softDeleteCurrentData: boolean;
}

/**
 * Oldest supported storage version.
 */
export const LEGACY_STORAGE_VERSION = 1;

/**
 * Default storage version for newly persisted sync rules.
 */
export const CURRENT_STORAGE_VERSION = 2;

/**
 * Not enabled by default yet.
 */
export const INCREMENTAL_REPROCESSING_STORAGE_VERSION = 3;

export const SUPPORTED_STORAGE_VERSIONS = [1, 2, 3];
/**
 * Shared storage-version behavior across storage implementations.
 */
export const STORAGE_VERSION_CONFIG: Record<number, StorageVersionConfig | undefined> = {
  [1]: {
    version: 1,
    versionedBuckets: false,
    softDeleteCurrentData: false
  },
  [2]: {
    version: 2,
    versionedBuckets: true,
    softDeleteCurrentData: false
  },
  [3]: {
    version: 3,
    versionedBuckets: true,
    softDeleteCurrentData: true
  }
};
