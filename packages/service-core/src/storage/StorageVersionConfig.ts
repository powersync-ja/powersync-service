export interface StorageVersionConfig {
  version: number;

  /**
   * Whether versioned bucket names are automatically enabled.
   *
   * If this is false, bucket names may still be versioned depending on the sync config.
   *
   * Introduced in STORAGE_VERSION_2.
   */
  versionedBuckets: boolean;

  /**
   * Whether to use soft deletes for current_data, improving replication concurrency support.
   *
   * Introduced in STORAGE_VERSION_3.
   */
  softDeleteCurrentData: boolean;
}

/**
 * Corresponds to the storage version initially used, before we started explicitly versioning storage.
 */
export const STORAGE_VERSION_1 = 1;

/**
 * First new storage version.
 *
 * Uses versioned bucket names.
 *
 * On MongoDB storage, this always uses Long for checksums.
 */
export const STORAGE_VERSION_2 = 2;

/**
 * This version is currently unstable, and not enabled by default yet.
 *
 * This is used to build towards incremental reprocessing.
 */
export const STORAGE_VERSION_3 = 3;

/**
 * Oldest supported storage version.
 */
export const LEGACY_STORAGE_VERSION = STORAGE_VERSION_1;

/**
 * Default storage version for newly persisted sync rules.
 */
export const CURRENT_STORAGE_VERSION = STORAGE_VERSION_2;

/**
 * All versions that can be loaded.
 *
 * This includes unstable versions.
 */
export const SUPPORTED_STORAGE_VERSIONS = [STORAGE_VERSION_1, STORAGE_VERSION_2, STORAGE_VERSION_3];

/**
 * Shared storage-version behavior across storage implementations.
 */
export const STORAGE_VERSION_CONFIG: Record<number, StorageVersionConfig | undefined> = {
  [STORAGE_VERSION_1]: {
    version: STORAGE_VERSION_1,
    versionedBuckets: false,
    softDeleteCurrentData: false
  },
  [STORAGE_VERSION_2]: {
    version: STORAGE_VERSION_2,
    versionedBuckets: true,
    softDeleteCurrentData: false
  },
  [STORAGE_VERSION_3]: {
    version: STORAGE_VERSION_3,
    versionedBuckets: true,
    softDeleteCurrentData: true
  }
};
