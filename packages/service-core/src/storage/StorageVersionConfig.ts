export interface StorageVersionConfig {
  /**
   * Whether versioned bucket names are automatically enabled.
   *
   * If this is false, bucket names may still be versioned depending on the sync config.
   */
  versionedBuckets: boolean;
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
 * Shared storage-version behavior across storage implementations.
 */
export const STORAGE_VERSION_CONFIG: Record<number, StorageVersionConfig | undefined> = {
  [LEGACY_STORAGE_VERSION]: {
    versionedBuckets: false
  },
  [CURRENT_STORAGE_VERSION]: {
    versionedBuckets: true
  }
};
