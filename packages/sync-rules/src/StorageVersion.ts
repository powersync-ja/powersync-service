export interface ParsedStorageVersion {
  version: number;

  /**
   * If false, this version may be dropped or fundamentally changed in any future service version.
   */
  stable: boolean;
}

export const STORAGE_VERSIONS = new Map<number, ParsedStorageVersion>([
  // version 1 is supported by the storage modules, but cannot be used in sync config
  [2, { version: 2, stable: true }],
  [3, { version: 3, stable: false }]
]);

export const DEFAULT_STORAGE_VERSION = STORAGE_VERSIONS.get(2)!;

/**
 * Parse a storage version.
 *
 * If the version number is unknown or not supported, returns undefined.
 *
 * Generally, even storage versions are stable, and odd storage versions unstable.
 */
export function parseStorageVersion(version: number): ParsedStorageVersion | undefined {
  return STORAGE_VERSIONS.get(version);
}
