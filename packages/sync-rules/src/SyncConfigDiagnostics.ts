import type { ErrorLocation } from './errors.js';

/**
 * A decoded config path, for example `['config', 'source_table_options', 'orders', 'option']`.
 */
export type SyncConfigSourcePath = readonly (string | number)[];
export type SyncConfigSourceLocationTarget = 'key' | 'value';

export interface SyncConfigSourceLocationResolver {
  /**
   * Resolve a field to its source span, falling back to its nearest existing ancestor.
   */
  getLocation(path: SyncConfigSourcePath, target?: SyncConfigSourceLocationTarget): ErrorLocation | undefined;
}
