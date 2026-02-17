/**
 * Table name used by PowerSync to write checkpoint markers into Convex via streaming import.
 *
 * Convex rejects table names starting with `_`, so we use a plain prefix.
 * Convex streaming import may materialize this table with a `source_` prefix
 * (e.g. `source_powersync_checkpoints`), so {@link isConvexCheckpointTable}
 * strips that prefix when matching.
 */
export const CONVEX_CHECKPOINT_TABLE = 'powersync_checkpoints';

/**
 * Prefix that Convex streaming import prepends to ingested table names.
 */
export const CONVEX_SOURCE_TABLE_PREFIX = 'source_';

/**
 * Returns true if `tableName` is the PowerSync checkpoint marker table,
 * accounting for zero or more `source_` prefixes added by Convex.
 */
export function isConvexCheckpointTable(tableName: string): boolean {
  let candidate = tableName;

  // Strip up to 4 layers of source_ prefix (defensive; typically 0-1).
  for (let depth = 0; depth < 4; depth += 1) {
    if (candidate === CONVEX_CHECKPOINT_TABLE) {
      return true;
    }

    if (!candidate.startsWith(CONVEX_SOURCE_TABLE_PREFIX)) {
      return false;
    }

    candidate = candidate.slice(CONVEX_SOURCE_TABLE_PREFIX.length);
  }

  return false;
}
