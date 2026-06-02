/**
 * Table name used by PowerSync to write checkpoint markers into Convex.
 *
 * Convex rejects table names starting with `_`, so we use a plain prefix.
 */
export const CONVEX_CHECKPOINT_TABLE = 'powersync_checkpoints';

/**
 * Returns true if `tableName` is the PowerSync checkpoint marker table.
 */
export function isConvexCheckpointTable(tableName: string): boolean {
  return tableName === CONVEX_CHECKPOINT_TABLE;
}
