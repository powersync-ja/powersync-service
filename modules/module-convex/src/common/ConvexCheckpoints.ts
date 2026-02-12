export const CONVEX_CHECKPOINT_TABLE_PRIMARY = '_powersync_checkpoints';
export const CONVEX_CHECKPOINT_TABLE_FALLBACK = 'powersync_checkpoints';
export const CONVEX_CHECKPOINT_TABLE_PREFIX = 'source_';

export const CONVEX_CHECKPOINT_TABLE_CANDIDATES = [
  CONVEX_CHECKPOINT_TABLE_PRIMARY,
  CONVEX_CHECKPOINT_TABLE_FALLBACK
] as const;

export function isConvexCheckpointTable(tableName: string): boolean {
  let candidate = tableName;

  for (let depth = 0; depth < 4; depth += 1) {
    if (
      CONVEX_CHECKPOINT_TABLE_CANDIDATES.includes(
        candidate as (typeof CONVEX_CHECKPOINT_TABLE_CANDIDATES)[number]
      )
    ) {
      return true;
    }

    if (!candidate.startsWith(CONVEX_CHECKPOINT_TABLE_PREFIX)) {
      return false;
    }

    candidate = candidate.slice(CONVEX_CHECKPOINT_TABLE_PREFIX.length);
  }

  return false;
}
