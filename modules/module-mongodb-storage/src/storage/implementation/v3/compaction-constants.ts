/**
 * Writers use this delay before scheduling their first chunk compaction.
 * Initial replication includes the same delay when it picks up that work.
 */
export const DEFAULT_MIN_COMPACT_CHUNK_INTERVAL_MS = 5 * 60 * 1000;
