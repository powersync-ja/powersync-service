import { DATABASE_TYPE, DatabaseType } from './DatabaseType.js';

/**
 * Scale factor applied to a default test timeout on cloud DocumentDB when no
 * explicit cloud override is given to {@link testTimeout}. Cloud DocumentDB has
 * 10-30s change-stream propagation spikes, so timeouts there need headroom.
 * Override with the `TEST_TIMEOUT_MULTIPLIER` env var; defaults to 6.
 */
export const TEST_TIMEOUT_MULTIPLIER = Number(process.env.TEST_TIMEOUT_MULTIPLIER ?? '6');

/**
 * Resolve a test timeout in milliseconds for the current backend.
 *
 * - Fast backend (local / standard MongoDB): uses `defaultMs` as-is.
 * - Cloud DocumentDB: uses `cloudOverride` when provided, otherwise scales
 *   `defaultMs` by {@link TEST_TIMEOUT_MULTIPLIER}.
 */
export function testTimeout(defaultMs: number, options?: { cloudOverride?: number }): number {
  if (DATABASE_TYPE !== DatabaseType.DOCUMENTDB) {
    return defaultMs;
  }
  return options?.cloudOverride ?? Math.ceil(defaultMs * TEST_TIMEOUT_MULTIPLIER);
}
