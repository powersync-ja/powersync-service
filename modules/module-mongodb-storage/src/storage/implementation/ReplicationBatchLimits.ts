/**
 * Fixed large batching targets. These are flush/backpressure thresholds, not
 * hard memory ceilings: a complete source row may exceed a target on its own.
 */
export const MAX_INPUT_BATCH_COUNT = 16_000;
export const MAX_INPUT_BATCH_BYTES = 8 * 1024 * 1024;
/** Per write category, preserving the existing transaction splitting rules. */
export const MAX_PREPARED_BATCH_COUNT = 16_000;
export const MAX_PREPARED_BATCH_BYTES = 16 * 1024 * 1024;

/** Headroom for JS objects, memberships, packed chunks and upload buffers. */
export const RETAINED_BYTES_FACTOR = 4;

/**
 * One predecessor plus its successor. Preparation's overlay assumes at most one
 * predecessor, so increasing this requires more than changing a capacity limit.
 */
export const MAX_PIPELINE_BATCHES = 2;

/**
 * Allow two normal prepared batches to overlap within the accounted-work target.
 * Accounted work includes packing headroom; this is not an RSS or single-row limit.
 */
export const MAX_PIPELINE_BYTES = MAX_PIPELINE_BATCHES * RETAINED_BYTES_FACTOR * MAX_PREPARED_BATCH_BYTES;
