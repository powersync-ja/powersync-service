import type { MongoReplicationBatchSize } from '../../types/types.js';

/** Headroom for JS objects, memberships, packed chunks and upload buffers. */
export const RETAINED_BYTES_FACTOR = 4;

/**
 * One predecessor plus its successor. Preparation's overlay assumes at most one
 * predecessor, so increasing this requires more than changing a capacity limit.
 */
export const MAX_PIPELINE_BATCHES = 2;

export const DEFAULT_REPLICATION_BATCH_SIZE: MongoReplicationBatchSize = 'medium';

/** Flush/backpressure targets, not hard memory ceilings or single-row limits. */
export interface ReplicationBatchLimits {
  /** Row operations or custom checkpoints in a writer's input buffer. */
  readonly maxInputBatchCount: number;
  /** Estimated input bytes before row evaluation. */
  readonly maxInputBatchBytes: number;
  /** Per write category, preserving the existing transaction splitting rules. */
  readonly maxTransactionBatchCount: number;
  readonly maxTransactionBatchBytes: number;
  /** Accounted work including packing headroom; excludes source/raw input buffers. */
  readonly maxPipelineBytes: number;
}

function profile(count: number, inputBytes: number, transactionBytes: number): ReplicationBatchLimits {
  return Object.freeze({
    maxInputBatchCount: count,
    maxInputBatchBytes: inputBytes,
    maxTransactionBatchCount: count,
    maxTransactionBatchBytes: transactionBytes,
    maxPipelineBytes: MAX_PIPELINE_BATCHES * RETAINED_BYTES_FACTOR * transactionBytes
  });
}

/** Larger profiles trade higher retained memory for fewer flushes and round trips. */
export const REPLICATION_BATCH_LIMITS: Readonly<Record<MongoReplicationBatchSize, ReplicationBatchLimits>> =
  Object.freeze({
    small: profile(4_000, 2 * 1024 * 1024, 4 * 1024 * 1024),
    medium: profile(8_000, 4 * 1024 * 1024, 8 * 1024 * 1024),
    large: profile(16_000, 8 * 1024 * 1024, 16 * 1024 * 1024)
  });

export const DEFAULT_REPLICATION_BATCH_LIMITS = REPLICATION_BATCH_LIMITS[DEFAULT_REPLICATION_BATCH_SIZE];
