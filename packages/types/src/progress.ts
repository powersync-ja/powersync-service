import * as t from 'ts-codec';
import { ReplicationError } from './definitions.js';

export enum ReplicationPhase {
  INITIAL_REPLICATION_SNAPSHOTTING = 'initial_replication_snapshotting',
  INITIAL_REPLICATION_COMPACTING = 'initial_replication_compacting',
  INITIAL_REPLICATION_CATCHING_UP = 'initial_replication_catching_up',
  INCREMENTAL_REPLICATION = 'incremental_replication'
}

const ReplicationProgress = t.object({
  /**
   * Number of records replicated so far
   */
  replicated_count: t.number.optional(),
  /**
   * Number of records estimated to be replicated. Absent if unknown or not yet estimated
   */
  total_estimated_count: t.number.optional(),
  /**
   * From 0 -> 100. Stays at less than 100 until replication is complete
   */
  percent: t.number.optional(),
  /**
   * Records / second. Current replication rate based on a moving window.
   */
  replication_rate: t.number.optional(),
  /**
   * UTC timestamp of the last progress update, in ISO 8601 format.
   * Example: "2026-10-15T10:30:00.000Z".
   */
  updated_at: t.string.optional(),
  /**
   * Seconds until replication is expected to complete from the last updated_at timestamp.
   * Calculated based on the current replication rate and the number of records remaining.
   */
  eta_seconds: t.number.optional()
});

export type ReplicationProgress = t.Encoded<typeof ReplicationProgress>;

/**
 *  Represents replication progress for an individual source table
 */
const SourceTableReplicationProgress = t
  .object({
    name: t.string,
    schema: t.string,
    complete: t.boolean
  })
  .and(ReplicationProgress);

export type SourceTableReplicationProgress = t.Encoded<typeof SourceTableReplicationProgress>;

/**
 *  Represents progress for the initial replication table snapshotting phase as a whole.
 *  Also includes the progress of the individual source tables being snapshotted.
 */
const SnapshotReplicationProgress = t
  .object({
    phase: t.literal(ReplicationPhase.INITIAL_REPLICATION_SNAPSHOTTING),
    /**
     *  Table level progress for the snapshot replication
     */
    source_tables: t.array(SourceTableReplicationProgress)
  })
  .and(ReplicationProgress);

export type SnapshotReplicationProgress = t.Encoded<typeof SnapshotReplicationProgress>;

/**
 *  Represents progress for the storage compaction run during initial replication.
 */
const CompactingProgress = t.object({
  phase: t.literal(ReplicationPhase.INITIAL_REPLICATION_COMPACTING)
});

export type CompactingProgress = t.Encoded<typeof CompactingProgress>;

/**
 *  Represents progress for the replication that runs after table snapshotting until the first consistent
 *  checkpoint is reached, signalling the completion of the initial replication phase.
 */
const CatchingUpProgress = t.object({
  phase: t.literal(ReplicationPhase.INITIAL_REPLICATION_CATCHING_UP)
});

export type CatchingUpProgress = t.Encoded<typeof CatchingUpProgress>;

/**
 *  Represents progress for the incremental replication phase that occurs once the initial replication phase is complete.
 *  The default replication phase for an ACTIVE sync config
 */
const IncrementalReplicationProgress = t.object({
  phase: t.literal(ReplicationPhase.INCREMENTAL_REPLICATION)
});

export type IncrementalReplicationProgress = t.Encoded<typeof IncrementalReplicationProgress>;

export const SyncConfigReplicationProgress = t.object({
  /**
   *  The current state of the sync config.Based on the values of the SyncRuleState enum from BucketStorage.ts
   */
  state: t.literal('ACTIVE').or(t.literal('PROCESSING')).or(t.literal('ERRORED')),
  version_label: t.string.optional(),
  /**
   *  The replication stream that this sync config is associated with
   */
  replication_stream: t.string.optional(),
  progress: SnapshotReplicationProgress.or(CompactingProgress)
    .or(CatchingUpProgress)
    .or(IncrementalReplicationProgress)
    .optional(),

  /**
   *  Errors encountered during replication
   */
  errors: t.array(ReplicationError)
});

export type SyncConfigReplicationProgress = t.Encoded<typeof SyncConfigReplicationProgress>;
