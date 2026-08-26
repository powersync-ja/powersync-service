import * as t from 'ts-codec';
import { ConnectionStatus, InstanceSchema, SyncRulesStatus } from './definitions.js';

export const GetSchemaRequest = t.object({});
export type GetSchemaRequest = t.Encoded<typeof GetSchemaRequest>;

export const GetSchemaResponse = InstanceSchema;
export type GetSchemaResponse = t.Encoded<typeof GetSchemaResponse>;

export const ExecuteSqlRequest = t.object({
  connection_id: t.string.optional(),
  sql: t.object({
    query: t.string,
    args: t.array(t.string.or(t.number).or(t.boolean))
  })
});
export type ExecuteSqlRequest = t.Encoded<typeof ExecuteSqlRequest>;

export const ExecuteSqlResponse = t.object({
  success: t.boolean,
  results: t.object({
    columns: t.array(t.string),
    rows: t.array(t.array(t.string.or(t.number).or(t.boolean).or(t.Null)))
  }),
  /** Set if success = false */
  error: t.string.optional()
});
export type ExecuteSqlResponse = t.Encoded<typeof ExecuteSqlResponse>;

export const DiagnosticsRequest = t.object({
  sync_rules_content: t.boolean.optional()
});
export type DiagnosticsRequest = t.Encoded<typeof DiagnosticsRequest>;

export const DiagnosticsResponse = t.object({
  /**
   * Connection-level errors are listed here.
   */
  connections: t.array(ConnectionStatus),

  /**
   * Present if there is a fully deployed sync config.
   *
   * Sync-config-level errors are listed here.
   */
  active_sync_rules: SyncRulesStatus.optional(),

  /**
   * Present if there is sync config in the process of being deployed / initial replication.
   *
   * Once initial replication is done, this will be placed in `active_sync_rules`.
   *
   * Sync-config-level errors are listed here.
   */
  deploying_sync_rules: SyncRulesStatus.optional()
});
export type DiagnosticsResponse = t.Encoded<typeof DiagnosticsResponse>;

export const ReprocessRequest = t.object({});
export type ReprocessRequest = t.Encoded<typeof ReprocessRequest>;

export const ReprocessResponse = t.object({
  connections: t.array(
    t.object({
      id: t.string.optional(),
      tag: t.string,
      slot_name: t.string
    })
  )
});
export type ReprocessResponse = t.Encoded<typeof ReprocessResponse>;

export const ValidateRequest = t.object({
  sync_rules: t.string
});
export type ValidateRequest = t.Encoded<typeof ValidateRequest>;

export const ValidateResponse = SyncRulesStatus;
export type ValidateResponse = t.Encoded<typeof ValidateResponse>;

export const BucketReportRequest = t.object({
  /**
   * Maximum number of buckets to return, ranked by operation count descending (worst offenders first).
   * Defaults to 50 when omitted. Must be an integer between 1 and 1000; anything else is rejected with a
   * validation error.
   */
  limit: t.number.optional()
});
export type BucketReportRequest = t.Encoded<typeof BucketReportRequest>;

export const SuggestedBucketAction = t
  .literal('none')
  .or(t.literal('compact'))
  .or(t.literal('defragment'))
  .or(t.literal('both'))
  .or(t.literal('unknown'));
export type SuggestedBucketAction = t.Encoded<typeof SuggestedBucketAction>;

export const BucketStorageStats = t.object({
  /** Full bucket name, e.g. `by_user["u1"]`. */
  bucket: t.string,
  /** Total operations in the bucket's history (PUT/REMOVE/MOVE/CLEAR). Exact and current. */
  operations: t.number,
  /** Approximate size of the operation history in bytes. */
  operation_bytes: t.number,
  /**
   * Operations written after the last full compact — the staleness indicator for `rows` and
   * `fragmentation`, which are snapshots from that compact. Equals `operations` when the bucket has never
   * been fully compacted.
   */
  uncompacted_operations: t.number,
  /**
   * Live rows in the bucket as of its last full compact, or null if the bucket has never been fully
   * compacted (or the storage version does not capture compact statistics).
   */
  rows: t.number.or(t.Null),
  /**
   * `operations / max(rows, 1)`. ~1 is healthy (fully compacted); higher means more operation-history
   * overhead that a compact/defragment can reclaim. Null whenever `rows` is null.
   */
  fragmentation: t.number.or(t.Null),
  /** ISO timestamp of the bucket's last full compact, which is when `rows` was captured. */
  last_full_compact_at: t.string.or(t.Null),
  /**
   * ISO timestamp of when the scheduled compactor will next consider this bucket. A suggested compact with
   * a future `next_compact_at` means the compact is already planned but throttled until then.
   */
  next_compact_at: t.string.or(t.Null),
  /**
   * Suggested maintenance action, derived from the bucket's compact statistics: `none` (healthy), `compact`
   * (un-compacted superseded history to reclaim), `defragment` (mostly compaction residue that only a
   * defragment collapses), `both`, or `unknown` (no compact statistics to derive one from).
   */
  suggested_action: SuggestedBucketAction
});
export type BucketStorageStats = t.Encoded<typeof BucketStorageStats>;

export const BucketDefinitionStats = t.object({
  /** Definition name as it prefixes bucket names, e.g. `1#by_user` (versioned in storage v2 and later). */
  definition: t.string,
  /** Number of buckets in this definition with stored operations. */
  bucket_count: t.number,
  /** Total operations across the definition's buckets. Exact and current. */
  operations: t.number,
  /** Approximate size of the definition's operation history in bytes. */
  operation_bytes: t.number,
  /**
   * Operations not covered by any bucket's last full compact — the staleness indicator for `rows` and
   * `fragmentation`.
   */
  uncompacted_operations: t.number,
  /**
   * Live rows across the definition's buckets, counting a row once per bucket that contains it. Derived
   * from each bucket's last full compact (extrapolated when only some buckets have been compacted); null
   * when none have.
   */
  rows: t.number.or(t.Null),
  /** `operations / max(rows, 1)` across the whole definition. Null whenever `rows` is null. */
  fragmentation: t.number.or(t.Null),
  /** Suggested maintenance action for the definition; same values as `buckets[].suggested_action`. */
  suggested_action: SuggestedBucketAction
});
export type BucketDefinitionStats = t.Encoded<typeof BucketDefinitionStats>;

export const BucketReportResponse = t.object({
  /** Worst-offender buckets, ranked by operation count then fragmentation. */
  buckets: t.array(BucketStorageStats),
  /** Per-definition rollup, ranked by operation count then fragmentation. */
  definitions: t.array(BucketDefinitionStats),
  totals: t.object({
    /** Number of buckets with stored operations. Exact, even when the other totals are estimates. */
    bucket_count: t.number,
    /** Sum of operations across all buckets. Estimated when the bucket set was sampled. */
    operations: t.number,
    /** Sum of operation-history bytes across all buckets. Estimated when the bucket set was sampled. */
    operation_bytes: t.number,
    /** True if the totals are estimated because the bucket set was sampled rather than fully scanned. */
    estimated: t.boolean
  }),
  /** True if there are more buckets than returned (more than `limit`). */
  buckets_truncated: t.boolean,
  /** True if the definition rollup is incomplete: more definitions exist than the report caps at. */
  definitions_truncated: t.boolean
});
export type BucketReportResponse = t.Encoded<typeof BucketReportResponse>;
