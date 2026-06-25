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
   * Caps the response only, not the query cost: totals are still computed across all buckets. Omit for
   * no limit.
   */
  limit: t.number.optional()
});
export type BucketReportRequest = t.Encoded<typeof BucketReportRequest>;

export const BucketStorageStats = t.object({
  /** Full bucket name, e.g. `by_user["u1"]`. */
  bucket: t.string,
  /** Total operations in the bucket's history (PUT/REMOVE/MOVE/CLEAR). */
  operations: t.number,
  /** Live rows currently in the bucket. */
  rows: t.number,
  /** Approximate size of the operation history in bytes. */
  operation_bytes: t.number,
  /**
   * `operations / max(rows, 1)`. ~1 is healthy (fully compacted); higher means more operation-history
   * overhead that a compact/defragment can reclaim.
   */
  fragmentation: t.number
});
export type BucketStorageStats = t.Encoded<typeof BucketStorageStats>;

export const BucketReportResponse = t.object({
  /** Per-bucket stats, ranked worst-first (most operations, then most fragmented). */
  buckets: t.array(BucketStorageStats),
  totals: t.object({
    /** Number of buckets with stored operations or rows (before any `limit`). */
    bucket_count: t.number,
    operations: t.number,
    /** Sum of per-bucket live rows. Rows in multiple buckets are counted per bucket. */
    rows: t.number,
    operation_bytes: t.number,
    /** Instance-wide `operations / max(rows, 1)`: operations a new client downloads per live row. */
    fragmentation: t.number
  }),
  /** True if `buckets` was truncated by `limit`. `totals` still reflects all buckets. */
  truncated: t.boolean
});
export type BucketReportResponse = t.Encoded<typeof BucketReportResponse>;
