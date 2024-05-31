import * as t from 'ts-codec';
import { ConnectionStatus, InstanceSchema, SyncRulesStatus } from './definitions';

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

export const DemoCredentialsRequest = t.object({
  connection_id: t.string.optional()
});
export type DemoCredentialsRequest = t.Encoded<typeof DemoCredentialsRequest>;

export const DemoCredentialsResponse = t.object({
  /** If this instance has a demo database, this contains the credentials. */
  credentials: t
    .object({
      postgres_uri: t.string
    })
    .optional()
});
export type DemoCredentialsResponse = t.Encoded<typeof DemoCredentialsResponse>;

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
   * Present if there are fully-deployed sync rules.
   *
   * Sync-rule-level errors are listed here.
   */
  active_sync_rules: SyncRulesStatus.optional(),

  /**
   * Present if there are sync rules in the process of being deployed / initial replication.
   *
   * Once initial replication is done, this will  be placed in `active_sync_rules`.
   *
   * Sync-rule-level errors are listed here.
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
