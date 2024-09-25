import * as t from 'ts-codec';

export const ReplicationError = t.object({
  /** Warning: Could indicate an issue. Fatal: Prevents replicating. */
  level: t.literal('warning').or(t.literal('fatal')),
  message: t.string
});
export type ReplicationError = t.Encoded<typeof ReplicationError>;

export const TableInfo = t.object({
  schema: t.string,
  name: t.string,

  /** Specified if this table is part of a wildcard pattern. */
  pattern: t.string.optional(),

  /** Usually just ['id'] */
  replication_id: t.array(t.string),
  /** Used in data replication */
  data_queries: t.boolean,
  /** Used for parameter query replication */
  parameter_queries: t.boolean,

  /** Also included in the global errors array. */
  errors: t.array(ReplicationError)
});
export type TableInfo = t.Encoded<typeof TableInfo>;

export const SyncRulesStatus = t.object({
  content: t.string.optional(),
  connections: t.array(
    t.object({
      id: t.string,
      tag: t.string,

      /**
       * PostgreSQL logical replication slot name.
       */
      slot_name: t.string,

      /**
       * Once initial replication is done, this moves over to
       * logical replication.
       */
      initial_replication_done: t.boolean,

      /**
       * The last LSN that has been replicated. This may be in the middle of a transaction.
       */
      last_lsn: t.string.optional(),

      /**
       * The last time any replication activity was recorded.
       *
       * This is typically (but not always) updated together with last_lsn
       */
      last_keepalive_ts: t.string.optional(),

      /**
       * The last time we created a new checkpoint. In other words, a transaction
       * was successfully replicated.
       */
      last_checkpoint_ts: t.string.optional(),

      /** Replication lag in bytes. undefined if we cannot calculate this. */
      replication_lag_bytes: t.number.optional(),

      tables: t.array(TableInfo)
    })
  ),
  /** Sync-rule-level errors */
  errors: t.array(ReplicationError)
});
export type SyncRulesStatus = t.Encoded<typeof SyncRulesStatus>;

export const ConnectionStatus = t.object({
  id: t.string,
  postgres_uri: t.string,
  connected: t.boolean,
  /** Connection-level errors */
  errors: t.array(ReplicationError)
});
export type ConnectionStatus = t.Encoded<typeof ConnectionStatus>;

export const ConnectionStatusV2 = t.object({
  id: t.string,
  uri: t.string,
  connected: t.boolean,
  /** Connection-level errors */
  errors: t.array(ReplicationError)
});
export type ConnectionStatusV2 = t.Encoded<typeof ConnectionStatusV2>;

export enum SqliteSchemaTypeText {
  null = 'null',
  blob = 'blob',
  text = 'text',
  integer = 'integer',
  real = 'real',
  numeric = 'numeric'
}

export const DatabaseSchema = t.object({
  name: t.string,
  tables: t.array(
    t.object({
      name: t.string,
      columns: t.array(
        t.object({
          name: t.string,

          /**
           * Option 1: SQLite type flags - see ExpressionType.typeFlags.
           * Option 2: SQLite type name in lowercase - 'text' | 'integer' | 'real' | 'numeric' | 'blob' | 'null'
           */
          sqlite_type: t.number.or(t.Enum(SqliteSchemaTypeText)),

          /**
           * Type name from the source database, e.g. "character varying(255)[]"
           */
          original_type: t.string,

          /**
           * Description for the field if available.
           */
          description: t.string.optional(),

          /**
           * Full type name, e.g. "character varying(255)[]"
           * @deprecated - use original_type
           */
          type: t.string.optional(),

          /**
           * Internal postgres type, e.g. "varchar[]".
           * @deprecated - use original_type
           */
          pg_type: t.string.optional()
        })
      )
    })
  )
});
export type DatabaseSchema = t.Encoded<typeof DatabaseSchema>;

export const InstanceSchema = t.object({
  connections: t.array(
    t.object({
      id: t.string.optional(),
      tag: t.string,
      schemas: t.array(DatabaseSchema)
    })
  )
});
export type InstanceSchema = t.Encoded<typeof InstanceSchema>;
