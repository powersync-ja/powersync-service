import * as pgwire from '@powersync/service-jpgwire';

import * as lib_postgres from '@powersync/lib-service-postgres';
import { ErrorCode, logger, ServiceAssertionError, ServiceError } from '@powersync/lib-services-framework';
import { PatternResult, storage } from '@powersync/service-core';
import * as sync_rules from '@powersync/service-sync-rules';
import * as service_types from '@powersync/service-types';
import { ReplicationIdentity } from './PgRelation.js';

export interface ReplicaIdentityResult {
  replicationColumns: storage.ColumnDescriptor[];
  replicationIdentity: ReplicationIdentity;
}

export async function getPrimaryKeyColumns(
  db: pgwire.PgClient,
  relationId: number,
  mode: 'primary' | 'replident'
): Promise<storage.ColumnDescriptor[]> {
  const indexFlag = mode == 'primary' ? `i.indisprimary` : `i.indisreplident`;
  const attrRows = await lib_postgres.retriedQuery(db, {
    statement: `SELECT a.attname as name, a.atttypid as typeid, t.typname as type, a.attnum as attnum
                                    FROM pg_index i
                                    JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY (i.indkey)
                                    JOIN pg_type t ON a.atttypid = t.oid
                                    WHERE i.indrelid = $1::oid
                                      AND ${indexFlag}
                                      AND a.attnum > 0
                                      ORDER BY a.attnum`,
    params: [{ value: relationId, type: 'int4' }]
  });

  return attrRows.rows.map((row) => {
    return {
      name: row.decodeWithoutCustomTypes(0) as string,
      typeId: row.decodeWithoutCustomTypes(1) as number
    } satisfies storage.ColumnDescriptor;
  });
}

export async function getAllColumns(db: pgwire.PgClient, relationId: number): Promise<storage.ColumnDescriptor[]> {
  const attrRows = await lib_postgres.retriedQuery(db, {
    statement: `SELECT a.attname as name, a.atttypid as typeid, t.typname as type, a.attnum as attnum
                                    FROM pg_attribute a
                                    JOIN pg_type t ON a.atttypid = t.oid
                                    WHERE a.attrelid = $1::oid
                                      AND attnum > 0
                                    ORDER BY a.attnum`,
    params: [{ type: 'varchar', value: relationId }]
  });
  return attrRows.rows.map((row) => {
    return {
      name: row.decodeWithoutCustomTypes(0) as string,
      typeId: row.decodeWithoutCustomTypes(1) as number
    } satisfies storage.ColumnDescriptor;
  });
}

export async function getReplicationIdentityColumns(
  db: pgwire.PgClient,
  relationId: number
): Promise<ReplicaIdentityResult> {
  const rows = await lib_postgres.retriedQuery(db, {
    statement: `SELECT CASE relreplident
        WHEN 'd' THEN 'default'
        WHEN 'n' THEN 'nothing'
        WHEN 'f' THEN 'full'
        WHEN 'i' THEN 'index'
     END AS replica_identity
FROM pg_class
WHERE oid = $1::oid LIMIT 1`,
    params: [{ type: 'int8', value: relationId }]
  });
  const idType: string = rows.rows[0]?.decodeWithoutCustomTypes(0);
  if (idType == 'nothing' || idType == null) {
    return { replicationIdentity: 'nothing', replicationColumns: [] };
  } else if (idType == 'full') {
    return { replicationIdentity: 'full', replicationColumns: await getAllColumns(db, relationId) };
  } else if (idType == 'default') {
    return {
      replicationIdentity: 'default',
      replicationColumns: await getPrimaryKeyColumns(db, relationId, 'primary')
    };
  } else if (idType == 'index') {
    return {
      replicationIdentity: 'index',
      replicationColumns: await getPrimaryKeyColumns(db, relationId, 'replident')
    };
  } else {
    return { replicationIdentity: 'nothing', replicationColumns: [] };
  }
}

export async function checkSourceConfiguration(db: pgwire.PgClient, publicationName: string): Promise<void> {
  // Check basic config
  await lib_postgres.retriedQuery(
    db,
    `DO $$
BEGIN
if current_setting('wal_level') is distinct from 'logical' then
raise exception 'wal_level must be set to ''logical'', your database has it set to ''%''. Please edit your config file and restart PostgreSQL.', current_setting('wal_level');
end if;
if (current_setting('max_replication_slots')::int >= 1) is not true then
raise exception 'Your max_replication_slots setting is too low, it must be greater than 1. Please edit your config file and restart PostgreSQL.';
end if;
if (current_setting('max_wal_senders')::int >= 1) is not true then
raise exception 'Your max_wal_senders setting is too low, it must be greater than 1. Please edit your config file and restart PostgreSQL.';
end if;
end;
$$ LANGUAGE plpgsql;`
  );

  // Check that publication exists
  const rs = await lib_postgres.retriedQuery(db, {
    statement: `SELECT * FROM pg_publication WHERE pubname = $1`,
    params: [{ type: 'varchar', value: publicationName }]
  });
  const row = pgwire.pgwireRows(rs)[0];
  if (row == null) {
    throw new ServiceError(
      ErrorCode.PSYNC_S1141,
      `Publication '${publicationName}' does not exist. Run: \`CREATE PUBLICATION ${publicationName} FOR ALL TABLES\`, or read the documentation for details.`
    );
  }
  if (row.pubinsert == false || row.pubupdate == false || row.pubdelete == false || row.pubtruncate == false) {
    throw new ServiceError(
      ErrorCode.PSYNC_S1142,
      `Publication '${publicationName}' does not publish all changes. Create a publication using \`WITH (publish = "insert, update, delete, truncate")\` (the default).`
    );
  }
  if (row.pubviaroot) {
    throw new ServiceError(
      ErrorCode.PSYNC_S1143,
      `'${publicationName}' uses publish_via_partition_root, which is not supported.`
    );
  }
}

export async function checkTableRls(
  db: pgwire.PgClient,
  relationId: number
): Promise<{ canRead: boolean; message?: string }> {
  const rs = await lib_postgres.retriedQuery(db, {
    statement: `
WITH user_info AS (
    SELECT 
        current_user as username,
        r.rolsuper,
        r.rolbypassrls
    FROM pg_roles r 
    WHERE r.rolname = current_user
)
SELECT
    c.relname as tablename,
    c.relrowsecurity as rls_enabled,
    u.username as username,
    u.rolsuper as is_superuser,
    u.rolbypassrls as bypasses_rls
FROM pg_class c
CROSS JOIN user_info u
WHERE c.oid = $1::oid;
`,
    params: [{ type: 'int4', value: relationId }]
  });

  const rows = pgwire.pgwireRows<{
    rls_enabled: boolean;
    tablename: string;
    username: string;
    is_superuser: boolean;
    bypasses_rls: boolean;
  }>(rs);
  if (rows.length == 0) {
    // Not expected, since we already got the oid
    throw new ServiceAssertionError(`Table with OID ${relationId} does not exist.`);
  }
  const row = rows[0];
  if (row.is_superuser || row.bypasses_rls) {
    // Bypasses RLS automatically.
    return { canRead: true };
  }

  if (row.rls_enabled) {
    // Don't skip, since we _may_ still be able to get results.
    return {
      canRead: false,
      message: `[${ErrorCode.PSYNC_S1145}] Row Level Security is enabled on table "${row.tablename}". To make sure that ${row.username} can read the table, run: 'ALTER ROLE ${row.username} BYPASSRLS'.`
    };
  }

  return { canRead: true };
}

export interface GetDebugTablesInfoOptions {
  db: pgwire.PgClient;
  publicationName: string;
  connectionTag: string;
  tablePatterns: sync_rules.TablePattern[];
  syncRules: sync_rules.SyncConfig;
}

interface BatchedDebugTableRow {
  pattern_ord: number;
  schema_name: string;
  name: string;
  relation_id: number | null;
  replication_identity: ReplicationIdentity | null;
  replication_id_json: string;
  select_error: string | null;
  in_publication: boolean | null;
  rls_enabled: boolean | null;
  username: string | null;
  is_superuser: boolean | null;
  bypasses_rls: boolean | null;
}

export async function getDebugTablesInfoBatched(options: GetDebugTablesInfoOptions): Promise<PatternResult[]> {
  const { db, publicationName, connectionTag, tablePatterns, syncRules } = options;
  const patternPayload = JSON.stringify(
    tablePatterns.map((tablePattern, pattern_ord) => ({
      pattern_ord,
      schema_name: tablePattern.schema,
      table_pattern: tablePattern.tablePattern,
      is_wildcard: tablePattern.isWildcard,
      table_prefix: tablePattern.isWildcard ? tablePattern.tablePrefix : '',
      input_name: tablePattern.isWildcard ? tablePattern.tablePattern : tablePattern.name
    }))
  );

  let rows: BatchedDebugTableRow[];
  await db.query('BEGIN');
  try {
    // Anonymous DO blocks cannot take bind parameters directly, so pass the
    // pattern payload through transaction-local settings on the pinned connection.
    await db.query({
      statement: `SELECT set_config('powersync.debug.table_patterns', $1, true)`,
      params: [{ type: 'varchar', value: patternPayload }]
    });
    await db.query({
      statement: `SELECT set_config('powersync.debug.publication_name', $1, true)`,
      params: [{ type: 'varchar', value: publicationName }]
    });
    await db.query({
      statement: `DO $$
DECLARE
  input_patterns jsonb := current_setting('powersync.debug.table_patterns', true)::jsonb;
  current_publication text := current_setting('powersync.debug.publication_name', true);
  select_probe_results jsonb := '[]'::jsonb;
  debug_cursor refcursor := 'powersync_debug_tables_cursor';
  select_target record;
BEGIN
  -- Capture per-table read failures without aborting the whole batch.
  FOR select_target IN
    WITH patterns AS (
      SELECT *
      FROM jsonb_to_recordset(input_patterns) AS p(
        pattern_ord int,
        schema_name text,
        table_pattern text,
        is_wildcard boolean,
        table_prefix text,
        input_name text
      )
    )
    SELECT
      p.pattern_ord,
      p.schema_name,
      c.relname AS table_name,
      c.oid::int8 AS relid
    FROM patterns p
    JOIN pg_namespace n ON n.nspname = p.schema_name
    JOIN pg_class c ON c.relnamespace = n.oid
    WHERE c.relkind = 'r'
      AND (
        (p.is_wildcard AND c.relname LIKE p.table_pattern AND left(c.relname, length(p.table_prefix)) = p.table_prefix)
        OR
        (NOT p.is_wildcard AND c.relname = p.table_pattern)
      )
    ORDER BY p.pattern_ord, c.relname
  LOOP
    BEGIN
      EXECUTE format('SELECT 1 FROM %I.%I LIMIT 1', select_target.schema_name, select_target.table_name);
      select_probe_results := select_probe_results || jsonb_build_array(
        jsonb_build_object(
          'pattern_ord', select_target.pattern_ord,
          'relid', select_target.relid,
          'message', NULL
        )
      );
    EXCEPTION WHEN OTHERS THEN
      select_probe_results := select_probe_results || jsonb_build_array(
        jsonb_build_object(
          'pattern_ord', select_target.pattern_ord,
          'relid', select_target.relid,
          'message', SQLERRM
        )
      );
    END;
  END LOOP;

  -- OPEN debug_cursor to allow returning results
  OPEN debug_cursor FOR
    WITH patterns AS (
      -- Rehydrate the requested patterns once for the bulk metadata pass.
      SELECT *
      FROM jsonb_to_recordset(input_patterns) AS p(
        pattern_ord int,
        schema_name text,
        table_pattern text,
        is_wildcard boolean,
        table_prefix text,
        input_name text
      )
    ),
    matched AS (
      -- Expand each requested pattern to the concrete tables that exist.
      SELECT
        p.pattern_ord,
        p.schema_name,
        p.table_pattern,
        p.is_wildcard,
        p.input_name,
        c.relid,
        c.table_name
      FROM patterns p
      LEFT JOIN LATERAL (
        SELECT
          c.oid::int8 AS relid,
          c.relname AS table_name
        FROM pg_namespace n
        JOIN pg_class c ON c.relnamespace = n.oid
        WHERE n.nspname = p.schema_name
          AND c.relkind = 'r'
          AND (
            (p.is_wildcard AND c.relname LIKE p.table_pattern AND left(c.relname, length(p.table_prefix)) = p.table_prefix)
            OR
            (NOT p.is_wildcard AND c.relname = p.table_pattern)
          )
        ORDER BY c.relname
      ) c ON true
      WHERE NOT p.is_wildcard OR c.relid IS NOT NULL
    ),
    existing AS (
      -- Work from distinct relation ids for catalog lookups.
      SELECT DISTINCT relid::oid AS relid
      FROM matched
      WHERE relid IS NOT NULL
    ),
    replication_identity AS (
      -- Resolve the replica identity mode for each existing table.
      SELECT
        c.oid::int8 AS relid,
        CASE c.relreplident
          WHEN 'd' THEN 'default'
          WHEN 'n' THEN 'nothing'
          WHEN 'f' THEN 'full'
          WHEN 'i' THEN 'index'
        END AS replication_identity
      FROM pg_class c
      JOIN existing e ON e.relid = c.oid
    ),
    replication_columns AS (
      -- Resolve the actual replication-id columns for default/index/full modes.
      SELECT
        r.relid,
        a.attname AS name,
        a.attnum AS attnum
      FROM replication_identity r
      JOIN pg_index i ON i.indrelid = r.relid::oid AND i.indisprimary
      JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
      WHERE r.replication_identity = 'default' AND a.attnum > 0

      UNION ALL

      SELECT
        r.relid,
        a.attname AS name,
        a.attnum AS attnum
      FROM replication_identity r
      JOIN pg_index i ON i.indrelid = r.relid::oid AND i.indisreplident
      JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
      WHERE r.replication_identity = 'index' AND a.attnum > 0

      UNION ALL

      SELECT
        r.relid,
        a.attname AS name,
        a.attnum AS attnum
      FROM replication_identity r
      JOIN pg_attribute a ON a.attrelid = r.relid::oid
      WHERE r.replication_identity = 'full' AND a.attnum > 0
    ),
    publication_membership AS (
      -- Check publication membership in bulk instead of once per table.
      SELECT
        m.pattern_ord,
        m.relid
      FROM matched m
      JOIN pg_publication_tables p
        ON p.pubname = current_publication
       AND p.schemaname = m.schema_name
       AND p.tablename = m.table_name
      WHERE m.relid IS NOT NULL
    ),
    user_info AS (
      -- Current-role capabilities are constant across all tables in this batch.
      SELECT
        current_user AS username,
        r.rolsuper AS is_superuser,
        r.rolbypassrls AS bypasses_rls
      FROM pg_roles r
      WHERE r.rolname = current_user
    ),
    rls_check AS (
      -- Pair role capabilities with per-table row-security metadata.
      SELECT
        e.relid::int8 AS relid,
        c.relrowsecurity AS rls_enabled,
        u.username,
        u.is_superuser,
        u.bypasses_rls
      FROM existing e
      JOIN pg_class c ON c.oid = e.relid
      CROSS JOIN user_info u
    ),
    select_probes AS (
      -- Rehydrate the recorded SELECT probe failures for the final result set.
      SELECT *
      FROM jsonb_to_recordset(select_probe_results) AS s(
        pattern_ord int,
        relid int8,
        message text
      )
    )
    SELECT
      m.pattern_ord,
      m.schema_name,
      COALESCE(m.table_name, m.input_name) AS name,
      m.relid AS relation_id,
      ri.replication_identity,
      COALESCE(
        jsonb_agg(rc.name ORDER BY rc.attnum) FILTER (WHERE rc.name IS NOT NULL),
        '[]'::jsonb
      )::text AS replication_id_json,
      sp.message AS select_error,
      (pm.relid IS NOT NULL) AS in_publication,
      rls.rls_enabled,
      rls.username,
      rls.is_superuser,
      rls.bypasses_rls
    FROM matched m
    LEFT JOIN replication_identity ri ON ri.relid = m.relid
    LEFT JOIN replication_columns rc ON rc.relid = m.relid
    LEFT JOIN publication_membership pm ON pm.pattern_ord = m.pattern_ord AND pm.relid = m.relid
    LEFT JOIN rls_check rls ON rls.relid = m.relid
    LEFT JOIN select_probes sp ON sp.pattern_ord = m.pattern_ord AND sp.relid = m.relid
    GROUP BY
      m.pattern_ord,
      m.schema_name,
      COALESCE(m.table_name, m.input_name),
      m.relid,
      ri.replication_identity,
      sp.message,
      pm.relid,
      rls.rls_enabled,
      rls.username,
      rls.is_superuser,
      rls.bypasses_rls
    ORDER BY m.pattern_ord, COALESCE(m.table_name, m.input_name);
END
$$`
    });

    const fetched = await db.query(`FETCH ALL FROM powersync_debug_tables_cursor`);
    rows = pgwire.pgwireRows<BatchedDebugTableRow>(fetched);
    await db.query('COMMIT');
  } catch (e) {
    try {
      await db.query('ROLLBACK');
    } catch {
      // Ignore rollback errors after a failed transaction.
    }
    throw e;
  }

  const result: PatternResult[] = tablePatterns.map((tablePattern) => ({
    schema: tablePattern.schema,
    pattern: tablePattern.tablePattern,
    wildcard: tablePattern.isWildcard,
    ...(tablePattern.isWildcard ? { tables: [] } : {})
  }));

  for (const row of rows) {
    const tablePattern = tablePatterns[row.pattern_ord];
    const idColumns = JSON.parse(row.replication_id_json) as string[];
    const sourceTable = new storage.SourceTable({
      id: '',
      connectionTag,
      objectId: row.relation_id ?? 0,
      schema: tablePattern.schema,
      name: row.name,
      replicaIdColumns: idColumns.map((name) => ({ name })),
      snapshotComplete: true
    });
    const syncData = syncRules.tableSyncsData(sourceTable);
    const syncParameters = syncRules.tableSyncsParameters(sourceTable);

    let errors: service_types.ReplicationError[];
    if (row.relation_id == null) {
      errors = [{ level: 'warning', message: `Table ${sourceTable.qualifiedName} not found.` }];
    } else {
      const idColumnsError =
        idColumns.length == 0 && row.replication_identity != 'nothing'
          ? {
              level: 'fatal' as const,
              message: `No replication id found for ${sourceTable.qualifiedName}. Replica identity: ${row.replication_identity}.${row.replication_identity == 'default' ? ' Configure a primary key on the table.' : ''}`
            }
          : null;
      const selectError = row.select_error == null ? null : { level: 'fatal' as const, message: row.select_error };
      const replicateError =
        row.in_publication === false
          ? {
              level: 'fatal' as const,
              message: `Table ${sourceTable.qualifiedName} is not part of publication '${publicationName}'. Run: \`ALTER PUBLICATION ${publicationName} ADD TABLE ${sourceTable.qualifiedName}\`.`
            }
          : null;
      const rlsError =
        row.rls_enabled && !row.is_superuser && !row.bypasses_rls && row.username != null
          ? {
              level: 'warning' as const,
              message: `[${ErrorCode.PSYNC_S1145}] Row Level Security is enabled on table "${row.name}". To make sure that ${row.username} can read the table, run: 'ALTER ROLE ${row.username} BYPASSRLS'.`
            }
          : null;

      errors = [idColumnsError, selectError, replicateError, rlsError].filter(
        (error) => error != null
      ) as service_types.ReplicationError[];
    }

    const tableInfo: service_types.TableInfo = {
      schema: tablePattern.schema,
      name: row.name,
      pattern: tablePattern.isWildcard ? tablePattern.tablePattern : undefined,
      replication_id: idColumns,
      data_queries: syncData,
      parameter_queries: syncParameters,
      errors
    };

    if (tablePattern.isWildcard) {
      result[row.pattern_ord].tables!.push(tableInfo);
    } else {
      result[row.pattern_ord].table = tableInfo;
    }
  }

  return result;
}

export interface GetDebugTableInfoOptions {
  db: pgwire.PgClient;
  name: string;
  publicationName: string;
  connectionTag: string;
  tablePattern: sync_rules.TablePattern;
  relationId: number | null;
  syncRules: sync_rules.SyncConfig;
}

export async function getDebugTableInfo(options: GetDebugTableInfoOptions): Promise<service_types.TableInfo> {
  const { db, name, publicationName, connectionTag, tablePattern, relationId, syncRules } = options;
  const schema = tablePattern.schema;
  let id_columns_result: ReplicaIdentityResult | undefined = undefined;
  let id_columns_error = null;

  if (relationId != null) {
    try {
      id_columns_result = await getReplicationIdentityColumns(db, relationId);
    } catch (e) {
      id_columns_error = { level: 'fatal', message: e.message };
    }
  }

  const id_columns = id_columns_result?.replicationColumns ?? [];

  const sourceTable = new storage.SourceTable({
    id: '', // not used
    connectionTag: connectionTag,
    objectId: relationId ?? 0,
    schema: schema,
    name: name,
    replicaIdColumns: id_columns,
    snapshotComplete: true
  });

  const syncData = syncRules.tableSyncsData(sourceTable);
  const syncParameters = syncRules.tableSyncsParameters(sourceTable);

  if (relationId == null) {
    return {
      schema: schema,
      name: name,
      pattern: tablePattern.isWildcard ? tablePattern.tablePattern : undefined,
      replication_id: [],
      data_queries: syncData,
      parameter_queries: syncParameters,
      // Also
      errors: [{ level: 'warning', message: `Table ${sourceTable.qualifiedName} not found.` }]
    };
  }
  if (id_columns.length == 0 && id_columns_error == null) {
    let message = `No replication id found for ${sourceTable.qualifiedName}. Replica identity: ${id_columns_result?.replicationIdentity}.`;
    if (id_columns_result?.replicationIdentity == 'default') {
      message += ' Configure a primary key on the table.';
    }
    id_columns_error = { level: 'fatal', message };
  }

  let selectError = null;
  try {
    await lib_postgres.retriedQuery(db, `SELECT * FROM ${sourceTable.qualifiedName} LIMIT 1`);
  } catch (e) {
    selectError = { level: 'fatal', message: e.message };
  }

  let replicateError = null;

  const publications = await lib_postgres.retriedQuery(db, {
    statement: `SELECT tablename FROM pg_publication_tables WHERE pubname = $1 AND schemaname = $2 AND tablename = $3`,
    params: [
      { type: 'varchar', value: publicationName },
      { type: 'varchar', value: tablePattern.schema },
      { type: 'varchar', value: name }
    ]
  });
  if (publications.rows.length == 0) {
    replicateError = {
      level: 'fatal',
      message: `Table ${sourceTable.qualifiedName} is not part of publication '${publicationName}'. Run: \`ALTER PUBLICATION ${publicationName} ADD TABLE ${sourceTable.qualifiedName}\`.`
    };
  }

  const rlsCheck = await checkTableRls(db, relationId);
  const rlsError = rlsCheck.canRead ? null : { message: rlsCheck.message!, level: 'warning' };

  return {
    schema: schema,
    name: name,
    pattern: tablePattern.isWildcard ? tablePattern.tablePattern : undefined,
    replication_id: id_columns.map((c) => c.name),
    data_queries: syncData,
    parameter_queries: syncParameters,
    errors: [id_columns_error, selectError, replicateError, rlsError].filter(
      (error) => error != null
    ) as service_types.ReplicationError[]
  };
}

export async function cleanUpReplicationSlot(slotName: string, db: pgwire.PgClient): Promise<void> {
  logger.info(`Cleaning up Postgres replication slot: ${slotName}...`);

  await db.query({
    statement: 'SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1',
    params: [{ type: 'varchar', value: slotName }]
  });
}
