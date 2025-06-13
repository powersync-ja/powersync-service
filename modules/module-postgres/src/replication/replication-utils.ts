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
      name: row[0] as string,
      typeId: row[1] as number
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
      name: row[0] as string,
      typeId: row[1] as number
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
  const idType: string = rows.rows[0]?.[0];
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
  syncRules: sync_rules.SqlSyncRules;
}

export async function getDebugTablesInfo(options: GetDebugTablesInfoOptions): Promise<PatternResult[]> {
  const { db, publicationName, connectionTag, tablePatterns, syncRules } = options;
  let result: PatternResult[] = [];

  for (let tablePattern of tablePatterns) {
    const schema = tablePattern.schema;

    let patternResult: PatternResult = {
      schema: schema,
      pattern: tablePattern.tablePattern,
      wildcard: tablePattern.isWildcard
    };
    result.push(patternResult);

    if (tablePattern.isWildcard) {
      patternResult.tables = [];
      const prefix = tablePattern.tablePrefix;
      const results = await lib_postgres.retriedQuery(db, {
        statement: `SELECT c.oid AS relid, c.relname AS table_name
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = $1
        AND c.relkind = 'r'
        AND c.relname LIKE $2`,
        params: [
          { type: 'varchar', value: schema },
          { type: 'varchar', value: tablePattern.tablePattern }
        ]
      });

      for (let row of pgwire.pgwireRows(results)) {
        const name = row.table_name as string;
        const relationId = row.relid as number;
        if (!name.startsWith(prefix)) {
          continue;
        }
        const details = await getDebugTableInfo({
          db,
          name,
          publicationName,
          connectionTag,
          tablePattern,
          relationId,
          syncRules: syncRules
        });
        patternResult.tables.push(details);
      }
    } else {
      const results = await lib_postgres.retriedQuery(db, {
        statement: `SELECT c.oid AS relid, c.relname AS table_name
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = $1
        AND c.relkind = 'r'
        AND c.relname = $2`,
        params: [
          { type: 'varchar', value: schema },
          { type: 'varchar', value: tablePattern.tablePattern }
        ]
      });
      if (results.rows.length == 0) {
        // Table not found
        patternResult.table = await getDebugTableInfo({
          db,
          name: tablePattern.name,
          publicationName,
          connectionTag,
          tablePattern,
          relationId: null,
          syncRules: syncRules
        });
      } else {
        const row = pgwire.pgwireRows(results)[0];
        const name = row.table_name as string;
        const relationId = row.relid as number;
        patternResult.table = await getDebugTableInfo({
          db,
          name,
          publicationName,
          connectionTag,
          tablePattern,
          relationId,
          syncRules: syncRules
        });
      }
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
  syncRules: sync_rules.SqlSyncRules;
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

  const sourceTable = new storage.SourceTable(0, connectionTag, relationId ?? 0, schema, name, id_columns, true);

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
    await lib_postgres.retriedQuery(db, `SELECT * FROM ${sourceTable.escapedIdentifier} LIMIT 1`);
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
