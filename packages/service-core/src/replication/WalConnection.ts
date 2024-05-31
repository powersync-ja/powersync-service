import * as pgwire from '@powersync/service-jpgwire';
import { pgwireRows } from '@powersync/service-jpgwire';
import { DEFAULT_TAG, SqlSyncRules, TablePattern } from '@powersync/service-sync-rules';
import { ReplicationError, TableInfo } from '@powersync/service-types';

import * as storage from '@/storage/storage-index.js';
import * as util from '@/util/util-index.js';

import { ReplicaIdentityResult, getReplicationIdentityColumns } from './util.js';
/**
 * Connection that _manages_ WAL, but does not do streaming.
 */
export class WalConnection {
  db: pgwire.PgClient;
  connectionTag = DEFAULT_TAG;
  publication_name = 'powersync';

  sync_rules: SqlSyncRules;

  /**
   * db can be a PgConnection or PgPool.
   *
   * No transactions are used here, but it is up to the client to ensure
   * nothing here is called in the middle of another transaction if
   * PgConnection is used.
   */
  constructor(options: { db: pgwire.PgClient; sync_rules: SqlSyncRules }) {
    this.db = options.db;
    this.sync_rules = options.sync_rules;
  }

  async checkSourceConfiguration() {
    await checkSourceConfiguration(this.db);
  }

  async getDebugTableInfo(tablePattern: TablePattern, name: string, relationId: number | null): Promise<TableInfo> {
    const schema = tablePattern.schema;
    let id_columns_result: ReplicaIdentityResult | undefined = undefined;
    let id_columns_error = null;

    if (relationId != null) {
      try {
        id_columns_result = await getReplicationIdentityColumns(this.db, relationId);
      } catch (e) {
        id_columns_error = { level: 'fatal', message: e.message };
      }
    }

    const id_columns = id_columns_result?.columns ?? [];

    const sourceTable = new storage.SourceTable(0, this.connectionTag, relationId ?? 0, schema, name, id_columns, true);

    const syncData = this.sync_rules.tableSyncsData(sourceTable);
    const syncParameters = this.sync_rules.tableSyncsParameters(sourceTable);

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
      await util.retriedQuery(this.db, `SELECT * FROM ${sourceTable.escapedIdentifier} LIMIT 1`);
    } catch (e) {
      selectError = { level: 'fatal', message: e.message };
    }

    let replicateError = null;

    const publications = await util.retriedQuery(this.db, {
      statement: `SELECT tablename FROM pg_publication_tables WHERE pubname = $1 AND schemaname = $2 AND tablename = $3`,
      params: [
        { type: 'varchar', value: this.publication_name },
        { type: 'varchar', value: tablePattern.schema },
        { type: 'varchar', value: name }
      ]
    });
    if (publications.rows.length == 0) {
      replicateError = {
        level: 'fatal',
        message: `Table ${sourceTable.qualifiedName} is not part of publication '${this.publication_name}'. Run: \`ALTER PUBLICATION ${this.publication_name} ADD TABLE ${sourceTable.qualifiedName}\`.`
      };
    }

    return {
      schema: schema,
      name: name,
      pattern: tablePattern.isWildcard ? tablePattern.tablePattern : undefined,
      replication_id: id_columns.map((c) => c.name),
      data_queries: syncData,
      parameter_queries: syncParameters,
      errors: [id_columns_error, selectError, replicateError].filter((error) => error != null) as ReplicationError[]
    };
  }

  async getDebugTablesInfo(tablePatterns: TablePattern[]): Promise<PatternResult[]> {
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
        const results = await util.retriedQuery(this.db, {
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

        for (let row of pgwireRows(results)) {
          const name = row.table_name as string;
          const relationId = row.relid as number;
          if (!name.startsWith(prefix)) {
            continue;
          }
          const details = await this.getDebugTableInfo(tablePattern, name, relationId);
          patternResult.tables.push(details);
        }
      } else {
        const results = await util.retriedQuery(this.db, {
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
          const details = await this.getDebugTableInfo(tablePattern, tablePattern.name, null);
          patternResult.table = details;
        } else {
          const row = pgwireRows(results)[0];
          const name = row.table_name as string;
          const relationId = row.relid as number;
          patternResult.table = await this.getDebugTableInfo(tablePattern, name, relationId);
        }
      }
    }
    return result;
  }
}

export interface PatternResult {
  schema: string;
  pattern: string;
  wildcard: boolean;
  tables?: TableInfo[];
  table?: TableInfo;
}

export async function checkSourceConfiguration(db: pgwire.PgClient) {
  // TODO: configurable
  const publication_name = 'powersync';

  // Check basic config
  await util.retriedQuery(
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
  const rs = await util.retriedQuery(db, {
    statement: `SELECT * FROM pg_publication WHERE pubname = $1`,
    params: [{ type: 'varchar', value: publication_name }]
  });
  const row = pgwireRows(rs)[0];
  if (row == null) {
    throw new Error(
      `Publication '${publication_name}' does not exist. Run: \`CREATE PUBLICATION ${publication_name} FOR ALL TABLES\`, or read the documentation for details.`
    );
  }
  if (row.pubinsert == false || row.pubupdate == false || row.pubdelete == false || row.pubtruncate == false) {
    throw new Error(
      `Publication '${publication_name}' does not publish all changes. Create a publication using \`WITH (publish = "insert, update, delete, truncate")\` (the default).`
    );
  }
  if (row.pubviaroot) {
    throw new Error(`'${publication_name}' uses publish_via_partition_root, which is not supported.`);
  }
}
