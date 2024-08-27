import * as pgwire from '@powersync/service-jpgwire';
import * as sync_rules from '@powersync/service-sync-rules';
import { ReplicationError, TableInfo } from '@powersync/service-types';
import { PatternResult, SourceTable } from '@powersync/service-core';
import { getReplicationIdentityColumns, ReplicaIdentityResult } from './replication-utils.js';
import * as util from '../utils/pgwire_utils.js';

/**
 * Connection that _manages_ WAL, but does not do streaming.
 */
export class WalConnection {
  db: pgwire.PgClient;
  connectionTag = sync_rules.DEFAULT_TAG;
  publication_name = 'powersync';

  sync_rules: sync_rules.SqlSyncRules;

  /**
   * db can be a PgConnection or PgPool.
   *
   * No transactions are used here, but it is up to the client to ensure
   * nothing here is called in the middle of another transaction if
   * PgConnection is used.
   */
  constructor(options: { db: pgwire.PgClient; sync_rules: sync_rules.SqlSyncRules }) {
    this.db = options.db;
    this.sync_rules = options.sync_rules;
  }

  async getDebugTableInfo(
    tablePattern: sync_rules.TablePattern,
    name: string,
    relationId: number | null
  ): Promise<TableInfo> {
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

    const id_columns = id_columns_result?.replicationColumns ?? [];

    const sourceTable = new SourceTable(0, this.connectionTag, relationId ?? 0, schema, name, id_columns, true);

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

  async getDebugTablesInfo(tablePatterns: sync_rules.TablePattern[]): Promise<PatternResult[]> {
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

        for (let row of pgwire.pgwireRows(results)) {
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
          patternResult.table = await this.getDebugTableInfo(tablePattern, tablePattern.name, null);
        } else {
          const row = pgwire.pgwireRows(results)[0];
          const name = row.table_name as string;
          const relationId = row.relid as number;
          patternResult.table = await this.getDebugTableInfo(tablePattern, name, relationId);
        }
      }
    }
    return result;
  }
}
