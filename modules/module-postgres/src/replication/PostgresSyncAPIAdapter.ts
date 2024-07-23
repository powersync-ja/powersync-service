import * as pgwire from '@powersync/service-jpgwire';
import { api, replication, storage } from '@powersync/service-core';

import { DEFAULT_TAG, TablePattern } from '@powersync/service-sync-rules';
import { configFile, DatabaseSchema, internal_routes, ReplicationError, TableInfo } from '@powersync/service-types';

import * as pg_utils from '../utils/pgwire_utils.js';
import * as replication_utils from '../replication/replication-utils.js';
import { baseUri, ResolvedConnectionConfig } from '../types/types.js';

export class PostgresSyncAPIAdapter implements api.SyncAPI {
  protected pool: pgwire.PgClient;

  // TODO this should probably be configurable one day
  connectionTag = DEFAULT_TAG;
  publication_name = 'powersync';

  constructor(protected config: ResolvedConnectionConfig) {
    this.pool = pgwire.connectPgWirePool(config, {
      idleTimeout: 30_000
    });
  }

  async shutdown(): Promise<void> {
    await this.pool.end();
  }

  async getSourceConfig(): Promise<configFile.DataSourceConfig> {
    return this.config;
  }

  async getConnectionStatus(): Promise<api.ConnectionStatusResponse> {
    const base = {
      id: this.config.id,
      postgres_uri: baseUri(this.config)
    };

    try {
      await pg_utils.retriedQuery(this.pool, `SELECT 'PowerSync connection test'`);
    } catch (e) {
      return {
        ...base,
        connected: false,
        errors: [{ level: 'fatal', message: e.message }]
      };
    }

    try {
      await replication_utils.checkSourceConfiguration(this.pool);
    } catch (e) {
      return {
        ...base,
        connected: true,
        errors: [{ level: 'fatal', message: e.message }]
      };
    }

    return {
      ...base,
      connected: true,
      errors: []
    };
  }

  executeQuery(query: string, params: any[]): Promise<internal_routes.ExecuteSqlResponse> {
    throw new Error('Method not implemented.');
  }

  getDemoCredentials(): Promise<api.DemoCredentials> {
    throw new Error('Method not implemented.');
  }

  getDiagnostics(): Promise<{ connected: boolean; errors?: Array<{ level: string; message: string }> }> {
    throw new Error('Method not implemented.');
  }

  async getDebugTablesInfo(tablePatterns: TablePattern[]): Promise<replication.PatternResult[]> {
    let result: replication.PatternResult[] = [];

    for (let tablePattern of tablePatterns) {
      const schema = tablePattern.schema;

      let patternResult: replication.PatternResult = {
        schema: schema,
        pattern: tablePattern.tablePattern,
        wildcard: tablePattern.isWildcard
      };
      result.push(patternResult);

      if (tablePattern.isWildcard) {
        patternResult.tables = [];
        const prefix = tablePattern.tablePrefix;
        const results = await pg_utils.retriedQuery(this.pool, {
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
        const results = await pg_utils.retriedQuery(this.pool, {
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
          const row = pgwire.pgwireRows(results)[0];
          const name = row.table_name as string;
          const relationId = row.relid as number;
          patternResult.table = await this.getDebugTableInfo(tablePattern, name, relationId);
        }
      }
    }
    return result;
  }

  protected async getDebugTableInfo(
    tablePattern: TablePattern,
    name: string,
    relationId: number | null
  ): Promise<TableInfo> {
    const schema = tablePattern.schema;
    let id_columns_result: replication_utils.ReplicaIdentityResult | undefined = undefined;
    let id_columns_error = null;

    if (relationId != null) {
      try {
        id_columns_result = await replication_utils.getReplicationIdentityColumns(this.pool, relationId);
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
      await pg_utils.retriedQuery(this.pool, `SELECT * FROM ${sourceTable.escapedIdentifier} LIMIT 1`);
    } catch (e) {
      selectError = { level: 'fatal', message: e.message };
    }

    let replicateError = null;

    const publications = await pg_utils.retriedQuery(this.pool, {
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

  async getReplicationLag(slotName: string): Promise<number> {
    const results = await pg_utils.retriedQuery(this.pool, {
      statement: `SELECT
  slot_name,
  confirmed_flush_lsn, 
  pg_current_wal_lsn(), 
  (pg_current_wal_lsn() - confirmed_flush_lsn) AS lsn_distance
FROM pg_replication_slots WHERE slot_name = $1 LIMIT 1;`,
      params: [{ type: 'varchar', value: slotName }]
    });
    const [row] = pgwire.pgwireRows(results);
    if (row) {
      return Number(row.lsn_distance);
    }

    throw new Error(`Could not determine replication lag for slot ${slotName}`);
  }

  getCheckpoint(): Promise<bigint> {
    throw new Error('Method not implemented.');
  }

  getConnectionSchema(): Promise<DatabaseSchema[]> {
    throw new Error('Method not implemented.');
  }

  executeSQL(sql: string, params: any[]): Promise<internal_routes.ExecuteSqlResponse> {
    throw new Error('Method not implemented.');
  }
}
