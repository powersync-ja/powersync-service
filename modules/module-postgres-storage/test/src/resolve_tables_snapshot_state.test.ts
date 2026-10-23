import * as lib_postgres from '@powersync/lib-service-postgres';
import { logger } from '@powersync/lib-services-framework';
import { storage } from '@powersync/service-core';
import { DEFAULT_HYDRATION_STATE, nodeSqlite, SqlSyncRules } from '@powersync/service-sync-rules';
import * as sqlite from 'node:sqlite';
import { describe, expect, test } from 'vitest';
import { PostgresBucketBatch } from '../../src/storage/batch/PostgresBucketBatch.js';

const SYNC_CONFIG = `
streams:
  stream:
    query: SELECT * FROM synced_table

config:
  edition: 3
`;

const USED_TABLE = {
  connectionTag: 'default',
  schema: 'public',
  name: 'synced_table',
  objectId: 1,
  replicaIdColumns: [{ name: 'id', type: 'uuid', typeId: 2950 }],
  sendsCompleteRows: true
};

const UNUSED_TABLE = {
  ...USED_TABLE,
  name: 'unsynced_table',
  objectId: 2
};

type SqlParam = { type: string; value: unknown };

interface RecordedQuery {
  statement: string;
  params: SqlParam[];
}

/**
 * Minimal stand-in for the Postgres connection used by resolveTables().
 *
 * Only the sql template tag and transaction wrapper are implemented. Responses are
 * produced by the caller, so a test can decide which rows the database returns.
 */
function createStubDb(rowsFor: (statement: string) => Record<string, unknown>[]) {
  const queries: RecordedQuery[] = [];

  const connection = {
    sql: (strings: TemplateStringsArray, ...params: SqlParam[]) => {
      const statement = strings.join('?');
      queries.push({ statement, params });
      const rows = () => rowsFor(statement);
      return {
        execute: async () => rows(),
        rows: async () => rows(),
        first: async () => rows()[0] ?? null,
        decoded: <T>(codec: { decode(value: unknown): T }) => ({
          first: async () => {
            const row = rows()[0];
            return row == null ? null : codec.decode(row);
          },
          rows: async () => rows().map((row) => codec.decode(row))
        })
      };
    }
  };

  const db = {
    ...connection,
    transaction: async <T>(callback: (tx: typeof connection) => Promise<T>) => callback(connection)
  };

  return { db: db as unknown as lib_postgres.DatabaseClient, queries };
}

function tableRow(id: string, tableName: string, snapshotDone: boolean): Record<string, unknown> {
  return {
    id,
    group_id: 1,
    connection_id: 1,
    relation_id: JSON.stringify({ object_id: 2 }),
    schema_name: 'public',
    table_name: tableName,
    replica_id_columns: JSON.stringify([{ name: 'id', type: 'uuid', type_oid: 2950 }]),
    snapshot_done: snapshotDone,
    snapshot_total_estimated_count: null,
    snapshot_replicated_count: null,
    snapshot_last_key: null,
    source_metadata: null
  };
}

function createBatch(db: lib_postgres.DatabaseClient) {
  const syncConfig = SqlSyncRules.fromYaml(SYNC_CONFIG, { defaultSchema: 'public' }).config.hydrate({
    hydrationState: DEFAULT_HYDRATION_STATE,
    sqlite: nodeSqlite(sqlite)
  });

  return new PostgresBucketBatch({
    logger: logger.child({}),
    db,
    sync_rules: syncConfig,
    replicationStreamId: 1,
    replicationStreamName: 'test',
    last_checkpoint_lsn: null,
    store_current_data: true,
    resumeFromLsn: null,
    skip_existing_rows: false,
    batch_limits: {
      max_current_data_batch_size: 1_000_000,
      max_estimated_size: 1_000_000,
      max_record_count: 100
    },
    markRecordUnavailable: undefined,
    hooks: undefined,
    storageConfig: storage.STORAGE_VERSION_CONFIG[storage.STORAGE_VERSION_1]!
  });
}

const snapshotDoneUpdates = (queries: RecordedQuery[]) =>
  queries.filter((query) => query.statement.includes('UPDATE source_tables'));

describe('PostgresBucketBatch.resolveTables snapshot state', () => {
  test('marks a newly resolved table that the sync config does not use as snapshotted', async () => {
    const { db, queries } = createStubDb((statement) => {
      if (statement.includes('INSERT INTO')) {
        return [tableRow('table-1', 'unsynced_table', false)];
      }
      return [];
    });

    const batch = createBatch(db);
    const result = await batch.resolveTables({ connection_id: 1, source: UNUSED_TABLE });

    expect(result.tables).toHaveLength(1);
    expect(result.tables[0].syncAny).toBe(false);
    expect(result.tables[0].snapshotComplete).toBe(true);

    const updates = snapshotDoneUpdates(queries);
    expect(updates).toHaveLength(1);
    expect(updates[0].statement).toContain('snapshot_done = TRUE');
    expect(updates[0].params).toEqual([{ type: 'varchar', value: 'table-1' }]);
  });

  test('keeps a table that the sync config uses pending a snapshot', async () => {
    const { db, queries } = createStubDb((statement) => {
      if (statement.includes('INSERT INTO')) {
        return [tableRow('table-1', 'synced_table', false)];
      }
      return [];
    });

    const batch = createBatch(db);
    const result = await batch.resolveTables({ connection_id: 1, source: USED_TABLE });

    expect(result.tables).toHaveLength(1);
    expect(result.tables[0].syncAny).toBe(true);
    expect(result.tables[0].snapshotComplete).toBe(false);
    expect(snapshotDoneUpdates(queries)).toHaveLength(0);
  });

  test('marks an existing table that the sync config does not use as snapshotted', async () => {
    const { db, queries } = createStubDb((statement) => {
      if (statement.includes('SELECT')) {
        return [tableRow('table-1', 'unsynced_table', false)];
      }
      return [];
    });

    const batch = createBatch(db);
    const result = await batch.resolveTables({ connection_id: 1, source: UNUSED_TABLE });

    expect(result.tables).toHaveLength(1);
    expect(result.tables[0].snapshotComplete).toBe(true);
    expect(snapshotDoneUpdates(queries)).toHaveLength(1);
  });
});
