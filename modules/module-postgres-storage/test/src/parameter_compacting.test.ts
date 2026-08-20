import * as lib_postgres from '@powersync/lib-service-postgres';
import { updateSyncRulesFromYaml } from '@powersync/service-core';
import { test_utils } from '@powersync/service-core-tests';
import { describe, expect, test } from 'vitest';
import { PostgresParameterCompactor } from '../../src/storage/PostgresParameterCompactor.js';
import type { PostgresSyncRulesStorage } from '../../src/storage/PostgresSyncRulesStorage.js';
import { POSTGRES_STORAGE_FACTORY } from './util.js';

const PARAMETER_RULES = `
bucket_definitions:
  test:
    parameters: select id from test where id = request.user_id()
    data: []
`;

const LOOKUP = Buffer.from('lookup').toString('hex');

async function createActiveStorage() {
  const factory = await POSTGRES_STORAGE_FACTORY.factory();
  const syncRules = await factory.updateSyncRules(updateSyncRulesFromYaml(PARAMETER_RULES));
  const processingStorage = factory.getInstance(syncRules);
  await using writer = await processingStorage.createWriter(test_utils.BATCH_OPTIONS);
  await writer.markAllSnapshotDone('1/1');
  await writer.commit('1/1');

  const active = await factory.getActiveSyncConfig();
  if (active == null) {
    throw new Error('Expected an active sync config');
  }
  return {
    factory,
    storage: active.storage as PostgresSyncRulesStorage,
    groupId: syncRules.replicationStreamId
  };
}

type ParameterRowInput = {
  /** Serialized: JSON has no bigint. */
  id: string;
  group_id: number;
  source_key: string;
  bucket_parameters: string;
};

/** Inserts parameter entries with explicit operation ids, all sharing {@link LOOKUP}. */
async function insertParameterRows(db: lib_postgres.DatabaseClient, rows: ParameterRowInput[]) {
  await db.sql`
    INSERT INTO
      bucket_parameters (
        id,
        group_id,
        source_table,
        source_key,
        lookup,
        bucket_parameters
      )
    SELECT
      id,
      group_id,
      'test_table',
      decode(source_key, 'hex'),
      decode(${{ type: 'varchar', value: LOOKUP }}, 'hex'),
      bucket_parameters
    FROM
      json_to_recordset(${{ type: 'json', value: rows }}::json) AS t (
        id bigint,
        group_id integer,
        source_key text,
        bucket_parameters text
      )
  `.execute();
}

function entry(id: bigint, groupId: number, sourceKey: string, bucketParameters: unknown[]): ParameterRowInput {
  return {
    id: id.toString(),
    group_id: groupId,
    source_key: Buffer.from(sourceKey).toString('hex'),
    bucket_parameters: JSON.stringify(bucketParameters)
  };
}

async function parameterIds(db: lib_postgres.DatabaseClient): Promise<bigint[]> {
  const rows = await db.sql`
    SELECT
      id
    FROM
      bucket_parameters
    ORDER BY
      id ASC
  `.rows<{ id: bigint }>();
  return rows.map((row) => row.id);
}

async function compactedBefore(db: lib_postgres.DatabaseClient, groupId: number): Promise<bigint | null> {
  const row = await db.sql`
    SELECT
      parameter_compacted_before
    FROM
      sync_rules
    WHERE
      id = ${{ type: 'int4', value: groupId }}
  `.first<{ parameter_compacted_before: bigint | null }>();
  return row!.parameter_compacted_before;
}

describe('Postgres parameter compaction', () => {
  test('compacts incrementally and persists the cursor', async () => {
    const { factory, storage: bucketStorage, groupId } = await createActiveStorage();
    await using _factory = factory;
    const db = factory.db;

    await insertParameterRows(db, [
      // Another replication stream: outside this stream's compaction scope.
      entry(90n, groupId + 1, 'row', [{ id: 'other-stream' }]),
      entry(100n, groupId, 'row', [{ id: 'old' }]),
      entry(110n, groupId, 'row', [{ id: 'new' }]),
      entry(120n, groupId, 'deleted', [{ id: 'delete-me' }]),
      entry(130n, groupId, 'deleted', []),
      // At the target checkpoint, so not eligible.
      entry(200n, groupId, 'row', [{ id: 'at-target' }])
    ]);

    await bucketStorage.compact({
      compactBuckets: [],
      compactParameterData: true,
      incrementalOnly: true,
      maxOpId: 200n
    });

    expect(await parameterIds(db)).toEqual([90n, 110n, 200n]);
    expect(await compactedBefore(db, groupId)).toBe(200n);

    await insertParameterRows(db, [entry(210n, groupId, 'row', [{ id: 'later' }]), entry(220n, groupId, 'row', [])]);
    // A fresh compactor has an empty identity cache, so the remaining entry at 110 is removed by a
    // leading-history delete rather than by id.
    await new PostgresParameterCompactor(db, groupId, 300n, {}).compact();

    expect(await parameterIds(db)).toEqual([90n]);
    expect(await compactedBefore(db, groupId)).toBe(300n);
  });

  test('persists the cursor while a pass is in progress', async () => {
    const { factory, groupId } = await createActiveStorage();
    await using _factory = factory;
    const db = factory.db;

    await insertParameterRows(db, [
      entry(100n, groupId, 'a', [{ id: 'a1' }]),
      entry(110n, groupId, 'a', [{ id: 'a2' }]),
      entry(120n, groupId, 'a', [{ id: 'a3' }])
    ]);

    const persisted: bigint[] = [];
    class RecordingCompactor extends PostgresParameterCompactor {
      protected override async persistCompactedBefore(value: bigint): Promise<void> {
        persisted.push(value);
        return super.persistCompactedBefore(value);
      }
    }

    // One entry per batch, persisting after every batch.
    await new RecordingCompactor(db, groupId, 200n, {}, 1, 0).compact();

    // Progress is persisted past every batch, and once more at the end of the pass.
    expect(persisted).toEqual([101n, 111n, 121n, 200n, 200n]);
    expect(await parameterIds(db)).toEqual([120n]);
  });

  test('seeds the compaction cursor when a stream is created', async () => {
    await using factory = await POSTGRES_STORAGE_FACTORY.factory();
    // Stand in for parameter history written by earlier deployments: all replication streams share
    // the op id sequence and the `bucket_parameters` table.
    await factory.db.sql`
      SELECT
        setval('op_id_sequence', 500)
    `.execute();

    const syncRules = await factory.updateSyncRules(updateSyncRulesFromYaml(PARAMETER_RULES));

    // Entries for this stream can only be written above the sequence head, so its first compaction
    // does not have to scan the older entries of other streams.
    expect(await compactedBefore(factory.db, syncRules.replicationStreamId)).toBe(500n);
  });
});
