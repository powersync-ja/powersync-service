import { DirectSourceRowConverter } from '@module/replication/SourceRowConverter.js';
import { MONGO_PREPARATION_WORKER } from '@module/replication/writeMongoChange.js';
import { storage, utils } from '@powersync/service-core';
import { JSONBig } from '@powersync/service-jsonbig';
import { HydratedSyncConfig, nodeSqlite, SqlSyncRules } from '@powersync/service-sync-rules';
import { Binary, BSON, ObjectId, UUID } from 'bson';
import * as sqlite from 'node:sqlite';
import { describe, expect, test } from 'vitest';

function fixture(compiled: boolean) {
  const content = compiled
    ? `
config:
  edition: 3
streams:
  items:
    queries:
      - SELECT *, _id AS id FROM items WHERE owner = auth.user_id()
`
    : `
bucket_definitions:
  items:
    parameters: SELECT owner FROM items WHERE owner = request.user_id()
    data:
      - SELECT *, _id AS id FROM items WHERE owner = bucket.owner
`;
  // Two configs deliberately share names but have distinct persisted scopes.
  const definitions = [0, 1].map(() => SqlSyncRules.fromYaml(content, { defaultSchema: 'test' }).config);
  const buckets = definitions.flatMap((d) => d.bucketDataSources);
  const parameters = definitions.flatMap((d) => d.bucketParameterLookupSources);
  const config = new HydratedSyncConfig({
    definitions,
    createParams: {
      sqlite: nodeSqlite(sqlite),
      hydrationState: {
        getBucketSourceScope: (source) => ({ source, bucketPrefix: `persisted.${buckets.indexOf(source)}` }),
        getParameterIndexLookupScope: (source) => ({
          source,
          lookupName: `index.${parameters.indexOf(source)}`,
          queryId: ''
        })
      }
    }
  });
  const tables = definitions.map((definition, i) => {
    const table = new storage.SourceTable({
      id: new ObjectId(),
      ref: { schema: 'test', name: 'items', connectionTag: 'default' },
      objectId: undefined,
      replicaIdColumns: [],
      snapshotComplete: true,
      bucketDataSources: definition.bucketDataSources,
      parameterLookupSources: definition.bucketParameterLookupSources
    });
    table.syncEvent = false;
    return table;
  });
  return { config, tables };
}

describe('single Mongo row preparation worker', () => {
  test.each([false, true])('matches inline preparation, compiled=%s', async (compiled) => {
    const { config, tables } = fixture(compiled);
    await using worker = new storage.RowPreparationWorker(MONGO_PREPARATION_WORKER, config);
    const converter = new DirectSourceRowConverter(config.compatibility);
    const raw = BSON.serialize({
      _id: new ObjectId(),
      owner: 'user',
      num: 1152921504606846976n,
      nested: { quotes: '\"\\\n café 😀', array: [1, null, { yes: true }] },
      date: new Date('2025-01-01T00:00:00.123Z'),
      binary: Buffer.from([0, 255])
    });
    const inputs = tables.map((table) => ({ table, raw }));
    // Repeated requests also exercise retained evaluator selections and worker reuse.
    for (let i = 0; i < 2; i++) {
      const result = await worker.prepare(inputs);
      for (const [index, table] of tables.entries()) {
        const row = converter.rawToSqliteRow(raw as Buffer).row;
        const subkey = utils.mongoReplicaIdToSubkey(table.id, BSON.deserialize(raw)._id);
        expect(result[index].subkey).toBe(subkey);
        expect(result[index].deleteChecksum).toBe(utils.hashDelete(subkey));
        const evaluated = config.evaluateRowWithErrors({
          record: row,
          sourceTable: table.ref,
          bucketDataSources: table.bucketDataSources
        });
        expect(evaluated.results.length).toBeGreaterThan(0);
        expect(result[index].data).toEqual({
          errors: evaluated.errors,
          results: evaluated.results.map(({ data, source, ...value }) => {
            const json = JSONBig.stringify(data);
            return { ...value, source, json, checksum: utils.hashData(value.table, value.id, json) };
          })
        });
        const parameters = config.evaluateParameterRowWithErrors(table.ref, row, {
          parameterLookupSources: table.parameterLookupSources
        });
        expect(result[index].parameters).toEqual(parameters);
        result[index].parameters.results.forEach((value, i) => {
          expect(value.lookup.source).toBe(parameters.results[i].lookup.source);
          expect(value.lookup.serializedRepresentation).toBe(parameters.results[i].lookup.serializedRepresentation);
        });
      }
    }
  });

  test('worker failure rejects pending and subsequent preparation', async () => {
    const { config, tables } = fixture(false);
    await using worker = new storage.RowPreparationWorker(MONGO_PREPARATION_WORKER, config);
    const input = [{ table: tables[0], raw: Buffer.from([1, 2, 3]) }];
    await expect(worker.prepare(input)).rejects.toThrow();
    await expect(worker.prepare(input)).rejects.toThrow();
  });

  test('preserves BSON replica identity types and subkeys', async () => {
    const { config, tables } = fixture(false);
    await using worker = new storage.RowPreparationWorker(MONGO_PREPARATION_WORKER, config);
    const ids = [
      new ObjectId(),
      new UUID(),
      new Binary(Buffer.from([1, 2])),
      '123',
      123,
      1152921504606846976n,
      { part: 'a', nested: { value: 123n } }
    ];
    const results = await worker.prepare(
      ids.map((_id) => ({
        table: tables[0],
        raw: BSON.serialize({ _id, owner: 'user' })
      }))
    );
    results.forEach((result, i) => {
      expect(BSON.deserialize(result.replicaIdBson, { useBigInt64: true })._id).toEqual(ids[i]);
      const subkey = utils.mongoReplicaIdToSubkey(tables[0].id, ids[i]);
      expect(result.subkey).toBe(subkey);
      expect(result.deleteChecksum).toBe(utils.hashDelete(subkey));
    });
  });

  test('abort rejects pending preparation', async () => {
    const { config, tables } = fixture(false);
    const controller = new AbortController();
    await using worker = new storage.RowPreparationWorker(MONGO_PREPARATION_WORKER, config, controller.signal);
    const pending = worker.prepare([{ table: tables[0], raw: BSON.serialize({ _id: 'a' }) }]);
    controller.abort();
    await expect(pending).rejects.toThrow('aborted');
  });
});
