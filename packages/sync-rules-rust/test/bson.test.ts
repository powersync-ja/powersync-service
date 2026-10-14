import { JSONBig } from '@powersync/service-jsonbig';
import {
  DEFAULT_HYDRATION_STATE,
  SqlSyncRules,
  type SqliteRow,
  nodeSqlite,
  withBucketSource
} from '@powersync/service-sync-rules';
import {
  BSON,
  BSONRegExp,
  BSONSymbol,
  Binary,
  Code,
  Decimal128,
  Double,
  Int32,
  Long,
  MaxKey,
  MinKey,
  ObjectId,
  Timestamp
} from 'bson';
import { createRequire } from 'node:module';
import * as sqlite from 'node:sqlite';
import { setImmediate } from 'node:timers/promises';
import { describe, expect, test } from 'vitest';
import { replicaIdToSubkey } from '../../../modules/module-mongodb-storage/src/utils/util.js';
import {
  bufferToSqlite,
  getDateRenderMode,
  parseDocumentId
} from '../../../modules/module-mongodb/src/replication/bufferToSqlite.js';
import { hashData, hashDelete } from '../../service-core/src/util/utils.js';
import { RustSourceEvaluator } from '../src/index.js';
import { table } from './helpers.js';

function prepare(queries = ['SELECT *, _id AS id FROM docs'], options = '', sourceTable = table) {
  const { config } = SqlSyncRules.fromYaml(
    `config:\n  edition: 3\n  unstable_sqlite_expression_engine: true\n${options}streams:\n` +
      queries
        .map(
          (query, i) =>
            `  s${i}:\n    accept_potentially_dangerous_queries: true\n    query: ${JSON.stringify(query)}\n`
        )
        .join(''),
    { defaultSchema: 'public', throwOnError: true }
  );
  const rust = new RustSourceEvaluator(config, sourceTable);
  const reference = config.hydrate({ hydrationState: DEFAULT_HYDRATION_STATE, sqlite: nodeSqlite(sqlite) });
  const convert = (buffer: Uint8Array): SqliteRow =>
    bufferToSqlite(
      Buffer.from(buffer.buffer, buffer.byteOffset, buffer.byteLength),
      getDateRenderMode(config.compatibility)
    );
  const expected = (buffers: Uint8Array[]) =>
    buffers.map((buffer) => {
      const record = convert(buffer);
      const { results, errors } = reference.evaluateRowWithErrors({ sourceTable, record });
      return {
        data: {
          results: results.map((row) => withBucketSource({ ...row, data: JSONBig.stringify(row.data) }, row.source)),
          errors
        },
        parameters: reference.evaluateParameterRowWithErrors(sourceTable, record)
      };
    });
  return { rust, expected };
}

const cases = [
  ['null', null],
  ['string', 'line\n"\\\u0000😀'],
  ['objectId', new ObjectId('66e834cc91d805df11fa0ecb')],
  ['int32', new Int32(-123)],
  ['int64', Long.MAX_VALUE],
  ['int64-min', Long.MIN_VALUE],
  ['double', new Double(1.25)],
  ['integral-double', new Double(2)],
  ['negative-zero', new Double(-0)],
  ['small-double', new Double(1e-8)],
  ['true', true],
  ['false', false],
  ['date', new Date('2026-09-14T12:34:56.123Z')],
  ['before-epoch', new Date(-1)],
  ['extended-date', new Date('+010000-01-01T00:00:00.000Z')],
  ['decimal', Decimal128.fromString('123456789.00100')],
  ['regex', new BSONRegExp('a\\w+"', 'ims')],
  ['symbol', new BSONSymbol('symbol')],
  ['code', new Code('return 1;')],
  ['scope', new Code('return x;', { x: 1, date: new Date(123) })],
  ['binary', new Binary(Buffer.from([1, 2, 3]))],
  ['old-binary', new Binary(Buffer.from([1, 2, 3]), 2)],
  ['uuid', new Binary(Buffer.from('00112233445566778899aabbccddeeff', 'hex'), 4)],
  ['old-uuid', new Binary(Buffer.from('00112233445566778899aabbccddeeff', 'hex'), 3)],
  ['timestamp', new Timestamp({ t: 1700000000, i: 123 })],
  ['min-key', new MinKey()],
  ['max-key', new MaxKey()],
  ['document', { a: 1, b: true, c: { label: 'nested' } }],
  ['array', [1, null, 'two', false]]
] as const;

describe('raw BSON through evaluation and JSON', () => {
  const tableId = new ObjectId('66e834cc91d805df11fa0ecb');
  function preparedReference(expected: ReturnType<typeof prepare>['expected'], buffers: Uint8Array[]) {
    return expected(buffers).map((row, i) => {
      const bytes = buffers[i];
      const { id, idBuffer } = parseDocumentId(Buffer.from(bytes.buffer, bytes.byteOffset, bytes.byteLength));
      const subkey = replicaIdToSubkey(tableId, id);
      return {
        ...row,
        replicaIdBson: new Uint8Array(idBuffer),
        subkey,
        deleteChecksum: hashDelete(subkey),
        data: {
          ...row.data,
          results: row.data.results.map((value) =>
            withBucketSource({ ...value, checksum: hashData(value.table, value.id, value.data) }, value.source)
          )
        }
      };
    });
  }

  test.each(cases.filter(([name]) => !['regex', 'code', 'scope'].includes(name)))(
    'native preparation preserves %s replica identity and checksums',
    async (_name, id) => {
      // Use a separate logical row ID so every replica-ID type can be exercised.
      const { rust, expected } = prepare(['SELECT id, title, value FROM docs']);
      const buffers = [BSON.serialize({ title: 'Straße😀', _id: id, id: 'row.😀', value: Long.MAX_VALUE })];
      expect(await rust.prepareBsonAsync(buffers, tableId.toHexString())).toEqual(preparedReference(expected, buffers));
    }
  );

  test('native preparation covers nested identity normalization, fanout, filters and parameter indexes', async () => {
    const { rust, expected } = prepare([
      "SELECT docs.*, docs.id AS id FROM docs, json_each(docs.tags) t WHERE t.value = subscription.parameter('tag')",
      'SELECT id FROM docs WHERE active = 1',
      'SELECT others.* FROM others WHERE owner IN (SELECT id FROM docs WHERE owner = auth.user_id())'
    ]);
    const buffers = Array.from({ length: 100 }, (_, i) =>
      BSON.serialize({
        _id: { nested: [new Double(i), Long.MAX_VALUE, new Binary(Buffer.from([0, 1, 255]))], text: String(i) },
        id: String(i),
        owner: 'user',
        tags: ['a', 'b', 'a'],
        active: i % 2 === 0
      })
    );
    expect(await rust.prepareBsonAsync(buffers, tableId.toHexString())).toEqual(preparedReference(expected, buffers));
  });

  test('native preparation owns input and handles concurrent, empty and rejected batches', async () => {
    const { rust, expected } = prepare();
    const good = BSON.serialize({ _id: 'x', value: 'original' });
    const padded = Buffer.concat([Buffer.alloc(5), good, Buffer.alloc(3)]);
    const input = padded.subarray(5, 5 + good.length);
    const pending = rust.prepareBsonAsync([input], tableId.toHexString());
    input.fill(0);
    const reference = preparedReference(expected, [good]);
    expect(await pending).toEqual(reference);
    expect(
      await Promise.all([
        rust.prepareBsonAsync([good], tableId.toHexString()),
        rust.prepareBsonAsync([good], tableId.toHexString())
      ])
    ).toEqual([reference, reference]);
    expect(await rust.prepareBsonAsync([], tableId.toHexString())).toEqual([]);
    await expect(rust.prepareBsonAsync([good], 'invalid')).rejects.toThrow('table ID');
    for (const bad of [
      Buffer.alloc(0),
      // Deprecated BSON Undefined _id; JS serialization would omit it from the subkey hash.
      Buffer.from([10, 0, 0, 0, 6, 95, 105, 100, 0, 0]),
      BSON.serialize({ value: 1 }),
      BSON.serialize({ _id: new BSONRegExp('x', 'i') }),
      BSON.serialize({ _id: JSON.parse('{"__proto__":1}') })
    ]) {
      await expect(rust.prepareBsonAsync([good, bad], tableId.toHexString())).rejects.toThrow();
      expect(await rust.prepareBsonAsync([good], tableId.toHexString())).toEqual(reference);
    }
  });
  test.each(cases)('%s at top level, in objects and arrays', async (_name, value) => {
    const { rust, expected } = prepare();
    const buffers = [{ value }, { value: { nested: value } }, { value: [value] }].map((row) =>
      BSON.serialize({ _id: 'x', ...row })
    );
    expect(rust.evaluateBson(buffers)).toEqual(expected(buffers));
    expect(await rust.evaluateBsonAsync(buffers)).toEqual(expected(buffers));
  });

  test.each(['', '  timestamp_max_precision: seconds\n', '  timestamps_iso8601: false\n'])(
    'date policy %s',
    async (options) => {
      const { rust, expected } = prepare(undefined, options);
      const buffers = [BSON.serialize({ _id: 'x', date: new Date(-1), nested: { date: new Date(123) } })];
      expect(await rust.evaluateBsonAsync(buffers)).toEqual(expected(buffers));
    }
  );

  test('JSON expressions, filters, fanout and multiple queries', async () => {
    const { rust, expected } = prepare([
      "SELECT _id AS id, json_extract(details, '$.value') AS value, upper(title) AS title FROM docs WHERE active = 1",
      "SELECT docs.*, docs._id AS id FROM docs, json_each(docs.tags) t WHERE t.value = subscription.parameter('tag')"
    ]);
    const buffers = Array.from({ length: 200 }, (_, i) =>
      BSON.serialize({
        _id: String(i),
        active: i % 2 === 0,
        title: 'Straße😀',
        details: { value: Long.fromNumber(i) },
        tags: ['one', 'two', 'one']
      })
    );
    expect(await rust.evaluateBsonAsync(buffers)).toEqual(expected(buffers));
  });

  test('parameter evaluation stays native alongside BSON conversion', async () => {
    const { rust, expected } = prepare(
      [
        'SELECT docs.* FROM docs WHERE owner IN (SELECT users.id FROM users, json_each(users.teams) t WHERE t.value = auth.user_id())'
      ],
      '',
      { ...table, name: 'users' }
    );
    const buffers = [BSON.serialize({ _id: 'x', id: 'u', teams: ['a', 'a', 'b', null] })];
    expect(await rust.evaluateBsonAsync(buffers)).toEqual(expected(buffers));
  });

  test('owns input bytes, handles subarrays, concurrent batches and empty batches', async () => {
    const { rust, expected } = prepare();
    const raw = BSON.serialize({ _id: 'x', value: 'original' });
    const padded = Buffer.concat([Buffer.alloc(7), raw, Buffer.alloc(11)]);
    const input = padded.subarray(7, 7 + raw.length);
    const reference = expected([raw]);
    const pending = rust.evaluateBsonAsync([input]);
    input.fill(0);
    expect(await pending).toEqual(reference);
    expect(await Promise.all([rust.evaluateBsonAsync([raw]), rust.evaluateBsonAsync([raw])])).toEqual([
      reference,
      reference
    ]);
    expect(rust.evaluateBson([])).toEqual([]);
    expect(await rust.evaluateBsonAsync([])).toEqual([]);
  });

  test('background work allows event-loop progress', async () => {
    const { rust } = prepare();
    const raw = BSON.serialize({ _id: 'x', nested: Array.from({ length: 200 }, (_, i) => ({ i, label: 'text' })) });
    const pending = rust.evaluateBsonAsync(Array(2000).fill(raw));
    let finished = false;
    void pending.then(() => {
      finished = true;
    });
    await setImmediate();
    expect(finished).toBe(false);
    expect(await pending).toHaveLength(2000);
  });

  test('malformed BSON and out-of-range integers reject the batch without poisoning the evaluator', async () => {
    const { rust, expected } = prepare();
    const good = BSON.serialize({ _id: 'x' });
    for (const bad of [
      Buffer.alloc(0),
      Buffer.from([5, 0, 0, 0, 1]),
      good.subarray(0, good.length - 1),
      BSON.serialize({ _id: 'x', value: new Double(2 ** 63) }),
      BSON.serialize({ _id: 'x', value: new Timestamp({ t: 0xffffffff, i: 0xffffffff }) })
    ]) {
      expect(() => rust.evaluateBson([good, bad])).toThrow();
      await expect(rust.evaluateBsonAsync([good, bad])).rejects.toThrow();
      expect(await rust.evaluateBsonAsync([good])).toEqual(expected([good]));
    }
  });

  test('nested nonfinite doubles and unsigned timestamps preserve JSON conversion', async () => {
    const { rust, expected } = prepare();
    const buffers = [
      BSON.serialize({
        _id: 'x',
        nested: [new Double(Infinity), new Double(NaN), new Timestamp({ t: 0xffffffff, i: 0xffffffff })]
      })
    ];
    expect(await rust.evaluateBsonAsync(buffers)).toEqual(expected(buffers));
  });

  test('enforces nested depth bound', async () => {
    const { rust, expected } = prepare();
    let value: unknown = 1;
    for (let i = 0; i < 21; i++) value = { nested: value };
    const allowed = BSON.serialize({ _id: 'x', value });
    expect(await rust.evaluateBsonAsync([allowed])).toEqual(expected([allowed]));
    const tooDeep = BSON.serialize({ _id: 'x', value: { nested: value } });
    await expect(rust.evaluateBsonAsync([tooDeep])).rejects.toThrow('depth');
  });

  test('native API rejects invalid date policy', async () => {
    const { NativeEvaluator } = createRequire(import.meta.url)('../dist/evaluator.node');
    const native = new NativeEvaluator('[]');
    expect(() => native.evaluateBson([], 9)).toThrow('date render mode');
    expect(() => native.evaluateBsonAsync([], 9)).toThrow('date render mode');
    expect(() => native.prepareBsonAsync([], 9, tableId.toHexString())).toThrow('date render mode');
  });

  test('documents known BSON conversion limits explicitly', async () => {
    const { rust } = prepare(['SELECT * FROM docs']);
    function field(type: number, payload: Buffer) {
      const bytes = Buffer.concat([Buffer.alloc(4), Buffer.from([type, 120, 0]), payload, Buffer.from([0])]);
      bytes.writeInt32LE(bytes.length);
      return bytes;
    }
    const millis = Buffer.alloc(8);
    millis.writeBigInt64LE(9223372036854775807n);
    await expect(rust.evaluateBsonAsync([field(0x09, millis)])).rejects.toThrow('datetime');
    const namespace = Buffer.from([2, 0, 0, 0, 97, 0]);
    await expect(rust.evaluateBsonAsync([field(0x0c, Buffer.concat([namespace, Buffer.alloc(12)]))])).rejects.toThrow(
      'DBPointer'
    );
    await expect(rust.evaluateBsonAsync([field(0x02, Buffer.from([2, 0, 0, 0, 255, 0]))])).rejects.toThrow('bson');
    expect(
      JSONBig.parse((await rust.evaluateBsonAsync([field(0x06, Buffer.alloc(0))]))[0].data.results[0].data)
    ).toEqual({ x: null });
  });
});
