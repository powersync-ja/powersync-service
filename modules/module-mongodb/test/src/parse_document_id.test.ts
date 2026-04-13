import { parseDocumentId } from '@module/replication/bufferToSqlite.js';
import { mongo } from '@powersync/lib-service-mongodb';
import { bson } from '@powersync/service-core';
import { describe, expect, test } from 'vitest';

function expectSameId(actual: any, expected: any) {
  expect(actual).toEqual(expected);
  expect(bson.serialize({ id: actual })).toEqual(bson.serialize({ id: expected }));
}

describe('parseDocumentId', () => {
  test('matches naive bson.deserialize for representative _id values', () => {
    const ids = [
      1,
      2n,
      1 / Math.PI,
      'abc',
      { a: 1, b: { c: 2 } },
      new mongo.ObjectId('0123456789abcdef01234567'),
      mongo.Timestamp.fromBits(123, 456),
      new mongo.Binary(Buffer.from([1, 2, 3]), 0),
      mongo.Decimal128.fromString('1234.5678'),
      new mongo.MinKey(),
      new mongo.MaxKey()
    ];

    for (const id of ids) {
      const source = bson.serialize({ before: 'x', _id: id, after: 1 }) as Buffer;
      const parsed = parseDocumentId(source);
      const naive = bson.deserialize(source, { useBigInt64: true })._id;

      expectSameId(parsed.id, naive);
      expect(parsed.idBuffer).toEqual(bson.serialize({ _id: naive }));
    }
  });

  test('finds _id regardless of field position', () => {
    const id = { a: 1, b: 'two' };
    const docs = [
      { _id: id, other: true },
      { other: true, _id: id },
      { first: 1, second: 'x', _id: id, third: false }
    ];

    for (const doc of docs) {
      const source = bson.serialize(doc) as Buffer;
      const parsed = parseDocumentId(source);
      const naive = bson.deserialize(source, { useBigInt64: true })._id;

      expectSameId(parsed.id, naive);
      expect(parsed.idBuffer).toEqual(bson.serialize({ _id: naive }));
    }
  });
});
