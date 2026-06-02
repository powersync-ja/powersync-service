import { jsonSchemaToSQLiteType, readConvexFieldJsonType, toSqliteInputRow } from '@module/common/convex-to-sqlite.js';
import {
  applyRowContext,
  CompatibilityContext,
  CompatibilityEdition,
  ExpressionType
} from '@powersync/service-sync-rules';
import { describe, expect, it } from 'vitest';

const context = new CompatibilityContext({ edition: CompatibilityEdition.SYNC_STREAMS });

describe('convex-to-sqlite', () => {
  it('detects bytes and id field types from schema metadata', () => {
    expect(jsonSchemaToSQLiteType(readConvexFieldJsonType({ type: 'string', format: 'id' }))).toBe(ExpressionType.TEXT);
    expect(jsonSchemaToSQLiteType(readConvexFieldJsonType({ type: 'object' }))).toBe(ExpressionType.TEXT);
    expect(jsonSchemaToSQLiteType(readConvexFieldJsonType({ $description: 'base64 bytes', type: 'string' }))).toBe(
      ExpressionType.TEXT
    );
    expect(
      jsonSchemaToSQLiteType(
        readConvexFieldJsonType({ $description: 'int64 represented as base10 string', type: 'string' })
      )
    ).toBe(ExpressionType.TEXT);
  });

  it('keeps bytes as base64 strings without using schema metadata for row conversion', () => {
    const row = applyRowContext(
      toSqliteInputRow({
        _id: 'doc1',
        payload: 'AQID',
        plain_text: 'AQID'
      }),
      context
    );

    expect(row._id).toBe('doc1');
    expect(row.payload).toBe('AQID');
    expect(row.plain_text).toBe('AQID');
  });

  it('strips Convex metadata fields that should not be reported to storage', () => {
    const row = applyRowContext(
      toSqliteInputRow({
        _id: 'doc1',
        _creationTime: 1772817606884,
        _table: 'users',
        _ts: 1772817606884944123n,
        name: 'Alice'
      }),
      context
    );

    expect(row._id).toBe('doc1');
    expect(row._table).toBeUndefined();
    expect(row._ts).toBeUndefined();
  });

  it('keeps raw JSON wire values instead of applying declared numeric field types', () => {
    const row = applyRowContext(
      toSqliteInputRow({
        int_value: '9007199254740991',
        float_value: 1
      }),
      context
    );

    expect(row.int_value).toBe('9007199254740991');
    expect(row.float_value).toBe(1);
    expect(typeof row.float_value).toBe('number');
  });
});
