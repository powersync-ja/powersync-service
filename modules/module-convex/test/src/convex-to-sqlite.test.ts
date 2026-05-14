import { jsonSchemaToSQLiteType, readConvexFieldJsonType, toSqliteInputRow } from '@module/common/convex-to-sqlite.js';
import {
  applyRowContext,
  CompatibilityContext,
  CompatibilityEdition,
  ExpressionType,
  isJsonValue
} from '@powersync/service-sync-rules';
import { describe, expect, it } from 'vitest';

const context = new CompatibilityContext({ edition: CompatibilityEdition.SYNC_STREAMS });

describe('convex-to-sqlite', () => {
  it('detects bytes and id field types from schema metadata', () => {
    expect(jsonSchemaToSQLiteType(readConvexFieldJsonType({ type: 'string', format: 'id' }))).toBe(ExpressionType.TEXT);
    expect(jsonSchemaToSQLiteType(readConvexFieldJsonType({ type: 'object' }))).toBe(ExpressionType.TEXT);
    expect(jsonSchemaToSQLiteType(readConvexFieldJsonType({ description: 'base64 bytes', type: 'string' }))).toBe(
      ExpressionType.TEXT
    );
  });

  it('decodes bytes to Uint8Array and keeps them out of JSON-compatible values', () => {
    const row = applyRowContext(
      toSqliteInputRow(
        {
          _id: 'doc1',
          payload: 'AQID',
          plain_text: 'AQID'
        },
        {
          _id: { type: 'id' },
          payload: { $description: 'base64 bytes', type: 'string' },
          plain_text: { type: 'string' }
        }
      ),
      context
    );

    const payload = row.payload;
    expect(row._id).toBe('doc1');
    expect(payload).toEqual(Uint8Array.of(1, 2, 3));
    expect(payload instanceof Uint8Array).toBe(true);
    if (!(payload instanceof Uint8Array)) {
      throw new Error('Expected payload to be Uint8Array');
    }
    expect(isJsonValue(payload)).toBe(false);
    expect(row.plain_text).toBe('AQID');
  });

  it('strips Convex metadata fields that should not be reported to storage', () => {
    const row = applyRowContext(
      toSqliteInputRow(
        {
          _id: 'doc1',
          _creationTime: 1772817606884,
          _table: 'users',
          _ts: 1772817606884944123n,
          name: 'Alice'
        },
        {
          _id: { type: 'id' },
          _creationTime: { type: 'float64' },
          name: { type: 'string' }
        }
      ),
      context
    );

    expect(row._id).toBe('doc1');
    expect(row._table).toBeUndefined();
    expect(row._ts).toBeUndefined();
  });

  it('uses declared numeric field types instead of integer-looking runtime values', () => {
    const row = applyRowContext(
      toSqliteInputRow(
        {
          int_value: 1,
          float_value: 1
        },
        {
          int_value: { type: 'int64' },
          float_value: { type: 'float64' }
        }
      ),
      context
    );

    expect(row.int_value).toBe(1);
    expect(row.float_value).toBe(1);
    expect(typeof row.float_value).toBe('number');
  });
});
