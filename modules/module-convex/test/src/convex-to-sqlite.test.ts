import {
  applyRowContext,
  CompatibilityContext,
  CompatibilityEdition,
  ExpressionType,
  isJsonValue
} from '@powersync/service-sync-rules';
import { describe, expect, it } from 'vitest';
import {
  readConvexFieldType,
  toExpressionTypeFromConvexType,
  toSqliteInputRow
} from '@module/common/convex-to-sqlite.js';

const context = new CompatibilityContext({ edition: CompatibilityEdition.SYNC_STREAMS });

describe('convex-to-sqlite', () => {
  it('maps Convex types to SQLite expression types', () => {
    expect(toExpressionTypeFromConvexType('id')).toBe(ExpressionType.TEXT);
    expect(toExpressionTypeFromConvexType('bytes')).toBe(ExpressionType.BLOB);
    expect(toExpressionTypeFromConvexType('int64')).toBe(ExpressionType.INTEGER);
    expect(toExpressionTypeFromConvexType('float64')).toBe(ExpressionType.REAL);
    expect(toExpressionTypeFromConvexType('record')).toBe(ExpressionType.TEXT);
    expect(toExpressionTypeFromConvexType('null')).toBe(ExpressionType.NONE);
  });

  it('detects bytes and id field types from schema metadata', () => {
    expect(readConvexFieldType({ type: 'string', format: 'bytes' })).toBe('bytes');
    expect(readConvexFieldType({ type: 'string', format: 'id' })).toBe('id');
    expect(readConvexFieldType({ valueType: 'record' })).toBe('record');
    expect(readConvexFieldType({ contentEncoding: 'base64', type: 'string' })).toBe('bytes');
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
          payload: { type: 'bytes' },
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
});
