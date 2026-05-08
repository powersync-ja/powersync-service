import { describe, expect, test } from 'vitest';

import { JsonBufferWriter } from '@module/replication/JsonBufferWriter.js';

describe('JsonBufferWriter', () => {
  test('writeQuotedUtf8Slice escapes large control-heavy payloads without corruption', () => {
    const writer = new JsonBufferWriter(32);
    const payload = Buffer.alloc(8192, 0x01);

    writer.writeByte(0x7b); // {
    writer.writeQuotedUtf8Slice(payload, 0, payload.length);
    writer.writeByte(0x7d); // }

    const expected = `{${JSON.stringify(payload.toString('latin1'))}}`;
    expect(writer.toString()).toBe(expected);
  });
});
