import { describe, expect, test } from 'vitest';
import { LSN } from '@module/common/LSN.js';

describe('LSN', () => {
  test('normalizes lowercase hex strings to uppercase', () => {
    const lsn = LSN.fromString('0000002f:00000de0:0001');
    expect(lsn.toString()).toEqual('0000002F:00000DE0:0001');
  });

  test('accepts uppercase hex strings', () => {
    const lsn = LSN.fromString('0000002F:00000DE0:0001');
    expect(lsn.toString()).toEqual('0000002F:00000DE0:0001');
  });
});
