import { describe, expect, it } from 'vitest';
import { ConvexLSN } from '@module/common/ConvexLSN.js';

describe('ConvexLSN', () => {
  it('serializes and deserializes cursor and timestamp', () => {
    const source = ConvexLSN.fromCursor('12345');
    const roundTrip = ConvexLSN.fromSerialized(source.comparable);

    expect(roundTrip.timestamp).toBe(12345n);
    expect(roundTrip.toCursorString()).toBe('12345');
  });

  it('sorts lexicographically by timestamp', () => {
    const older = ConvexLSN.fromCursor('9').comparable;
    const newer = ConvexLSN.fromCursor('10').comparable;

    expect(older < newer).toBe(true);
  });

  it('supports legacy plain timestamp cursor format', () => {
    const parsed = ConvexLSN.fromSerialized('42');
    expect(parsed.timestamp).toBe(42n);
    expect(parsed.toCursorString()).toBe('42');
  });
});
