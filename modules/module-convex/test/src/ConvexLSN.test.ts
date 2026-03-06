import { describe, expect, it } from 'vitest';
import { ConvexLSN } from '@module/common/ConvexLSN.js';

describe('ConvexLSN', () => {
  it('serializes and deserializes the numeric cursor', () => {
    const source = ConvexLSN.fromCursor('12345');
    const roundTrip = ConvexLSN.fromSerialized(source.comparable);

    expect(source.comparable).toBe('00000000000000012345');
    expect(roundTrip.toCursorString()).toBe('12345');
  });

  it('sorts lexicographically by timestamp', () => {
    const older = ConvexLSN.fromCursor('9').comparable;
    const newer = ConvexLSN.fromCursor('10').comparable;

    expect(older < newer).toBe(true);
  });

  it('handles bare numeric cursor string', () => {
    const parsed = ConvexLSN.fromSerialized('42');
    expect(parsed.toCursorString()).toBe('42');
  });

  it('normalizes zero-padded serialized values', () => {
    const parsed = ConvexLSN.fromSerialized('00000000000000012345');
    expect(parsed.toCursorString()).toBe('12345');
  });

  it('rejects delimiter-based serialized payloads', () => {
    expect(() => ConvexLSN.fromSerialized('n00000000000000012345|12345')).toThrow(
      'Convex cursor is not a valid numeric timestamp'
    );
  });

  it('rejects non-numeric cursors', () => {
    expect(() => ConvexLSN.fromCursor('{"tablet":"abc","id":"xyz"}')).toThrow(
      'Convex cursor is not a valid numeric timestamp'
    );
  });
});
