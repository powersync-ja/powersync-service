import { describe, expect, it } from 'vitest';
import { ConvexLSN } from '@module/common/ConvexLSN.js';

describe('ConvexLSN', () => {
  it('serializes and deserializes the numeric cursor', () => {
    const source = ConvexLSN.fromCursor('1772817606884944136');
    const roundTrip = ConvexLSN.fromSerialized(source.comparable);

    expect(source.comparable).toBe('1772817606884944136');
    expect(roundTrip.toCursorString()).toBe('1772817606884944136');
  });

  it('sorts lexicographically by timestamp', () => {
    const older = ConvexLSN.fromCursor('1772817606884944136').comparable;
    const newer = ConvexLSN.fromCursor('1772817606884944137').comparable;

    expect(older < newer).toBe(true);
  });

  it('handles bare numeric cursor string', () => {
    const parsed = ConvexLSN.fromSerialized('1772817606884944136');
    expect(parsed.toCursorString()).toBe('1772817606884944136');
  });

  it('rejects padded serialized payloads', () => {
    expect(() => ConvexLSN.fromSerialized('0001772817606884944136')).toThrow(
      'Convex cursor is not a canonical numeric timestamp'
    );
  });

  it('rejects delimiter-based serialized payloads', () => {
    expect(() => ConvexLSN.fromSerialized('1772817606884944136|1772817606884944136')).toThrow(
      'Convex cursor is not a valid numeric timestamp'
    );
  });

  it('rejects non-numeric cursors', () => {
    expect(() => ConvexLSN.fromCursor('{"tablet":"abc","id":"xyz"}')).toThrow(
      'Convex cursor is not a valid numeric timestamp'
    );
  });
});
