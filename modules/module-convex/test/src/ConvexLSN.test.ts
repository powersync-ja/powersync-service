import { describe, expect, it } from 'vitest';
import { parseConvexLsn, toConvexLsn } from '@module/common/ConvexLSN.js';

describe('Convex cursor LSN helpers', () => {
  it('validates and round-trips the numeric cursor', () => {
    const source = toConvexLsn('1772817606884944136');
    const roundTrip = parseConvexLsn(source);

    expect(source).toBe('1772817606884944136');
    expect(roundTrip).toBe('1772817606884944136');
  });

  it('sorts lexicographically by timestamp', () => {
    const older = toConvexLsn('1772817606884944136');
    const newer = toConvexLsn('1772817606884944137');

    expect(older < newer).toBe(true);
  });

  it('handles bare numeric cursor string', () => {
    const parsed = parseConvexLsn('1772817606884944136');
    expect(parsed).toBe('1772817606884944136');
  });

  it('rejects padded serialized payloads', () => {
    expect(() => parseConvexLsn('0001772817606884944136')).toThrow(
      'Convex cursor is not a canonical numeric timestamp'
    );
  });

  it('rejects delimiter-based serialized payloads', () => {
    expect(() => parseConvexLsn('1772817606884944136|1772817606884944136')).toThrow(
      'Convex cursor is not a valid numeric timestamp'
    );
  });

  it('rejects non-numeric cursors', () => {
    expect(() => toConvexLsn('{"tablet":"abc","id":"xyz"}')).toThrow(
      'Convex cursor is not a valid numeric timestamp'
    );
  });
});
