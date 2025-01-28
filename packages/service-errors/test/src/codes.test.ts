import { describe, it, expect } from 'vitest';
import { ErrorCode } from '../../src/codes.js';

// This file never exports anything - it is only used for checking our
// error code definitions.

// Step 1: Check that the codes match the syntax `PSYNC_xxxxx`.

type Digit = '0' | '1' | '2' | '3' | '4' | '5' | '6' | '7' | '8' | '9';
type Category = 'S' | 'R';

// Note: This generates a type union of 20k possiblities,
// which could potentially slow down the TypeScript compiler.
// If it does, we could switch to a simpler `PSYNC_${Category}${number}` type.
type ServiceErrorCode = `PSYNC_${Category}${Digit}${Digit}${Digit}${Digit}`;

describe('Service Error Codes', () => {
  it('should match PSYNC_xxxxx', () => {
    // tsc checks this for us
    null as unknown as ErrorCode satisfies ServiceErrorCode;
  });

  it('should have matching keys and values', () => {
    const codes = Object.keys(ErrorCode);
    expect(codes.length).toBeGreaterThan(40);
    for (let key of codes) {
      const value = (ErrorCode as any)[key];
      expect(value).toEqual(key);
    }
  });
});
