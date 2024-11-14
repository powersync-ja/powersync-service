import { describe, expect, test } from 'vitest';
import { isVersionAtLeast } from '@module/utils/mysql-utils.js';

describe('MySQL Utility Tests', () => {
  test('Minimum version checking ', () => {
    const newerVersion = '8.4.0';
    const olderVersion = '5.7';
    const sameVersion = '8.0';
    // Improperly formatted semantic versions should be handled gracefully if possible
    const improperSemver = '5.7.42-0ubuntu0.18.04.1-log';

    expect(isVersionAtLeast(newerVersion, '8.0')).toBeTruthy();
    expect(isVersionAtLeast(sameVersion, '8.0')).toBeTruthy();
    expect(isVersionAtLeast(olderVersion, '8.0')).toBeFalsy();
    expect(isVersionAtLeast(improperSemver, '5.7')).toBeTruthy();
  });
});
