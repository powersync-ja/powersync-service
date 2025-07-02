import { describe, expect, test } from 'vitest';
import { isVersionAtLeast, matchedSchemaChangeQuery } from '@module/utils/mysql-utils.js';

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

  test('matchedSchemaChangeQuery function', () => {
    const matcher = (tableName: string) => tableName === 'users';

    // DDL matches and table name matches
    expect(matchedSchemaChangeQuery('CREATE TABLE clientSchema.users (id INT)', matcher)).toBeTruthy();
    expect(matchedSchemaChangeQuery('ALTER TABLE users ADD COLUMN name VARCHAR(255)', matcher)).toBeTruthy();
    expect(matchedSchemaChangeQuery('DROP TABLE users', matcher)).toBeTruthy();
    expect(matchedSchemaChangeQuery('TRUNCATE TABLE users', matcher)).toBeTruthy();
    expect(matchedSchemaChangeQuery('RENAME TABLE new_users TO users', matcher)).toBeTruthy();

    // Can handle backticks in table names
    expect(matchedSchemaChangeQuery('CREATE TABLE `clientSchema`.`users` (id INT)', matcher)).toBeTruthy();

    // DDL matches, but table name does not match
    expect(matchedSchemaChangeQuery('CREATE TABLE clientSchema.clients (id INT)', matcher)).toBeFalsy();
    // No DDL match
    expect(matchedSchemaChangeQuery('SELECT * FROM users', matcher)).toBeFalsy();
  });
});
