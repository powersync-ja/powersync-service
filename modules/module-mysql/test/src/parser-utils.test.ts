import { describe, expect, test } from 'vitest';
import { matchedSchemaChangeQuery } from '@module/utils/parser-utils.js';

describe('MySQL Parser Util Tests', () => {
  test('matchedSchemaChangeQuery function', () => {
    const matcher = (tableName: string) => tableName === 'users';

    // DDL matches and table name matches
    expect(matchedSchemaChangeQuery('ALTER TABLE users ADD COLUMN name VARCHAR(255)', matcher)).toBeTruthy();
    expect(matchedSchemaChangeQuery('DROP TABLE users', matcher)).toBeTruthy();
    expect(matchedSchemaChangeQuery('TRUNCATE TABLE users', matcher)).toBeTruthy();
    expect(matchedSchemaChangeQuery('RENAME TABLE new_users TO users', matcher)).toBeTruthy();

    // Can handle backticks in table names
    expect(
      matchedSchemaChangeQuery('ALTER TABLE `clientSchema`.`users` ADD COLUMN name VARCHAR(255)', matcher)
    ).toBeTruthy();

    // DDL matches, but table name does not match
    expect(matchedSchemaChangeQuery('DROP TABLE clientSchema.clients', matcher)).toBeFalsy();
    // No DDL match
    expect(matchedSchemaChangeQuery('SELECT * FROM users', matcher)).toBeFalsy();
  });
});
