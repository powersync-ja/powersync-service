import { describe, expect, test } from 'vitest';
import { CustomSqliteValue, SqlSyncRules, DateTimeValue, toSyncRulesValue } from '../../src/index.js';

import { ASSETS, PARSE_OPTIONS } from './util.js';

describe('handling historical quirks', () => {
  describe('timestamps', () => {
    const value = new DateTimeValue('2025-08-19T09:21:00Z');

    test('uses old format by default', () => {
      const rules = SqlSyncRules.fromYaml(
        `
bucket_definitions:
  mybucket:
    data:
      - SELECT id, description FROM assets
    `,
        PARSE_OPTIONS
      );

      expect(
        rules.evaluateRow({
          sourceTable: ASSETS,
          record: {
            id: 'id',
            description: value
          }
        })
      ).toStrictEqual([
        { bucket: 'mybucket[]', data: { description: '2025-08-19 09:21:00Z', id: 'id' }, id: 'id', table: 'assets' }
      ]);
    });

    test('can opt-in to new format', () => {
      const rules = SqlSyncRules.fromYaml(
        `
bucket_definitions:
  mybucket:
    data:
      - SELECT id, description FROM assets

fixed_quirks:
  - non_iso8601_timestamps
    `,
        PARSE_OPTIONS
      );

      expect(
        rules.evaluateRow({
          sourceTable: ASSETS,
          record: {
            id: 'id',
            description: value
          }
        })
      ).toStrictEqual([
        { bucket: 'mybucket[]', data: { description: '2025-08-19T09:21:00Z', id: 'id' }, id: 'id', table: 'assets' }
      ]);
    });

    test('streams use new format by default', () => {
      const rules = SqlSyncRules.fromYaml(
        `
streams:
  stream:
    query: SELECT id, description FROM assets
    `,
        PARSE_OPTIONS
      );

      expect(
        rules.evaluateRow({
          sourceTable: ASSETS,
          record: {
            id: 'id',
            description: value
          }
        })
      ).toStrictEqual([
        { bucket: 'stream|0[]', data: { description: '2025-08-19T09:21:00Z', id: 'id' }, id: 'id', table: 'assets' }
      ]);
    });
  });

  test('warning for unknown quirk', () => {
    expect(() => {
      SqlSyncRules.fromYaml(
        `
bucket_definitions:
  mybucket:
    data:
      - SELECT id, description FROM assets

fixed_quirks:
  - does_not_exist
    `,
        PARSE_OPTIONS
      );
    }).toThrow(/must be equal to one of the allowed values/);
  });

  test('arrays', () => {
    const data = toSyncRulesValue(['static value', new DateTimeValue('2025-08-19T09:21:00Z')]);

    for (const withFixedQuirk of [false, true]) {
      let syncRules = `
bucket_definitions:
  mybucket:
    data:
      - SELECT id, description FROM assets
      `;

      if (withFixedQuirk) {
        syncRules += `
fixed_quirks:
  - non_iso8601_timestamps
        `;
      }

      const rules = SqlSyncRules.fromYaml(syncRules, PARSE_OPTIONS);
      expect(
        rules.evaluateRow({
          sourceTable: ASSETS,
          record: {
            id: 'id',
            description: data
          }
        })
      ).toStrictEqual([
        {
          bucket: 'mybucket[]',
          data: {
            description: withFixedQuirk
              ? '["static value","2025-08-19T09:21:00Z"]'
              : '["static value","2025-08-19 09:21:00Z"]',
            id: 'id'
          },
          id: 'id',
          table: 'assets'
        }
      ]);
    }
  });
});
