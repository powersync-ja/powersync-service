import { describe, expect, test } from 'vitest';
import { SqlSyncRules, DateTimeValue, toSyncRulesValue } from '../../src/index.js';

import { ASSETS, PARSE_OPTIONS } from './util.js';

describe('compatibility options', () => {
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

config:
  timestamps_iso8601: true
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

config:
  edition: 2
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

    test('streams can disable new format', () => {
      const rules = SqlSyncRules.fromYaml(
        `
streams:
  stream:
    query: SELECT id, description FROM assets

config:
  edition: 2
  timestamps_iso8601: false
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
        { bucket: 'stream|0[]', data: { description: '2025-08-19 09:21:00Z', id: 'id' }, id: 'id', table: 'assets' }
      ]);
    });
  });

  test('warning for unknown option', () => {
    expect(() => {
      SqlSyncRules.fromYaml(
        `
bucket_definitions:
  mybucket:
    data:
      - SELECT id, description FROM assets

config:
  unknown_option: true
    `,
        PARSE_OPTIONS
      );
    }).toThrow(/must NOT have additional properties/);
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
config:
  edition: 2
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
