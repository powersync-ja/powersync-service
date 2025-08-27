import { describe, expect, test } from 'vitest';
import { SqlSyncRules, DateTimeValue, toSyncRulesValue } from '../../src/index.js';

import { ASSETS, normalizeQuerierOptions, PARSE_OPTIONS } from './util.js';

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
          bucketIdTransformer: SqlSyncRules.versionedBucketIdTransformer(''),
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
          bucketIdTransformer: SqlSyncRules.versionedBucketIdTransformer(''),
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
    auto_subscribe: true

config:
  edition: 2
    `,
        PARSE_OPTIONS
      );

      expect(
        rules.evaluateRow({
          bucketIdTransformer: SqlSyncRules.versionedBucketIdTransformer('1'),
          sourceTable: ASSETS,
          record: {
            id: 'id',
            description: value
          }
        })
      ).toStrictEqual([
        { bucket: '1#stream|0[]', data: { description: '2025-08-19T09:21:00Z', id: 'id' }, id: 'id', table: 'assets' }
      ]);

      expect(
        rules.getBucketParameterQuerier(
          normalizeQuerierOptions({}, {}, {}, SqlSyncRules.versionedBucketIdTransformer('1'))
        ).querier.staticBuckets
      ).toStrictEqual([
        {
          bucket: '1#stream|0[]',
          definition: 'stream',
          inclusion_reasons: ['default'],
          priority: 3
        }
      ]);
    });

    test('streams can disable new format', () => {
      const rules = SqlSyncRules.fromYaml(
        `
streams:
  stream:
    query: SELECT id, description FROM assets
    auto_subscribe: true

config:
  edition: 2
  timestamps_iso8601: false
  versioned_bucket_ids: false
    `,
        PARSE_OPTIONS
      );

      expect(
        rules.evaluateRow({
          bucketIdTransformer: SqlSyncRules.versionedBucketIdTransformer('1'),
          sourceTable: ASSETS,
          record: {
            id: 'id',
            description: value
          }
        })
      ).toStrictEqual([
        { bucket: 'stream|0[]', data: { description: '2025-08-19 09:21:00Z', id: 'id' }, id: 'id', table: 'assets' }
      ]);
      expect(
        rules.getBucketParameterQuerier(
          normalizeQuerierOptions({}, {}, {}, SqlSyncRules.versionedBucketIdTransformer('1'))
        ).querier.staticBuckets
      ).toStrictEqual([
        {
          bucket: 'stream|0[]',
          definition: 'stream',
          inclusion_reasons: ['default'],
          priority: 3
        }
      ]);
    });
  });

  test('can use versioned bucket ids', () => {
    const rules = SqlSyncRules.fromYaml(
      `
bucket_definitions:
  mybucket:
    data:
      - SELECT id, description FROM assets

config:
  edition: 1
  versioned_bucket_ids: true
    `,
      PARSE_OPTIONS
    );

    expect(
      rules.evaluateRow({
        sourceTable: ASSETS,
        bucketIdTransformer: SqlSyncRules.versionedBucketIdTransformer('1'),
        record: {
          id: 'id',
          description: 'desc'
        }
      })
    ).toStrictEqual([{ bucket: '1#mybucket[]', data: { description: 'desc', id: 'id' }, id: 'id', table: 'assets' }]);
  });

  test('streams use new options by default', () => {
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
        bucketIdTransformer: SqlSyncRules.versionedBucketIdTransformer('1'),
        record: {
          id: 'id',
          description: new DateTimeValue('2025-08-19T09:21:00Z')
        }
      })
    ).toStrictEqual([
      { bucket: '1#stream|0[]', data: { description: '2025-08-19T09:21:00Z', id: 'id' }, id: 'id', table: 'assets' }
    ]);
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
          bucketIdTransformer: SqlSyncRules.versionedBucketIdTransformer('1'),
          record: {
            id: 'id',
            description: data
          }
        })
      ).toStrictEqual([
        {
          bucket: withFixedQuirk ? '1#mybucket[]' : 'mybucket[]',
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

      expect(
        rules.getBucketParameterQuerier(
          normalizeQuerierOptions({}, {}, {}, SqlSyncRules.versionedBucketIdTransformer('1'))
        ).querier.staticBuckets
      ).toStrictEqual([
        {
          bucket: withFixedQuirk ? '1#mybucket[]' : 'mybucket[]',
          definition: 'mybucket',
          inclusion_reasons: ['default'],
          priority: 3
        }
      ]);
    }
  });
});
