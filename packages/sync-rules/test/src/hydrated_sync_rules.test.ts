import { describe, expect, test } from 'vitest';
import { DEFAULT_HYDRATION_STATE } from '../../src/HydrationState.js';
import { HydratedSyncConfig, ScopedParameterLookup, SqlSyncRules } from '../../src/index.js';
import { ASSETS, lookupScope, PARSE_OPTIONS, USERS } from './util.js';

describe('hydrated sync rules', () => {
  const hydrationParams = { hydrationState: DEFAULT_HYDRATION_STATE };

  test('can evaluate rows across multiple sync configs', () => {
    const { config: first } = SqlSyncRules.fromYaml(
      `
bucket_definitions:
  first:
    data:
      - SELECT id, description FROM assets
    `,
      PARSE_OPTIONS
    );
    const { config: second } = SqlSyncRules.fromYaml(
      `
bucket_definitions:
  second:
    parameters:
      - SELECT users.id as user_id FROM users
    data:
      - SELECT id FROM assets WHERE assets.user_id = bucket.user_id
    `,
      PARSE_OPTIONS
    );

    const hydrated = new HydratedSyncConfig({
      definitions: [first, second],
      createParams: hydrationParams
    });

    expect(hydrated.bucketDataSources).toEqual([first.bucketDataSources[0], second.bucketDataSources[0]]);
    expect(hydrated.bucketParameterLookupSources).toEqual([second.bucketParameterLookupSources[0]]);
    expect(hydrated.bucketSourceDefinitions).toEqual([first.bucketSources[0], second.bucketSources[0]]);
    expect(hydrated.tableSyncsData(ASSETS)).toBe(true);
    expect(hydrated.tableSyncsParameters(USERS)).toBe(true);
    expect(
      hydrated
        .getSourceTables()
        .map((table) => table.name)
        .sort()
    ).toEqual(['assets', 'users']);

    expect(
      hydrated.evaluateRow({
        sourceTable: ASSETS,
        record: { id: 'asset1', description: 'test', user_id: 'user1' }
      })
    ).toEqual([
      {
        bucket: 'first[]',
        id: 'asset1',
        data: {
          id: 'asset1',
          description: 'test'
        },
        table: 'assets'
      },
      {
        bucket: 'second["user1"]',
        id: 'asset1',
        data: {
          id: 'asset1'
        },
        table: 'assets'
      }
    ]);

    expect(hydrated.evaluateParameterRow(USERS, { id: 'user1' })).toEqual([
      {
        bucketParameters: [{ user_id: 'user1' }],
        lookup: ScopedParameterLookup.direct(lookupScope('second', '1'), [])
      }
    ]);
  });

  test('matching sources can scope evaluation to source table memberships', () => {
    const { config: first } = SqlSyncRules.fromYaml(
      `
bucket_definitions:
  first:
    data:
      - SELECT id FROM assets
    `,
      PARSE_OPTIONS
    );
    const { config: second } = SqlSyncRules.fromYaml(
      `
bucket_definitions:
  second:
    parameters:
      - SELECT users.id as user_id FROM users
    data:
      - SELECT id FROM assets WHERE assets.user_id = bucket.user_id
    `,
      PARSE_OPTIONS
    );

    const hydrated = new HydratedSyncConfig({
      definitions: [first, second],
      createParams: hydrationParams
    });

    const matchingAssets = hydrated.getMatchingSources(ASSETS);
    expect(hydrated.getMatchingSources(ASSETS)).toBe(matchingAssets);
    expect(matchingAssets.bucketDataSources).toEqual([first.bucketDataSources[0], second.bucketDataSources[0]]);
    expect(matchingAssets.parameterLookupSources).toEqual([]);

    expect(
      hydrated.evaluateRowWithErrors({
        sourceTable: ASSETS,
        record: { id: 'asset1', user_id: 'user1' },
        bucketDataSources: [second.bucketDataSources[0]]
      }).results
    ).toEqual([
      {
        bucket: 'second["user1"]',
        id: 'asset1',
        data: {
          id: 'asset1'
        },
        table: 'assets'
      }
    ]);

    const matchingUsers = hydrated.getMatchingSources(USERS);
    expect(matchingUsers.bucketDataSources).toEqual([]);
    expect(matchingUsers.parameterLookupSources).toEqual([second.bucketParameterLookupSources[0]]);
    expect(
      hydrated.evaluateParameterRowWithErrors(USERS, { id: 'user1' }, { parameterLookupSources: [] }).results
    ).toEqual([]);
  });

  test('deduplicates sources with the same hydration keys', () => {
    const yaml = `
bucket_definitions:
  shared:
    parameters:
      - SELECT users.id as user_id FROM users
    data:
      - SELECT id FROM assets WHERE assets.user_id = bucket.user_id
    `;
    const { config: first } = SqlSyncRules.fromYaml(yaml, PARSE_OPTIONS);
    const { config: second } = SqlSyncRules.fromYaml(yaml, PARSE_OPTIONS);

    const hydrated = new HydratedSyncConfig({
      definitions: [first, second],
      createParams: hydrationParams
    });

    expect(hydrated.bucketDataSources).toEqual([first.bucketDataSources[0]]);
    expect(hydrated.bucketParameterLookupSources).toEqual([first.bucketParameterLookupSources[0]]);

    expect(
      hydrated.evaluateRow({
        sourceTable: ASSETS,
        record: { id: 'asset1', user_id: 'user1' }
      })
    ).toEqual([
      {
        bucket: 'shared["user1"]',
        id: 'asset1',
        data: {
          id: 'asset1'
        },
        table: 'assets'
      }
    ]);

    expect(hydrated.evaluateParameterRow(USERS, { id: 'user1' })).toEqual([
      {
        bucketParameters: [{ user_id: 'user1' }],
        lookup: ScopedParameterLookup.direct(lookupScope('shared', '1'), [])
      }
    ]);
  });

  test('requires matching compatibility contexts for multiple sync configs', () => {
    const { config: legacy } = SqlSyncRules.fromYaml(
      `
bucket_definitions:
  first:
    data:
      - SELECT id FROM assets
    `,
      PARSE_OPTIONS
    );
    const { config: edition2 } = SqlSyncRules.fromYaml(
      `
config:
  edition: 2
bucket_definitions:
  second:
    data:
      - SELECT id FROM assets
    `,
      PARSE_OPTIONS
    );

    expect(
      () =>
        new HydratedSyncConfig({
          definitions: [legacy, edition2],
          createParams: hydrationParams
        })
    ).toThrow(/same CompatibilityContext/);
  });
});
