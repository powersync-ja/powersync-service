import { assert, describe, expect, test } from 'vitest';
import { SqlSyncRules } from '../../src/index.js';
import { ASSETS, BASIC_SCHEMA, PARSE_OPTIONS } from './util.js';

describe('streams', () => {
  test('without parameters', () => {
    const desc = parseSingleBucketDescription(`
streams:
  lists:
    query: SELECT * FROM assets
`);
    expect(desc.bucketParameters).toBeFalsy();
  });

  test('static filter', () => {
    const desc = parseSingleBucketDescription(`
streams:
  lists:
    query: SELECT * FROM assets WHERE count > 5
`);
    expect(desc.bucketParameters).toBeFalsy();
    assert.isEmpty(desc.parameterQueries);

    assert.isEmpty(
      desc.evaluateRow({
        sourceTable: ASSETS,
        record: {
          count: 4
        }
      })
    );

    expect(
      desc.evaluateRow({
        sourceTable: ASSETS,
        record: {
          count: 6
        }
      })
    ).toHaveLength(1);
  });

  test('stream param', () => {
    const desc = parseSingleBucketDescription(`
streams:
  lists:
    query: SELECT * FROM assets WHERE id = stream.params() ->> 'id';
`);

    // This should be desuraged to
    //  params: SELECT request.params() ->> 'id' AS p0
    //  data: SELECT * FROM assets WHERE id = bucket.p0

    expect(desc.globalParameterQueries).toHaveLength(1);
    const [parameter] = desc.globalParameterQueries;
    expect(parameter.bucketParameters).toEqual(['p0']);

    const [data] = desc.dataQueries;
    expect(data.bucketParameters).toEqual(['bucket.p0']);
  });

  test('user filter', () => {
    const desc = parseSingleBucketDescription(`
streams:
  lists:
    query: SELECT * FROM assets WHERE request.jwt() ->> 'isAdmin'
`);

    // This should be desuraged to
    //  params: SELECT request.params() ->> 'id' AS p0
    //  data: SELECT * FROM assets WHERE id = bucket.p0

    const [data] = desc.dataQueries;
    expect(data.bucketParameters).toEqual(['bucket.p0']);
  });
});

/**

SELECT * FROM assets WHERE id IN (SELECT id FROM asset_groups WHERE owner = request.user_id())
  parameter: SELECT id AS p0 FROM asset_groups WHERE owner = request.user_id()
  data: SELECT * FROM assets WHERE id = bucket.p0

SELECT * FROM assets WHERE id IN (SELECT id FROM asset_groups WHERE owner = request.user_id())
                        OR count > 10
  parameter: SELECT id AS p0 FROM asset_groups WHERE owner = request.user_id()
  data: SELECT * FROM assets WHERE id = bucket.p0 OR count > 10

SELECT * FROM assets WHERE id IN (SELECT id FROM asset_groups WHERE owner = request.user_id())
                       OR request.jwt() ->> 'isAdmin'
  parameter: SELECT id AS p0, request.jwt() ->> 'isAdmin' AS p1 FROM asset_groups WHERE owner = request.user_id()
  parameter: SELECT NULL as p0, request.jwt() ->> 'isAdmin' AS p1
  data: SELECT * FROM assets WHERE id = bucket.p0 OR bucket.p1

SELECT * FROM assets WHERE id IN (SELECT id FROM asset_groups WHERE owner = request.user_id())
                       AND request.jwt() ->> 'isAdmin'
  parameter: SELECT id AS p0, request.jwt() ->> 'isAdmin' AS p1 FROM asset_groups WHERE owner = request.user_id()
                                                                                        AND request.jwt() ->> 'isAdmin'
  data: SELECT * FROM assets WHERE id = bucket.p0

SELECT * FROM assets WHERE id IN (SELECT id FROM asset_groups WHERE owner = request.user_id())
                        OR name IN (SELECT name FROM test2 WHERE owner = request.user_id())
  parameter: SELECT id AS p0, NULL AS p1 FROM asset_groups WHERE owner = request.user_id()
  parameter: SELECT NULL AS p0, NULL AS p1 FROM test2 WHERE owner = request.user_id()
  data: SELECT * FROM assets WHERE id = bucket.p0 OR name = bucket.p1

SELECT * FROM assets WHERE id IN (SELECT id FROM asset_groups WHERE owner = request.user_id())
                        AND name IN (SELECT name FROM test2 WHERE owner = request.user_id())
  parameter: SELECT id AS p0, NULL AS p1 FROM asset_groups WHERE owner = request.user_id()
  parameter: SELECT NULL AS p0, NULL AS p1 FROM test2 WHERE owner = request.user_id()
  data: SELECT * FROM assets WHERE id = bucket.p0 AND name = bucket.p1
  */

const options = { schema: BASIC_SCHEMA, ...PARSE_OPTIONS };

function parseSyncRules(yaml: string) {
  const rules = SqlSyncRules.fromYaml(yaml, options);
  assert.isEmpty(rules.errors);
  return rules;
}

function parseSingleBucketDescription(yaml: string) {
  const rules = parseSyncRules(yaml);
  expect(rules.bucketDescriptors).toHaveLength(1);
  return rules.bucketDescriptors[0];
}
