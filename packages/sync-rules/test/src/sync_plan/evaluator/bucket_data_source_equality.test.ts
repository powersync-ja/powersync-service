import { describe, expect, it } from 'vitest';
import { StableHasher } from '../../../../src/compiler/equality.js';
import {
  BucketDataSource,
  SerializedBucketDataSourceWithDataSources,
  serializedStreamBucketDataSourceEquality,
  serializeSyncPlan,
  SyncConfig
} from '../../../../src/index.js';
import { compileToSyncPlanWithoutErrors } from '../../compiler/utils.js';

describe('prepared bucket data source equality', () => {
  it('matches equivalent prepared bucket sources from different plans', () => {
    const firstYaml = `
config:
  edition: 3

streams:
  stream:
    query: SELECT id, owner_id FROM todos WHERE owner_id = auth.user_id()
`;
    const secondYaml = `
config:
  edition: 3

streams:
  stream:
    query: SELECT id, owner_id FROM todos WHERE owner_id = auth.user_id()
`;
    expectSerializedBucketSources(firstYaml, secondYaml, true);
  });

  it('does not match when output columns differ', () => {
    const firstYaml = `
config:
  edition: 3

streams:
  stream:
    query: SELECT id, owner_id FROM todos WHERE owner_id = auth.user_id()
`;
    const secondYaml = `
config:
  edition: 3

streams:
  stream:
    query: SELECT id, title FROM todos WHERE owner_id = auth.user_id()
`;
    expectSerializedBucketSources(firstYaml, secondYaml, false);
  });

  it('does not match when partitioning differs', () => {
    const firstYaml = `
config:
  edition: 3

streams:
  stream:
    query: SELECT * FROM todos WHERE owner_id = auth.user_id()
`;
    const secondYaml = `
config:
  edition: 3

streams:
  stream:
    query: SELECT * FROM todos WHERE project_id = auth.user_id()
`;
    expectSerializedBucketSources(firstYaml, secondYaml, false);
  });

  it('does not match equivalent bucket sources with different stream names', () => {
    const firstYaml = `
config:
  edition: 3

streams:
  first_stream:
    query: SELECT id, owner_id FROM todos WHERE owner_id = auth.user_id()
`;
    const secondYaml = `
config:
  edition: 3

streams:
  second_stream:
    query: SELECT id, owner_id FROM todos WHERE owner_id = auth.user_id()
`;
    const first = firstBucketSource(sync.prepareWithoutHydration(firstYaml));
    const second = firstBucketSource(sync.prepareWithoutHydration(secondYaml));

    expect(first.uniqueName).not.toEqual(second.uniqueName);
    expectSerializedBucketSources(firstYaml, secondYaml, false, true);
  });

  it('matches when subqueries differ but the data source does not', () => {
    const firstYaml = `
config:
  edition: 3

streams:
  stream:
    query: |
      SELECT * FROM projects
      WHERE org_id IN (
        SELECT org_id FROM memberships WHERE user_id = auth.user_id()
      )
`;
    const secondYaml = `
config:
  edition: 3

streams:
  stream:
    query: |
      SELECT * FROM projects
      WHERE org_id IN (
        SELECT id FROM organizations WHERE owner_id = auth.user_id()
      )
`;
    expectSerializedBucketSources(firstYaml, secondYaml, true);
  });

  it('matches when only bucket input parameters differ', () => {
    const firstYaml = `
config:
  edition: 3

streams:
  stream:
    query: SELECT * FROM todos WHERE owner_id = auth.user_id()
`;
    const secondYaml = `
config:
  edition: 3

streams:
  stream:
    query: SELECT * FROM todos WHERE owner_id = auth.parameter('user_id')
`;
    expectSerializedBucketSources(firstYaml, secondYaml, true);
  });

  it('matches buckets with the same data sources in a different order', () => {
    const firstYaml = `
config:
  edition: 3

streams:
  stream:
    queries:
      - SELECT * FROM products
      - SELECT * FROM stores
`;
    const secondYaml = `
config:
  edition: 3

streams:
  stream:
    queries:
      - SELECT * FROM stores
      - SELECT * FROM products
`;
    expectSerializedBucketSources(firstYaml, secondYaml, true);
  });

  it('compares table-valued function output expressions by their bindings', () => {
    const firstYaml = `
config:
  edition: 3

streams:
  stream:
    query: |
      SELECT customers.id as id
      FROM customers, json_each(customers.active_regions) AS region
      WHERE region.value < 'm'
`;
    const secondYaml = `
config:
  edition: 3

streams:
  stream:
    query: |
      SELECT customers.id as id
      FROM customers, json_each(customers.active_regions) AS region
      WHERE region.value < 'm'
`;

    expectSerializedBucketSources(firstYaml, secondYaml, true);
  });
});

function firstBucketSource(config: SyncConfig): BucketDataSource {
  return config.bucketDataSources[0];
}

function expectSerializedBucketSources(
  firstYaml: string,
  secondYaml: string,
  equal: boolean,
  expectDifferentUniqueNames = false
) {
  const first = firstSerializedBucketSource(firstYaml);
  const second = firstSerializedBucketSource(secondYaml);

  if (expectDifferentUniqueNames) {
    expect(first.bucket.uniqueName).not.toEqual(second.bucket.uniqueName);
  }

  expect(serializedStreamBucketDataSourceEquality.equals(first, second)).toBe(equal);
  if (equal) {
    expect(StableHasher.hashWith(serializedStreamBucketDataSourceEquality, first)).toEqual(
      StableHasher.hashWith(serializedStreamBucketDataSourceEquality, second)
    );
  }
}

function firstSerializedBucketSource(yaml: string): SerializedBucketDataSourceWithDataSources {
  const plan = serializeSyncPlan(compileToSyncPlanWithoutErrors(yaml));
  return {
    bucket: plan.buckets[0],
    dataSources: plan.dataSources
  };
}
