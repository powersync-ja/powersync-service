import { describe, expect } from 'vitest';
import { Equality, StableHasher } from '../../../../src/compiler/equality.js';
import {
  BucketDataSource,
  SerializedBucketDataSourceWithDataSources,
  serializedStreamBucketDataSourceEquality,
  serializeSyncPlan,
  SyncConfig
} from '../../../../src/index.js';
import { compileToSyncPlanWithoutErrors } from '../../compiler/utils.js';
import { syncTest } from './utils.js';

describe('prepared bucket data source equality', () => {
  syncTest('matches equivalent prepared bucket sources from different plans', ({ sync }) => {
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
    const first = firstBucketSource(sync.prepareWithoutHydration(firstYaml));
    const second = firstBucketSource(sync.prepareWithoutHydration(secondYaml));

    expect(first.equals(second)).toBe(true);
    expect(hashCode(first)).toEqual(hashCode(second));
    expectSerializedBucketSources(firstYaml, secondYaml, true);
  });

  syncTest('does not match when output columns differ', ({ sync }) => {
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
    const first = firstBucketSource(sync.prepareWithoutHydration(firstYaml));
    const second = firstBucketSource(sync.prepareWithoutHydration(secondYaml));

    expect(first.equals(second)).toBe(false);
    expectSerializedBucketSources(firstYaml, secondYaml, false);
  });

  syncTest('does not match when partitioning differs', ({ sync }) => {
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
    const first = firstBucketSource(sync.prepareWithoutHydration(firstYaml));
    const second = firstBucketSource(sync.prepareWithoutHydration(secondYaml));

    expect(first.equals(second)).toBe(false);
    expectSerializedBucketSources(firstYaml, secondYaml, false);
  });

  syncTest('does not match equivalent bucket sources with different stream names', ({ sync }) => {
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
    expect(first.equals(second)).toBe(false);
    expectSerializedBucketSources(firstYaml, secondYaml, false, true);
  });

  syncTest('matches when subqueries differ but the data source does not', ({ sync }) => {
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
    const first = firstBucketSource(sync.prepareWithoutHydration(firstYaml));
    const second = firstBucketSource(sync.prepareWithoutHydration(secondYaml));

    expect(first.equals(second)).toBe(true);
    expect(hashCode(first)).toEqual(hashCode(second));
    expectSerializedBucketSources(firstYaml, secondYaml, true);
  });

  syncTest('matches when only bucket input parameters differ', ({ sync }) => {
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
    const first = firstBucketSource(sync.prepareWithoutHydration(firstYaml));
    const second = firstBucketSource(sync.prepareWithoutHydration(secondYaml));

    expect(first.equals(second)).toBe(true);
    expect(hashCode(first)).toEqual(hashCode(second));
    expectSerializedBucketSources(firstYaml, secondYaml, true);
  });

  syncTest('matches buckets with the same data sources in a different order', ({ sync }) => {
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
    const first = firstBucketSource(sync.prepareWithoutHydration(firstYaml));
    const second = firstBucketSource(sync.prepareWithoutHydration(secondYaml));

    expect(first.equals(second)).toBe(true);
    expect(hashCode(first)).toEqual(hashCode(second));
    expectSerializedBucketSources(firstYaml, secondYaml, true);
  });

  syncTest('compares table-valued function output expressions by their bindings', ({ sync }) => {
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
    const first = firstBucketSource(sync.prepareWithoutHydration(firstYaml));
    const second = firstBucketSource(sync.prepareWithoutHydration(secondYaml));

    expect(first.equals(second)).toBe(true);
    expect(hashCode(first)).toEqual(hashCode(second));
    expectSerializedBucketSources(firstYaml, secondYaml, true);
  });
});

function firstBucketSource(config: SyncConfig): BucketDataSource {
  return config.bucketDataSources[0];
}

function hashCode(source: BucketDataSource): number {
  const hasher = new StableHasher();
  source.buildHash(hasher);
  return hasher.buildHashCode();
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
    expect(hashWith(serializedStreamBucketDataSourceEquality, first)).toEqual(
      hashWith(serializedStreamBucketDataSourceEquality, second)
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

function hashWith<T>(equality: Equality<T>, value: T): number {
  const hasher = new StableHasher();
  equality.hash(hasher, value);
  return hasher.buildHashCode();
}
