import { describe, expect } from 'vitest';
import { StableHasher } from '../../../../src/compiler/equality.js';
import { BucketDataSource, SyncConfig } from '../../../../src/index.js';
import { syncTest } from './utils.js';

describe('prepared bucket data source equality', () => {
  syncTest('matches equivalent prepared bucket sources from different plans', ({ sync }) => {
    const first = firstBucketSource(
      sync.prepareWithoutHydration(`
config:
  edition: 3

streams:
  stream:
    query: SELECT id, owner_id FROM todos WHERE owner_id = auth.user_id()
`)
    );
    const second = firstBucketSource(
      sync.prepareWithoutHydration(`
config:
  edition: 3

streams:
  stream:
    query: SELECT id, owner_id FROM todos WHERE owner_id = auth.user_id()
`)
    );

    expect(first.equals(second)).toBe(true);
    expect(hashCode(first)).toEqual(hashCode(second));
  });

  syncTest('does not match when output columns differ', ({ sync }) => {
    const first = firstBucketSource(
      sync.prepareWithoutHydration(`
config:
  edition: 3

streams:
  stream:
    query: SELECT id, owner_id FROM todos WHERE owner_id = auth.user_id()
`)
    );
    const second = firstBucketSource(
      sync.prepareWithoutHydration(`
config:
  edition: 3

streams:
  stream:
    query: SELECT id, title FROM todos WHERE owner_id = auth.user_id()
`)
    );

    expect(first.equals(second)).toBe(false);
  });

  syncTest('does not match when partitioning differs', ({ sync }) => {
    const first = firstBucketSource(
      sync.prepareWithoutHydration(`
config:
  edition: 3

streams:
  stream:
    query: SELECT * FROM todos WHERE owner_id = auth.user_id()
`)
    );
    const second = firstBucketSource(
      sync.prepareWithoutHydration(`
config:
  edition: 3

streams:
  stream:
    query: SELECT * FROM todos WHERE project_id = auth.user_id()
`)
    );

    expect(first.equals(second)).toBe(false);
  });

  syncTest('matches equivalent bucket sources with different stream names', ({ sync }) => {
    const first = firstBucketSource(
      sync.prepareWithoutHydration(`
config:
  edition: 3

streams:
  first_stream:
    query: SELECT id, owner_id FROM todos WHERE owner_id = auth.user_id()
`)
    );
    const second = firstBucketSource(
      sync.prepareWithoutHydration(`
config:
  edition: 3

streams:
  second_stream:
    query: SELECT id, owner_id FROM todos WHERE owner_id = auth.user_id()
`)
    );

    expect(first.uniqueName).not.toEqual(second.uniqueName);
    expect(first.equals(second)).toBe(true);
    expect(hashCode(first)).toEqual(hashCode(second));
  });

  syncTest('matches when subqueries differ but the data source does not', ({ sync }) => {
    const first = firstBucketSource(
      sync.prepareWithoutHydration(`
config:
  edition: 3

streams:
  stream:
    query: |
      SELECT * FROM projects
      WHERE org_id IN (
        SELECT org_id FROM memberships WHERE user_id = auth.user_id()
      )
`)
    );
    const second = firstBucketSource(
      sync.prepareWithoutHydration(`
config:
  edition: 3

streams:
  stream:
    query: |
      SELECT * FROM projects
      WHERE org_id IN (
        SELECT id FROM organizations WHERE owner_id = auth.user_id()
      )
`)
    );

    expect(first.equals(second)).toBe(true);
    expect(hashCode(first)).toEqual(hashCode(second));
  });

  syncTest('matches when only bucket input parameters differ', ({ sync }) => {
    const first = firstBucketSource(
      sync.prepareWithoutHydration(`
config:
  edition: 3

streams:
  stream:
    query: SELECT * FROM todos WHERE owner_id = auth.user_id()
`)
    );
    const second = firstBucketSource(
      sync.prepareWithoutHydration(`
config:
  edition: 3

streams:
  stream:
    query: SELECT * FROM todos WHERE owner_id = auth.parameter('user_id')
`)
    );

    expect(first.equals(second)).toBe(true);
    expect(hashCode(first)).toEqual(hashCode(second));
  });

  syncTest('matches buckets with the same data sources in a different order', ({ sync }) => {
    const first = firstBucketSource(
      sync.prepareWithoutHydration(`
config:
  edition: 3

streams:
  stream:
    queries:
      - SELECT * FROM products
      - SELECT * FROM stores
`)
    );
    const second = firstBucketSource(
      sync.prepareWithoutHydration(`
config:
  edition: 3

streams:
  stream:
    queries:
      - SELECT * FROM stores
      - SELECT * FROM products
`)
    );

    expect(first.equals(second)).toBe(true);
    expect(hashCode(first)).toEqual(hashCode(second));
  });

  syncTest('compares table-valued function output expressions by their bindings', ({ sync }) => {
    const first = firstBucketSource(
      sync.prepareWithoutHydration(`
config:
  edition: 3

streams:
  stream:
    query: |
      SELECT customers.id as id
      FROM customers, json_each(customers.active_regions) AS region
      WHERE region.value < 'm'
`)
    );
    const second = firstBucketSource(
      sync.prepareWithoutHydration(`
config:
  edition: 3

streams:
  stream:
    query: |
      SELECT customers.id as id
      FROM customers, json_each(customers.active_regions) AS region
      WHERE region.value < 'm'
`)
    );

    expect(first.equals(second)).toBe(true);
    expect(hashCode(first)).toEqual(hashCode(second));
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
