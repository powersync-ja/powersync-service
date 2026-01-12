import { describe, expect, test } from 'vitest';
import { compileSingleStreamAndSerialize } from './utils.js';

describe('new sync stream features', () => {
  test('order-independent parameters', () => {
    // We should be able to merge buckets with the same parameters in a different order. Obviously, it's important that
    // we re-order the instantiation as well to keep it consistent.
    expect(
      compileSingleStreamAndSerialize(
        "SELECT * FROM stores WHERE region = subscription.parameter('region') AND org = auth.parameter('org')",
        "SELECT * FROM products WHERE org = auth.parameter('org') AND region = subscription.parameter('region')"
      )
    ).toMatchSnapshot();
  });
});
