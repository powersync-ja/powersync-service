import { OplogEntry } from '@/util/protocol-types.js';
import { addChecksums, reduceBucket } from '@/util/utils.js';
import { expect } from 'vitest';

/**
 * Validate this property, as described in /docs/bucket-properties.md:
 *
 * $r(B_{[..id_n]}) = r(r(B_{[..id_i]}) \cup B_{[id_{i+1}..id_n]}) \;\forall\; i \in [1..n]$
 *
 * We test that a client syncing the entire bucket in one go (left side of the equation),
 * ends up with the same result as another client syncing up to operation id_i, then sync
 * the rest.
 */
export function validateBucket(bucket: OplogEntry[]) {
  const r1 = reduceBucket(bucket);
  for (let i = 0; i <= bucket.length; i++) {
    const r2 = reduceBucket(bucket.slice(0, i + 1));
    const b3 = bucket.slice(i + 1);
    const r3 = r2.concat(b3);
    const r4 = reduceBucket(r3);
    expect(r4).toEqual(r1);
  }

  // This is the same check, just implemented differently
  validateCompactedBucket(bucket, bucket);
}

/**
 * Validate these properties for a bucket $B$ and its compacted version $B'$,:
 * as described in /docs/bucket-properties.md:
 *
 * 1. $r(B) = r(B')$
 * 2. $r(B_{[..c]}) = r(r(B_{[..c_i]}) \cup B'_{[c_i+1..c]}) \;\forall\; c_i \in B$
 *
 * The first one is that the result of syncing the original bucket is the same as
 * syncing the compacted bucket.
 *
 * The second property is that result of syncing the entire original bucket, is the same
 * as syncing any partial version of that (up to op $c_i$), and then continue syncing
 * using the compacted bucket.
 */
export function validateCompactedBucket(bucket: OplogEntry[], compacted: OplogEntry[]) {
  // r(B_{[..c]})
  const r1 = reduceBucket(bucket);
  // r(B) = r(B')
  expect(reduceBucket(compacted)).toEqual(r1);

  for (let i = 0; i < bucket.length; i++) {
    // r(B_{[..c_i]})
    const r2 = reduceBucket(bucket.slice(0, i + 1));
    const c_i = BigInt(bucket[i].op_id);
    // B'_{[c_i+1..c]}
    const b3 = compacted.filter((op) => BigInt(op.op_id) > c_i);
    // r(B_{[..c_i]}) \cup B'_{[c_i+1..c]}
    const r3 = r2.concat(b3);
    // r(r(B_{[..c_i]}) \cup B'_{[c_i+1..c]})
    const r4 = reduceBucket(r3);
    expect(r4).toEqual(r1);
  }
}
