import { OplogEntry } from '@/util/protocol-types.js';
import { addChecksums } from '@/util/utils.js';
import { expect } from 'vitest';

/**
 * Reduce a bucket to the final state as stored on the client.
 */
export function reduceBucket(operations: OplogEntry[]) {
  let rowState = new Map<string, OplogEntry>();
  let otherChecksum = 0;

  for (let op of operations) {
    const key = rowKey(op);
    if (op.op == 'PUT') {
      const existing = rowState.get(key);
      if (existing) {
        otherChecksum = addChecksums(otherChecksum, existing.checksum as number);
      }
      rowState.set(key, op);
    } else if (op.op == 'REMOVE') {
      const existing = rowState.get(key);
      if (existing) {
        otherChecksum = addChecksums(otherChecksum, existing.checksum as number);
      }
      rowState.delete(key);
      otherChecksum = addChecksums(otherChecksum, op.checksum as number);
    } else if (op.op == 'CLEAR') {
      rowState.clear();
      otherChecksum = op.checksum as number;
    } else if (op.op == 'MOVE') {
      otherChecksum = addChecksums(otherChecksum, op.checksum as number);
    } else {
      throw new Error(`Unknown operation ${op.op}`);
    }
  }

  const puts = [...rowState.values()].sort((a, b) => {
    return Number(BigInt(a.op_id) - BigInt(b.op_id));
  });

  let finalState: OplogEntry[] = [
    // Special operation to indiciate the checksum remainder
    { op_id: '0', op: 'CLEAR', checksum: otherChecksum },
    ...puts
  ];

  return finalState;
}

function rowKey(entry: OplogEntry) {
  return `${entry.object_type}/${entry.object_id}/${entry.subkey}`;
}

/**
 * Validate this property:
 *
 * $r(B_{[..id_n]}) = r(r(B_{[..id_i]}) \cup B_{[id_{i+1}..id_n]}) \;\forall\; i \in [1..n]$
 */
export function validateReducedSets(bucket: OplogEntry[]) {
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
 * Validate this property:
 *
 * $r(B_{[..c]}) = r(r(B_{[..c_i]}) \cup B'_{[c_i+1..c]}) \;\forall\; c_i \in B$
 */
export function validateCompactedBucket(bucket: OplogEntry[], compacted: OplogEntry[]) {
  // r(B_{[..c]})
  const r1 = reduceBucket(bucket);
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
