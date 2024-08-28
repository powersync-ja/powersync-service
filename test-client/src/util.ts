import type * as types from '@powersync/service-core';

export type BucketData = Record<string, types.OplogEntry[]>;

/**
 * Combine all chunks of received data, excluding any data after the checkpoint.
 */
export function normalizeData(
  checkpoint: types.StreamingSyncCheckpoint,
  chunks: types.StreamingSyncData[],
  options: { raw: boolean }
) {
  const lastOpId = BigInt(checkpoint.checkpoint.last_op_id);
  let buckets: BucketData = {};
  for (let {
    data: { bucket, data }
  } of chunks) {
    buckets[bucket] ??= [];
    for (let entry of data) {
      if (BigInt(entry.op_id) > lastOpId) {
        continue;
      }
      buckets[bucket].push(entry);
    }
  }

  if (options.raw) {
    return buckets;
  }

  return Object.fromEntries(
    Object.entries(buckets).map(([bucket, entries]) => {
      return [bucket, reduceBucket(entries)];
    })
  );
}

export function isStreamingSyncData(line: types.StreamingSyncLine): line is types.StreamingSyncData {
  return (line as types.StreamingSyncData).data != null;
}

export function isCheckpointComplete(line: types.StreamingSyncLine): line is types.StreamingSyncCheckpointComplete {
  return (line as types.StreamingSyncCheckpointComplete).checkpoint_complete != null;
}

export function isCheckpoint(line: types.StreamingSyncLine): line is types.StreamingSyncCheckpoint {
  return (line as types.StreamingSyncCheckpoint).checkpoint != null;
}

/**
 * Reduce a bucket to the final state as stored on the client.
 *
 * This keeps the final state for each row as a PUT operation.
 *
 * All other operations are replaced with a single CLEAR operation,
 * summing their checksums, and using a 0 as an op_id.
 *
 * This is the function $r(B)$, as described in /docs/bucket-properties.md.
 */
export function reduceBucket(operations: types.OplogEntry[]) {
  let rowState = new Map<string, types.OplogEntry>();
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

  let finalState: types.OplogEntry[] = [
    // Special operation to indiciate the checksum remainder
    { op_id: '0', op: 'CLEAR', checksum: otherChecksum },
    ...puts
  ];

  return finalState;
}

function rowKey(entry: types.OplogEntry) {
  return `${entry.object_type}/${entry.object_id}/${entry.subkey}`;
}

export function addChecksums(a: number, b: number) {
  return (a + b) & 0xffffffff;
}
