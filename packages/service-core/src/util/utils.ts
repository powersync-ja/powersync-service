import crypto from 'crypto';
import { BucketChecksum, OpId } from './protocol-types.js';

export type ChecksumMap = Map<string, BucketChecksum>;

export function escapeIdentifier(identifier: string) {
  return `"${identifier.replace(/"/g, '""').replace(/\./g, '"."')}"`;
}

export function hashData(type: string, id: string, data: string): number {
  const hash = crypto.createHash('sha256');
  hash.update(`put.${type}.${id}.${data}`);
  const buffer = hash.digest();
  return buffer.readUInt32LE(0);
}

export function hashDelete(sourceKey: string) {
  const hash = crypto.createHash('sha256');
  hash.update(`delete.${sourceKey}`);
  const buffer = hash.digest();
  return buffer.readUInt32LE(0);
}

export function timestampToOpId(ts: bigint): OpId {
  // Dynamic values are passed in in some cases, so we make extra sure that the
  // number is a bigint and not number or Long.
  if (typeof ts != 'bigint') {
    throw new Error(`bigint expected, got: ${ts} (${typeof ts})`);
  }
  return ts.toString(10);
}

export function checksumsDiff(previous: ChecksumMap, current: ChecksumMap) {
  // All changed ones
  const updatedBuckets = new Map<string, BucketChecksum>();

  const toRemove = new Set<string>(previous.keys());

  for (let checksum of current.values()) {
    const p = previous.get(checksum.bucket);
    if (p == null) {
      // Added
      updatedBuckets.set(checksum.bucket, checksum);
    } else {
      toRemove.delete(checksum.bucket);
      if (checksum.checksum != p.checksum || checksum.count != p.count) {
        // Updated
        updatedBuckets.set(checksum.bucket, checksum);
      } else {
        // No change
      }
    }
  }

  return {
    updatedBuckets: [...updatedBuckets.values()],
    removedBuckets: [...toRemove]
  };
}

export function addChecksums(a: number, b: number) {
  return (a + b) & 0xffffffff;
}

export function addBucketChecksums(a: BucketChecksum, b: BucketChecksum | null): BucketChecksum {
  if (b == null) {
    return a;
  } else {
    return {
      bucket: a.bucket,
      count: a.count + b.count,
      checksum: addChecksums(a.checksum, b.checksum)
    };
  }
}
