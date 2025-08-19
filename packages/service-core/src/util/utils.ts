import * as sync_rules from '@powersync/service-sync-rules';
import * as bson from 'bson';
import crypto from 'crypto';
import * as uuid from 'uuid';
import { BucketChecksum, ProtocolOpId, OplogEntry } from './protocol-types.js';

import * as storage from '../storage/storage-index.js';

import { PartialChecksum } from '../storage/ChecksumCache.js';
import { ServiceAssertionError } from '@powersync/lib-services-framework';

export type ChecksumMap = Map<string, BucketChecksum>;

/**
 * op_id as used internally, for individual operations and checkpoints.
 *
 * This is just a type alias, but serves to document that we're working with an op_id.
 */
export type InternalOpId = bigint;

export const ID_NAMESPACE = 'a396dd91-09fc-4017-a28d-3df722f651e9';

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

/**
 * Internally we always use bigint for op_ids. Externally (in JSON) we use strings.
 * This converts between the two.
 */
export function internalToExternalOpId(ts: InternalOpId): ProtocolOpId {
  // Dynamic values are passed in in some cases, so we make extra sure that the
  // number is a bigint and not number or Long.
  if (typeof ts != 'bigint') {
    throw new ServiceAssertionError(`bigint expected, got: ${ts} (${typeof ts})`);
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

export function addBucketChecksums(a: BucketChecksum, b: PartialChecksum | null): BucketChecksum {
  if (b == null) {
    return a;
  } else if (b.isFullChecksum) {
    return {
      bucket: b.bucket,
      count: b.partialCount,
      checksum: b.partialChecksum
    };
  } else {
    return {
      bucket: a.bucket,
      count: a.count + b.partialCount,
      checksum: addChecksums(a.checksum, b.partialChecksum)
    };
  }
}

function getRawReplicaIdentity(
  tuple: sync_rules.ToastableSqliteRow,
  columns: storage.ColumnDescriptor[]
): Record<string, any> {
  let result: Record<string, any> = {};
  for (let column of columns) {
    const name = column.name;
    result[name] = tuple[name];
  }
  return result;
}

export function getUuidReplicaIdentityBson(
  tuple: sync_rules.ToastableSqliteRow,
  columns: storage.ColumnDescriptor[]
): bson.UUID {
  if (columns.length == 0) {
    // REPLICA IDENTITY NOTHING - generate random id
    return new bson.UUID(uuid.v4());
  }
  const rawIdentity = getRawReplicaIdentity(tuple, columns);

  return uuidForRowBson(rawIdentity);
}

export function uuidForRowBson(row: sync_rules.SqliteRow): bson.UUID {
  // Important: This must not change, since it will affect how ids are generated.
  // Use BSON so that it's a well-defined format without encoding ambiguities.
  const repr = bson.serialize(row);
  const buffer = Buffer.alloc(16);
  return new bson.UUID(uuid.v5(repr, ID_NAMESPACE, buffer));
}

export function hasToastedValues(row: sync_rules.ToastableSqliteRow) {
  for (let key in row) {
    if (typeof row[key] == 'undefined') {
      return true;
    }
  }
  return false;
}

/**
 * Returns true if we have a complete row.
 *
 * If we don't store data, we assume we always have a complete row.
 */
export function isCompleteRow(
  storeData: boolean,
  row: sync_rules.ToastableSqliteRow
): row is sync_rules.SqliteInputRow {
  if (!storeData) {
    // Assume the row is complete - no need to check
    return true;
  }
  return !hasToastedValues(row);
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
 *
 * Used for tests.
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

/**
 * Flattens string to reduce memory usage (around 320 bytes -> 120 bytes),
 * at the cost of some upfront CPU usage.
 *
 * From: https://github.com/davidmarkclements/flatstr/issues/8
 */
export function flatstr(s: string) {
  s.match(/\n/g);
  return s;
}

function rowKey(entry: OplogEntry) {
  return `${entry.object_type}/${entry.object_id}/${entry.subkey}`;
}

/**
 * Estimate in-memory size of row.
 */
export function estimateRowSize(record: sync_rules.ToastableSqliteRow | undefined) {
  if (record == null) {
    return 12;
  }
  let size = 0;
  for (let [key, value] of Object.entries(record)) {
    size += 12 + key.length;
    // number | string | null | bigint | Uint8Array
    if (value == null) {
      size += 4;
    } else if (typeof value == 'number') {
      size += 8;
    } else if (typeof value == 'bigint') {
      size += 8;
    } else if (typeof value == 'string') {
      size += value.length;
    } else if (value instanceof Uint8Array) {
      size += value.byteLength;
    }
  }
  return size;
}
