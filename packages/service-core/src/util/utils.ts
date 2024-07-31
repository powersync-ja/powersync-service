import * as sync_rules from '@powersync/service-sync-rules';
import * as bson from 'bson';
import crypto from 'crypto';
import * as uuid from 'uuid';
import { BucketChecksum, OpId } from './protocol-types.js';

import * as storage from '../storage/storage-index.js';

export type ChecksumMap = Map<string, BucketChecksum>;

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

export function getUuidReplicaIdentityString(
  tuple: sync_rules.ToastableSqliteRow,
  columns: storage.ColumnDescriptor[]
): string {
  const rawIdentity = getRawReplicaIdentity(tuple, columns);

  return uuidForRow(rawIdentity);
}

export function uuidForRow(row: sync_rules.SqliteRow): string {
  // Important: This must not change, since it will affect how ids are generated.
  // Use BSON so that it's a well-defined format without encoding ambiguities.
  const repr = bson.serialize(row);
  return uuid.v5(repr, ID_NAMESPACE);
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

export function isCompleteRow(row: sync_rules.ToastableSqliteRow): row is sync_rules.SqliteRow {
  return !hasToastedValues(row);
}
