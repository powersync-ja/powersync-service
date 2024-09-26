import crypto from 'crypto';

import { logger } from '@powersync/lib-services-framework';
import * as pgwire from '@powersync/service-jpgwire';
import { pgwireRows } from '@powersync/service-jpgwire';
import { PartialChecksum } from '../storage/ChecksumCache.js';
import * as storage from '../storage/storage-index.js';
import { retriedQuery } from './pgwire_utils.js';
import { BucketChecksum, OpId } from './protocol-types.js';

export type ChecksumMap = Map<string, BucketChecksum>;

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

export async function getClientCheckpoint(
  db: pgwire.PgClient,
  bucketStorage: storage.BucketStorageFactory,
  options?: { timeout?: number }
): Promise<OpId> {
  const start = Date.now();

  const [{ lsn }] = pgwireRows(await db.query(`SELECT pg_logical_emit_message(false, 'powersync', 'ping') as lsn`));

  // This old API needs a persisted checkpoint id.
  // Since we don't use LSNs anymore, the only way to get that is to wait.

  const timeout = options?.timeout ?? 50_000;

  logger.info(`Waiting for LSN checkpoint: ${lsn}`);
  while (Date.now() - start < timeout) {
    const cp = await bucketStorage.getActiveCheckpoint();
    if (!cp.hasSyncRules()) {
      throw new Error('No sync rules available');
    }
    if (cp.lsn >= lsn) {
      logger.info(`Got write checkpoint: ${lsn} : ${cp.checkpoint}`);
      return cp.checkpoint;
    }

    await new Promise((resolve) => setTimeout(resolve, 30));
  }

  throw new Error('Timeout while waiting for checkpoint');
}

export async function createWriteCheckpoint(
  db: pgwire.PgClient,
  bucketStorage: storage.BucketStorageFactory,
  user_id: string
): Promise<bigint> {
  const [{ lsn }] = pgwireRows(
    await retriedQuery(db, `SELECT pg_logical_emit_message(false, 'powersync', 'ping') as lsn`)
  );

  const id = await bucketStorage.createWriteCheckpoint(user_id, { '1': lsn });
  logger.info(`Write checkpoint 2: ${JSON.stringify({ lsn, id: String(id) })}`);
  return id;
}

export function checkpointUserId(user_id: string | undefined, client_id: string | undefined) {
  if (user_id == null) {
    throw new Error('user_id is required');
  }
  if (client_id == null) {
    return user_id;
  }
  return `${user_id}/${client_id}`;
}
