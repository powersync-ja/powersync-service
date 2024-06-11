import crypto from 'crypto';
import * as pgwire from '@powersync/service-jpgwire';
import { pgwireRows } from '@powersync/service-jpgwire';
import * as micro from '@journeyapps-platform/micro';

import * as storage from '@/storage/storage-index.js';
import { BucketChecksum, OpId } from './protocol-types.js';
import { retriedQuery } from './pgwire_utils.js';

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

export function fillEmptyChecksums(currentBuckets: string[], checksums: BucketChecksum[]) {
  // All current values
  const nextBuckets = new Map<string, BucketChecksum>();

  for (let checksum of checksums) {
    // Added checksum
    nextBuckets.set(checksum.bucket, checksum);
  }

  for (let bucket of currentBuckets) {
    if (!nextBuckets.has(bucket)) {
      // Empty diff - empty bucket
      const checksum: BucketChecksum = {
        bucket,
        checksum: 0,
        count: 0
      };
      nextBuckets.set(bucket, checksum);
    }
  }

  return nextBuckets;
}

export function checksumsDiff(
  previousBuckets: Map<string, BucketChecksum>,
  currentBuckets: string[],
  checksumDiff: BucketChecksum[]
) {
  // All changed ones
  const updatedBuckets = new Map<string, BucketChecksum>();
  // All current values
  const nextBuckets = new Map<string, BucketChecksum>();

  for (let cdiff of checksumDiff) {
    const p = previousBuckets.get(cdiff.bucket);
    if (p == null) {
      // Added
      updatedBuckets.set(cdiff.bucket, cdiff);
      nextBuckets.set(cdiff.bucket, cdiff);
    } else {
      // Updated
      const checksum: BucketChecksum = addBucketChecksums(p, cdiff);
      updatedBuckets.set(checksum.bucket, checksum);
      nextBuckets.set(checksum.bucket, checksum);
      previousBuckets.delete(cdiff.bucket);
    }
  }

  for (let bucket of currentBuckets) {
    if (!updatedBuckets.has(bucket)) {
      // Empty diff - either empty bucket, or unchanged
      const p = previousBuckets.get(bucket);
      if (p == null) {
        // Emtpy bucket
        const checksum: BucketChecksum = {
          bucket,
          checksum: 0,
          count: 0
        };
        updatedBuckets.set(bucket, checksum);
        nextBuckets.set(bucket, checksum);
      } else {
        // Unchanged bucket
        nextBuckets.set(bucket, p);
        previousBuckets.delete(bucket);
      }
    }
  }

  const removedBuckets: string[] = [...previousBuckets.keys()];
  return {
    updatedBuckets: [...updatedBuckets.values()],
    removedBuckets,
    nextBuckets
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

  micro.logger.info(`Waiting for LSN checkpoint: ${lsn}`);
  while (Date.now() - start < timeout) {
    const cp = await bucketStorage.getActiveCheckpoint();
    if (!cp.hasSyncRules()) {
      throw new Error('No sync rules available');
    }
    if (cp.lsn >= lsn) {
      micro.logger.info(`Got write checkpoint: ${lsn} : ${cp.checkpoint}`);
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
  micro.logger.info(`Write checkpoint 2: ${JSON.stringify({ lsn, id: String(id) })}`);
  return id;
}
