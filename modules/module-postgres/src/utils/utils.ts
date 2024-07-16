import * as pgwire from '@powersync/service-jpgwire';

import { storage, utils } from '@powersync/service-core';
import { logger } from '@powersync/lib-services-framework';

import * as pgwire_utils from './pgwire_utils.js';

// TODO these should probably be on the API provider

export async function getClientCheckpoint(
  db: pgwire.PgClient,
  bucketStorage: storage.BucketStorageFactory,
  options?: { timeout?: number }
): Promise<utils.OpId> {
  const start = Date.now();

  const [{ lsn }] = pgwire.pgwireRows(
    await db.query(`SELECT pg_logical_emit_message(false, 'powersync', 'ping') as lsn`)
  );

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
  const [{ lsn }] = pgwire.pgwireRows(
    await pgwire_utils.retriedQuery(db, `SELECT pg_logical_emit_message(false, 'powersync', 'ping') as lsn`)
  );

  const id = await bucketStorage.createWriteCheckpoint(user_id, { '1': lsn });
  logger.info(`Write checkpoint 2: ${JSON.stringify({ lsn, id: String(id) })}`);
  return id;
}
