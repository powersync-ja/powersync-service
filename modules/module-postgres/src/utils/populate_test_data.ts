import * as crypto from 'crypto';
import { Worker, isMainThread, parentPort, workerData } from 'node:worker_threads';

import * as pgwire from '@powersync/service-jpgwire';

// This util is actually for tests only, but we need it compiled to JS for the service to work, so it's placed in the service.

export interface PopulateDataOptions {
  connection: pgwire.NormalizedConnectionConfig;
  num_transactions: number;
  per_transaction: number;
  size: number;
}

if (isMainThread || parentPort == null) {
  // Not a worker - ignore
} else {
  try {
    const options = workerData as PopulateDataOptions;

    const result = await populateDataInner(options);
    parentPort.postMessage(result);
    process.exit(0);
  } catch (e) {
    // This is a bug, not a connection issue
    console.error(e);
    // Only closes the Worker thread
    process.exit(2);
  }
}

async function populateDataInner(options: PopulateDataOptions) {
  // Dedicated connection so we can release the memory easily
  const initialDb = await pgwire.connectPgWire(options.connection, { type: 'standard' });
  const largeDescription = crypto.randomBytes(options.size / 2).toString('hex');
  let operation_count = 0;
  for (let i = 0; i < options.num_transactions; i++) {
    const prefix = `test${i}K`;

    await initialDb.query({
      statement: `INSERT INTO test_data(id, description, other) SELECT $1 || i, $2, 'foo' FROM generate_series(1, $3) i`,
      params: [
        { type: 'varchar', value: prefix },
        { type: 'varchar', value: largeDescription },
        { type: 'int4', value: options.per_transaction }
      ]
    });
    operation_count += options.per_transaction;
  }
  await initialDb.end();
  return operation_count;
}

export async function populateData(options: PopulateDataOptions) {
  const WORKER_TIMEOUT = 30_000;

  const worker = new Worker(new URL('./populate_test_data.js', import.meta.url), {
    workerData: options
  });
  const timeout = setTimeout(() => {
    // Exits with code 1 below
    worker.terminate();
  }, WORKER_TIMEOUT);
  try {
    return await new Promise<number>((resolve, reject) => {
      worker.on('message', resolve);
      worker.on('error', reject);
      worker.on('exit', (code) => {
        if (code !== 0) {
          reject(new Error(`Populating data failed with exit code ${code}`));
        }
      });
    });
  } finally {
    clearTimeout(timeout);
  }
}
