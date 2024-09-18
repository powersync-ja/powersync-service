import * as crypto from 'crypto';
import { isMainThread, parentPort, workerData } from 'node:worker_threads';

import * as pgwire from '@powersync/service-jpgwire';
import type { PopulateDataOptions } from './populate_test_data.js';

// This util is actually for tests only, but we need it compiled to JS for the service to work, so it's placed in the service.

if (isMainThread || parentPort == null) {
  // Must not be imported - only expected to run in a worker
  throw new Error('Do not import this file');
} else {
  try {
    const options = workerData as PopulateDataOptions;
    if (options == null) {
      throw new Error('loaded worker without options');
    }

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
