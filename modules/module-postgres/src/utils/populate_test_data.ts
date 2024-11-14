import { Worker } from 'node:worker_threads';

import * as pgwire from '@powersync/service-jpgwire';

// This util is actually for tests only, but we need it compiled to JS for the service to work, so it's placed in the service.

export interface PopulateDataOptions {
  connection: pgwire.NormalizedConnectionConfig;
  num_transactions: number;
  per_transaction: number;
  size: number;
}

export async function populateData(options: PopulateDataOptions) {
  const WORKER_TIMEOUT = 30_000;

  const worker = new Worker(new URL('./populate_test_data_worker.js', import.meta.url), {
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
