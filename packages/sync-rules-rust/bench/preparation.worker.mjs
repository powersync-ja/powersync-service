// Adapted transport from optimize-replication's MongoRowPreparation.worker.ts:
// cloned BSON input, serialized data output and source indexes instead of class instances.
import { parentPort, workerData } from 'node:worker_threads';
import { compile, jsImplementation } from './evaluation.mjs';
import { completePreparation } from './worker-pool.mjs';

const config = compile(workerData.queries, workerData.sqlite);
const evaluate = jsImplementation(config, workerData.table, true);
parentPort.on('message', (inputs) => {
  try {
    const buffers = inputs.map(({ raw }) => Buffer.from(raw.buffer, raw.byteOffset, raw.byteLength));
    let rows = evaluate(buffers);
    if (workerData.full) rows = completePreparation(rows, buffers, true);
    parentPort.postMessage({ rows });
  } catch (error) {
    parentPort.postMessage({ error: error.stack ?? String(error) });
  }
});
parentPort.postMessage({ ready: true });
