import { parentPort, workerData } from 'worker_threads';
import { openStream, SyncOptions } from '../stream.js';

if (parentPort == null) {
  throw new Error(`Can only run this script in a worker_thread`);
}

const { i, print } = workerData;
const request: SyncOptions = workerData.request;

let size = 0;
let numOperations = 0;
let lastCheckpointStart = 0;
let printData: string[] = [];

const parseChunk = (chunk: any) => {
  if (print == null) {
    return;
  }
  chunk.data.forEach((data: any) => {
    if (data.op == 'PUT') {
      const payload = JSON.parse(data.data);
      printData.push(payload[print]);
    }
  });
};

for await (let chunk of openStream(request)) {
  if ('error' in chunk) {
    // Retried automatically
    console.error(new Date().toISOString(), i, `Error in stream: ${chunk.error}`);
  } else if ('checkpoint_complete' in chunk) {
    const duration = performance.now() - lastCheckpointStart;
    let message = `checkpoint_complete op_id: ${chunk.checkpoint_complete.last_op_id}, ops: ${numOperations}, bytes: ${size}, duration: ${duration.toFixed(0)}ms`;
    if (print) {
      message += `, data: [${printData}]`;
    }
    printData = [];
    console.log(new Date().toISOString(), i, message);

    if (request.once) {
      break;
    }
  } else if ('data' in chunk) {
    parseChunk(chunk.data);
    numOperations += chunk.data.data.length;
  } else if ('checkpoint' in chunk) {
    lastCheckpointStart = performance.now();
    console.log(new Date().toISOString(), i, `checkpoint buckets: ${chunk.checkpoint.buckets.length}`);
  } else if ('checkpoint_diff' in chunk) {
    lastCheckpointStart = performance.now();
    console.log(
      new Date().toISOString(),
      i,
      `checkpoint_diff removed_buckets: ${chunk.checkpoint_diff.removed_buckets.length} updated_buckets: ${chunk.checkpoint_diff.updated_buckets.length}`
    );
  } else if ('stats' in chunk) {
    size += chunk.stats.decodedBytes;
  } else {
    const key = Object.keys(chunk)[0];
    if (key != 'token_expires_in' && key != 'data') {
      console.log(new Date().toISOString(), i, key);
    }
  }
}
