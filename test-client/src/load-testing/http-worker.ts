import { ndjsonStream } from '../ndjson.js';

import { parentPort, workerData } from 'worker_threads';

if (parentPort == null) {
  throw new Error(`Can only run this script in a worker_thread`);
}

const { i, url, token, print } = workerData;

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

const response = await fetch(url + '/sync/stream', {
  method: 'POST',
  headers: {
    'Content-Type': 'application/json',
    Authorization: `Token ${token}`
  },
  body: JSON.stringify({
    raw_data: true,
    include_checksums: true
  })
});

if (!response.ok || response.body == null) {
  throw new Error(response.statusText + '\n' + (await response.text()));
}

for await (let chunk of ndjsonStream<any>(response.body)) {
  size += JSON.stringify(chunk).length;
  if (chunk?.checkpoint_complete) {
    const duration = performance.now() - lastCheckpointStart;
    let message = `checkpoint_complete op_id: ${chunk.checkpoint_complete.last_op_id}, ops: ${numOperations}, bytes: ${size}, duration: ${duration.toFixed(0)}ms`;
    if (print) {
      message += `, data: [${printData}]`;
    }
    printData = [];
    console.log(new Date().toISOString(), i, message);
  } else if (chunk?.data) {
    parseChunk(chunk.data);
    numOperations += chunk.data.data.length;
  } else if (chunk?.checkpoint) {
    lastCheckpointStart = performance.now();
    console.log(new Date().toISOString(), i, `checkpoint buckets: ${chunk.checkpoint.buckets.length}`);
  } else if (chunk?.checkpoint_diff) {
    lastCheckpointStart = performance.now();
    console.log(
      new Date().toISOString(),
      i,
      `checkpoint_diff removed_buckets: ${chunk.checkpoint_diff.removed_buckets.length} updated_buckets: ${chunk.checkpoint_diff.updated_buckets.length}`
    );
  } else {
    const key = Object.keys(chunk)[0];
    if (key != 'token_expires_in' && key != 'data') {
      console.log(new Date().toISOString(), i, key);
    }
  }
}

parentPort.postMessage({ done: true });
