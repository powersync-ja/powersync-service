import { ndjsonStream } from '../ndjson.js';

import { parentPort, workerData } from 'worker_threads';

if (parentPort == null) {
  throw new Error(`Can only run this script in a worker_thread`);
}

const { i, url, token } = workerData;

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

let size = 0;

for await (let chunk of ndjsonStream<any>(response.body)) {
  size += JSON.stringify(chunk).length;
  if (chunk?.checkpoint_complete) {
    console.log(new Date().toISOString(), i, 'checkpoint', chunk.checkpoint_complete.last_op_id, size);
  } else {
    const key = Object.keys(chunk)[0];
    if (key != 'token_expires_in') {
      console.log(new Date().toISOString(), i, key);
    }
  }
}

parentPort.postMessage({ done: true });
