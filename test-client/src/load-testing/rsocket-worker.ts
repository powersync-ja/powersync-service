import { RSocketConnector } from 'rsocket-core';
import { serialize, deserialize } from 'bson';
import WebSocket from 'ws';
import { WebsocketClientTransport } from 'rsocket-websocket-client';

import { parentPort, workerData } from 'worker_threads';

if (parentPort == null) {
  throw new Error(`Can only run this script in a worker_thread`);
}

const { i, url, token, print } = workerData;

const client = new RSocketConnector({
  transport: new WebsocketClientTransport({
    url,
    wsCreator: (url: string) => {
      return new WebSocket(url) as any;
    }
  }),
  setup: {
    dataMimeType: 'application/bson',
    metadataMimeType: 'application/bson',
    keepAlive: 15_000,
    lifetime: 120_000,
    payload: {
      data: null,
      metadata: Buffer.from(
        serialize({
          token: `Token ${token}`
        })
      )
    }
  }
});

const rsocket = await client.connect();

const SYNC_QUEUE_REQUEST_N = 2;

let pendingEventsCount = SYNC_QUEUE_REQUEST_N;
let size = 0;
let numOperations = 0;
let lastCheckpointStart = 0;
let printData: string[] = [];

const stream = rsocket.requestStream(
  {
    data: Buffer.from(serialize({})),
    metadata: Buffer.from(
      serialize({
        path: '/sync/stream'
      })
    )
  },
  SYNC_QUEUE_REQUEST_N, // The initial N amount
  {
    onError: (e) => {
      console.error(new Date().toISOString(), i, e);
    },
    onNext: (payload) => {
      const { data } = payload;

      // Less events are now pending
      pendingEventsCount--;
      if (!data) {
        return;
      }

      size += data.byteLength;

      const chunk = deserialize(data);

      if (chunk?.checkpoint_complete) {
        const duration = performance.now() - lastCheckpointStart;
        let message = `checkpoint_complete op_id: ${chunk.checkpoint_complete.last_op_id}, ops: ${numOperations}, bytes: ${size}, duration: ${duration.toFixed(0)}ms`;
        if (print) {
          message += `, data: [${printData}]`;
        }
        console.log(new Date().toISOString(), i, message);
        printData = [];
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

      const required = SYNC_QUEUE_REQUEST_N - pendingEventsCount;
      if (required > 0) {
        stream.request(SYNC_QUEUE_REQUEST_N - pendingEventsCount);
        pendingEventsCount = SYNC_QUEUE_REQUEST_N;
      }
    },
    onComplete: () => {
      stream.cancel();
    },
    onExtension: () => {}
  }
);

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
