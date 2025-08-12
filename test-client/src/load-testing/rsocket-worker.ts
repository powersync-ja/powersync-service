import { RSocketConnector } from 'rsocket-core';
import { serialize, deserialize } from 'bson';
import WebSocket from 'ws';
import { WebsocketClientTransport } from 'rsocket-websocket-client';

import { parentPort, workerData } from 'worker_threads';

if (parentPort == null) {
  throw new Error(`Can only run this script in a worker_thread`);
}

const { i, url, token } = workerData;

const client = new RSocketConnector({
  transport: new WebsocketClientTransport({
    url,
    wsCreator: (url) => {
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
        console.log(new Date().toISOString(), i, 'checkpoint', chunk.checkpoint_complete.last_op_id, size);
      } else {
        console.log(new Date().toISOString(), i, Object.keys(chunk)[0]);
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
