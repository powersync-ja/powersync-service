import type { StreamingSyncLine, StreamingSyncRequest } from '@powersync/service-core';
import { deserialize, serialize } from 'bson';
import { RSocketConnector } from 'rsocket-core';
import { WebsocketClientTransport } from 'rsocket-websocket-client';
import type { StreamEvent, SyncOptions } from './stream.js';

export async function* openWebSocketStream(options: SyncOptions): AsyncGenerator<StreamEvent> {
  const client = new RSocketConnector({
    transport: new WebsocketClientTransport({
      url: options.endpoint.replace(/^http/, 'ws'),
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
            token: `Token ${options.token}`
          })
        )
      }
    }
  });

  const rsocket = await client.connect();

  const SYNC_QUEUE_REQUEST_N = 10;

  let pendingEventsCount = SYNC_QUEUE_REQUEST_N;

  const q: StreamEvent[] = [];
  let resolveWaiter: (() => void) | null = null;
  let done = false;
  let error = null;

  const wake = () => {
    if (resolveWaiter) {
      resolveWaiter();
      resolveWaiter = null;
    }
  };
  const streamRequest: StreamingSyncRequest = {
    client_id: options.clientId,
    buckets: [...(options.bucketPositions ?? new Map()).entries()].map(([bucket, after]) => ({
      name: bucket,
      after: after
    }))
  };

  const stream = rsocket.requestStream(
    {
      data: Buffer.from(serialize(streamRequest)),
      metadata: Buffer.from(
        serialize({
          path: '/sync/stream'
        })
      )
    },
    SYNC_QUEUE_REQUEST_N, // The initial N amount
    {
      onError: (e) => {
        error = e ?? new Error('stream error');
        wake();
      },
      onNext: (payload) => {
        const { data } = payload;

        // Less events are now pending
        pendingEventsCount--;
        if (!data) {
          return;
        }

        const chunk = deserialize(data);
        // Note: We don't currently apply any backpressure.
        // Since we process data fairly synchronously in the test-client, it should not make a big difference.
        q.push({ stats: { decodedBytes: data.byteLength } });
        q.push(chunk as StreamingSyncLine);
        wake();

        const required = SYNC_QUEUE_REQUEST_N - pendingEventsCount;
        if (required > 0) {
          stream.request(required);
          pendingEventsCount += required;
        }
      },
      onComplete: () => {
        stream.cancel();
        done = true;
        wake();
      },
      onExtension: () => {}
    }
  );

  if (options.signal?.aborted) {
    stream.cancel();
  }
  options.signal?.addEventListener(
    'abort',
    () => {
      stream.cancel();
    },
    { once: true }
  );

  try {
    while (true) {
      if (error) {
        throw error;
      }
      if (q.length > 0) {
        yield q.shift()!;
        continue;
      }
      if (done) {
        break;
      }
      await new Promise<void>((r) => (resolveWaiter = r));
    }
  } finally {
    stream.cancel();
  }
}
