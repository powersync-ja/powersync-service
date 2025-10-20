import { StreamingSyncLine } from '@powersync/service-core';
import type { openHttpStream } from './httpStream.js';
import * as timers from 'node:timers/promises';

export type SyncOptions = {
  endpoint: string;
  token: string;
  clientId?: string | undefined;
  signal?: AbortSignal | undefined;
  once?: boolean | undefined;
  bucketPositions?: Map<string, string> | undefined;
  mode?: 'http' | 'websocket';
};

export type StreamEvent = StreamingSyncLine | { error: any } | { stats: { decodedBytes: number } };

export async function* openStream(options: SyncOptions) {
  let bucketPositions = new Map<string, string>(options.bucketPositions);
  while (!options.signal?.aborted) {
    try {
      const stream = openStreamIteration({ ...options, bucketPositions });

      for await (let chunk of stream) {
        if ('data' in chunk) {
          bucketPositions.set(chunk.data.bucket, chunk.data.next_after);
        }
        yield chunk;
      }
      // Connection ended, reconnecting
    } catch (e) {
      yield { error: e };
      // Connection error, reconnecting
      await timers.setTimeout(1000 + Math.random() * 1000);
    }
  }
}

export async function* openStreamIteration(options: SyncOptions): AsyncGenerator<StreamEvent> {
  let impl: typeof openHttpStream;
  if (options.mode === 'websocket') {
    impl = (await import('./rsocketStream.js')).openWebSocketStream;
  } else {
    impl = (await import('./httpStream.js')).openHttpStream;
  }
  yield* impl(options);
}
