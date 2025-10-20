import type { StreamingSyncLine, StreamingSyncRequest } from '@powersync/service-core';
import { Readable } from 'node:stream';
import { request } from 'undici';
import { ndjsonStream } from './ndjson.js';
import { StreamEvent, SyncOptions } from './stream.js';

export async function* openHttpStream(options: SyncOptions): AsyncGenerator<StreamEvent> {
  const streamRequest: StreamingSyncRequest = {
    raw_data: true,
    client_id: options.clientId,
    buckets: [...(options.bucketPositions ?? new Map()).entries()].map(([bucket, after]) => ({
      name: bucket,
      after: after
    }))
  };
  const response = await request(options.endpoint + '/sync/stream', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Token ${options.token}`
    },
    body: JSON.stringify(streamRequest),
    signal: options.signal
  });

  if (response.statusCode != 200) {
    throw new Error(`Request failed with code: ${response.statusCode}\n${await response.body.text()}`);
  }

  const stream = Readable.toWeb(response.body) as ReadableStream<Uint8Array>;

  for await (let { chunk, size } of ndjsonStream<StreamingSyncLine>(stream)) {
    yield { stats: { decodedBytes: size } };
    yield chunk;
  }
  // If we reach this, the connection was closed without error by the server
}
