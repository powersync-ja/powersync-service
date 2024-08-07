import { ndjsonStream } from './ndjson.js';
import type * as types from '@powersync/service-core';
import { isCheckpoint, isCheckpointComplete, isStreamingSyncData, normalizeData } from './util.js';

export interface GetCheckpointOptions {
  endpoint: string;
  token: string;
  raw?: boolean;
}

export async function getCheckpointData(options: GetCheckpointOptions) {
  const response = await fetch(`${options.endpoint}/sync/stream`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Token ${options.token}`
    },
    body: JSON.stringify({
      raw_data: true,
      include_checksum: true,
      parameters: {}
    } satisfies types.StreamingSyncRequest)
  });
  if (!response.ok) {
    throw new Error(response.statusText + '\n' + (await response.text()));
  }

  let data: types.StreamingSyncData[] = [];
  let checkpoint: types.StreamingSyncCheckpoint;

  for await (let chunk of ndjsonStream<types.StreamingSyncLine>(response.body!)) {
    if (isStreamingSyncData(chunk)) {
      data.push(chunk);
    } else if (isCheckpoint(chunk)) {
      checkpoint = chunk;
    } else if (isCheckpointComplete(chunk)) {
      break;
    }
  }

  return normalizeData(checkpoint!, data, { raw: options.raw ?? false });
}
