import { randomUUID } from 'node:crypto';
import { createNdjsonStreamReader, drainNdjsonUntilCheckpoint, NdjsonStreamReader } from '../utils/ndjson-drain.js';
import { ApiClientCommand, ApiClientConfiguration, ApiClientResponse } from './api-client-protocol.js';

let configuration: ApiClientConfiguration | undefined;
let reader: NdjsonStreamReader | undefined;
let queue = Promise.resolve();
const abortController = new AbortController();

process.on('message', (command: ApiClientCommand) => {
  queue = queue.then(async () => {
    try {
      switch (command.kind) {
        case 'initialize':
          if (configuration != null) throw new Error('API client is already initialized');
          configuration = command.configuration;
          send({ id: command.id });
          break;
        case 'open': {
          if (configuration == null) throw new Error('API client is not initialized');
          if (reader != null) throw new Error('API client connection is already open');
          const response = await fetch(`${configuration.endpoint}/sync/stream`, {
            method: 'POST',
            headers: {
              authorization: `Bearer ${configuration.token}`,
              'content-type': 'application/json',
              'accept-encoding': 'identity'
            },
            body: JSON.stringify({
              raw_data: true,
              client_id: randomUUID(),
              buckets: [],
              parameters: configuration.parameters,
              streams: configuration.streams
            }),
            signal: abortController.signal
          });
          reader = createNdjsonStreamReader(response, configuration.markerIds);
          send({ id: command.id, observation: await reader.nextCheckpoint() });
          break;
        }
        case 'drain':
          if (reader == null) throw new Error('API client connection is not open');
          send({ id: command.id, observation: await drainNdjsonUntilCheckpoint(reader, command.checkpoint) });
          break;
        case 'close':
          await reader?.close();
          process.send!({ id: command.id }, () => process.exit(0));
          break;
      }
    } catch (error) {
      send({ id: command.id, error: { message: String(error.message ?? error), stack: error.stack } });
    }
  });
});

// Cancel pending fetches and reads if the benchmark worker disappears.
process.on('disconnect', () => {
  abortController.abort();
  process.exit(1);
});

function send(response: ApiClientResponse): void {
  process.send!(response);
}
