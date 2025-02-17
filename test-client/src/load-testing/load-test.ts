import { Worker } from 'worker_threads';
import { Credentials } from '../auth.js';

export type Mode = 'http' | 'websocket';

export async function stream(i: number, credentials: Credentials, mode: Mode) {
  const worker =
    mode == 'websocket'
      ? new Worker(new URL('./rsocket-worker.js', import.meta.url), {
          workerData: { i, token: credentials.token, url: credentials.endpoint.replace(/^http/, 'ws') }
        })
      : new Worker(new URL('./http-worker.js', import.meta.url), {
          workerData: { i, token: credentials.token, url: credentials.endpoint }
        });
  await new Promise((resolve, reject) => {
    worker.on('message', (event) => resolve(event));
    worker.on('error', (err) => reject(err));
  });
  worker.terminate();
}

export async function concurrentConnections(credentials: Credentials, numClients: number, mode: Mode) {
  for (let i = 0; i < numClients; i++) {
    stream(i, credentials, mode);
  }
}
