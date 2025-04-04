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

export async function streamForever(i: number, credentials: Credentials, mode: Mode) {
  while (true) {
    try {
      await stream(i, credentials, mode);
      console.log(new Date().toISOString(), i, 'Stream ended');
    } catch (e) {
      console.error(new Date().toISOString(), i, e.message);
      await new Promise((resolve) => setTimeout(resolve, 1000 + Math.random()));
    }
  }
}

export async function concurrentConnections(credentials: Credentials, numClients: number, mode: Mode) {
  for (let i = 0; i < numClients; i++) {
    streamForever(i, credentials, mode);
  }
}
