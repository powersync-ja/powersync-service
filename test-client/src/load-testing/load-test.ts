import { Worker } from 'worker_threads';
import { SyncOptions } from '../stream.js';

export type Mode = 'http' | 'websocket';

export async function stream(i: number, request: SyncOptions, print: string | undefined) {
  const worker = new Worker(new URL('./load-test-worker.js', import.meta.url), {
    workerData: { i, request, print }
  });
  await new Promise((resolve, reject) => {
    worker.on('message', (event) => resolve(event));
    worker.on('error', (err) => reject(err));
    worker.on('exit', (__code) => {
      resolve(null);
    });
  });
  worker.terminate();
}

export async function concurrentConnections(options: SyncOptions, numClients: number, print: string | undefined) {
  for (let i = 0; i < numClients; i++) {
    stream(i, { ...options, clientId: options.clientId ?? `load-test-${i}` }, print);
  }
}
