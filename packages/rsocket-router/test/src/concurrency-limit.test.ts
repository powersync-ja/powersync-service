import { ErrorCode, logger } from '@powersync/lib-services-framework';
import { deserialize, serialize } from 'bson';
import * as http from 'http';
import { RSocketConnector } from 'rsocket-core';
import { WebsocketClientTransport } from 'rsocket-websocket-client';
import { afterEach, describe, expect, it, vi } from 'vitest';
import * as WebSocket from 'ws';
import { ReactiveSocketRouter, SocketBaseContext } from '../../src/router/ReactiveSocketRouter.js';

// Port range distinct from socket.test.ts (5433+) to avoid clashes between parallel workers
let nextPort = 5600;

describe('Concurrency limit', () => {
  let cleanup: (() => Promise<void> | void)[] = [];

  afterEach(async () => {
    for (const fn of cleanup.reverse()) {
      await fn();
    }
    cleanup = [];
  });

  function createConnector(address: string) {
    return new RSocketConnector({
      transport: new WebsocketClientTransport({
        url: address,
        wsCreator: (url) => new WebSocket.WebSocket(url) as any
      }),
      setup: {
        dataMimeType: 'application/bson',
        metadataMimeType: 'application/bson',
        payload: {
          data: null,
          metadata: Buffer.from(serialize({ token: 'test-token' }))
        }
      }
    });
  }

  it('rejects connections over max_concurrent_connections and reports each rejection', async () => {
    const port = nextPort++;
    const address = `ws://localhost:${port}`;

    const httpServer = http.createServer();
    await new Promise<void>((resolve) => httpServer.listen(port, resolve));
    cleanup.push(() => new Promise<void>((resolve) => httpServer.close(() => resolve())));

    const onLimitRejected = vi.fn();
    const router = new ReactiveSocketRouter<SocketBaseContext>({
      max_concurrent_connections: 1,
      on_concurrency_limit_rejected: onLimitRejected
    });

    router.applyWebSocketEndpoints(httpServer, {
      contextProvider: async () => ({ logger }),
      endpoints: [],
      metaDecoder: async (meta) => deserialize(meta.contents) as any,
      payloadDecoder: async (rawData) => rawData && deserialize(rawData.contents)
    });

    const firstClient = await createConnector(address).connect();
    cleanup.push(() => firstClient.close());
    expect(onLimitRejected).not.toHaveBeenCalled();

    // SETUP rejection reaches onClose, not connect().
    const secondClient = await createConnector(address).connect();
    const closeError = await new Promise<Error | undefined>((resolve) => secondClient.onClose(resolve));
    expect(closeError?.message).toMatch(/Maximum active concurrent connections limit has been reached/);
    expect(onLimitRejected).toHaveBeenCalledTimes(1);
    expect(onLimitRejected.mock.calls[0][0].errorData.code).toBe(ErrorCode.PSYNC_S2304);
  });
});
