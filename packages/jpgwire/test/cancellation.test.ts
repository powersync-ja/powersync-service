import { once } from 'node:events';
import net from 'node:net';
import { afterEach, expect, test, vi } from 'vitest';
import { pgconnection } from '../src/pgwire.js';

afterEach(() => vi.useRealTimers());

test('waits for cancellation EOF before reusing a connection whose query finished naturally', async () => {
  await using backend = await openBackend();
  const connection = pgconnection(backend.options);
  try {
    const abort = new AbortController();
    let settled = false;
    const query = connection.query('SELECT first', { signal: abort.signal }).catch((error) => error);
    void query.then(() => (settled = true));
    await backend.firstQuery.promise;
    abort.abort();
    const cancel = await backend.cancelRequest.promise;
    using completed = vi.spyOn(connection as any, '_recvCommandComplete');

    // The query completes naturally before the cancellation is processed.
    // Keep the cancellation connection open to make the ordering deterministic.
    backend.finishFirstQuery();
    await vi.waitFor(() => expect(completed).toHaveBeenCalled());
    expect(settled).toBe(false);

    const nextQuery = connection.query('SELECT second');
    expect(backend.queries).toEqual(['SELECT first']);
    cancel.end();
    expect(await query).toBeInstanceOf(Error);
    await nextQuery;
    expect(backend.queries).toEqual(['SELECT first', 'SELECT second']);
  } finally {
    connection.destroy();
  }
});

test('discards the original connection when cancellation EOF times out', async () => {
  await using backend = await openBackend();
  const connection = pgconnection(backend.options);
  try {
    await connection.query();
    // Fake only the cancellation deadline; TCP I/O still uses the real event loop.
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] });
    const cancellation = (connection as any)._cancelRequest() as Promise<void>;
    const failure = expect(cancellation).rejects.toThrow('postgres cancellation timed out');
    await backend.cancelRequest.promise;
    await vi.advanceTimersByTimeAsync(10_000);
    await failure;
    await expect(connection.query('SELECT second')).rejects.toThrow('postgres query late');
    expect(backend.queries).toEqual([]);
  } finally {
    connection.destroy();
  }
});

test('discards the original connection when the cancellation socket fails', async () => {
  await using backend = await openBackend();
  const connection = pgconnection(backend.options);
  try {
    await connection.query();
    const cancellation = (connection as any)._cancelRequest() as Promise<void>;
    const failure = expect(cancellation).rejects.toThrow();
    const cancel = await backend.cancelRequest.promise;
    cancel.resetAndDestroy();
    await failure;
    await expect(connection.query('SELECT second')).rejects.toThrow('postgres query late');
  } finally {
    connection.destroy();
  }
});

/** A scripted backend makes query completion and cancellation processing independently controllable. */
async function openBackend() {
  const firstQuery = Promise.withResolvers<void>();
  const cancelRequest = Promise.withResolvers<net.Socket>();
  const queries: string[] = [];
  const sockets = new Set<net.Socket>();
  let mainSocket: net.Socket;
  const server = net.createServer((socket) => {
    sockets.add(socket);
    socket.on('error', () => {});
    socket.on('close', () => sockets.delete(socket));
    let pending = Buffer.alloc(0);
    let startup = true;
    socket.on('data', (data) => {
      pending = Buffer.concat([pending, data]);
      while (pending.length >= (startup ? 4 : 5)) {
        const length = pending.readInt32BE(startup ? 0 : 1) + (startup ? 0 : 1);
        if (pending.length < length) return;
        const packet = pending.subarray(0, length);
        pending = pending.subarray(length);
        if (startup) {
          if (packet.readInt32BE(4) === 80877102) {
            cancelRequest.resolve(socket);
            return;
          }
          startup = false;
          mainSocket = socket;
          socket.write(Buffer.concat([message('R', Buffer.alloc(4)), message('K', Buffer.alloc(8)), ready()]));
        } else if (packet[0] === 83 /* Sync */) {
          socket.write(ready());
        } else if (packet[0] === 81 /* Query */) {
          const sql = packet.subarray(5, -1).toString();
          if (sql === '') {
            socket.write(Buffer.concat([message('I'), ready()]));
          } else {
            queries.push(sql);
            if (sql === 'SELECT first') firstQuery.resolve();
            else finishQuery(socket);
          }
        }
      }
    });
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const address = server.address() as net.AddressInfo;
  return {
    options: new URL(`postgres://test@${address.address}:${address.port}/test?sslmode=disable`),
    firstQuery,
    cancelRequest,
    queries,
    finishFirstQuery: () => finishQuery(mainSocket),
    async [Symbol.asyncDispose]() {
      const closed = once(server, 'close');
      server.close();
      for (const socket of sockets) socket.destroy();
      await closed;
    }
  };
}

function message(tag: string, payload = Buffer.alloc(0)) {
  const header = Buffer.alloc(5);
  header[0] = tag.charCodeAt(0);
  header.writeInt32BE(payload.length + 4, 1);
  return Buffer.concat([header, payload]);
}

function ready() {
  return message('Z', Buffer.from('I'));
}

function finishQuery(socket: net.Socket) {
  socket.write(Buffer.concat([message('C', Buffer.from('SELECT 0\0')), ready()]));
}
