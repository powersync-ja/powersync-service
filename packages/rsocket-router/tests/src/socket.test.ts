import '@journeyapps-platform/micro/register';
import * as WebSocket from 'ws';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { RSocketConnector, RSocketServer } from 'rsocket-core';
import { WebsocketClientTransport } from 'rsocket-websocket-client';

import { WebsocketServerTransport } from '../../src/router/transport/WebSocketServerTransport.js';
import { WebsocketDuplexConnection } from '../../src/router/transport/WebsocketDuplexConnection.js';
import { Duplex } from 'stream';

const WS_PORT = process.env.WS_PORT ? parseInt(process.env.WS_PORT) : 4532;
const WS_ADDRESS = `ws://localhost:${WS_PORT}`;

describe('Sockets', () => {
  let server: WebSocket.WebSocketServer;
  let closeServer: () => void;

  beforeEach(() => {
    let closed = false;
    server = new WebSocket.WebSocketServer({
      port: WS_PORT
    });

    /**
     * The server doesn't have a closed status.
     * Some tests involve closing while others can be closed
     * after each test. This method should prevent double closing.
     */
    closeServer = () => {
      if (closed) {
        return;
      }
      server.close();
      closed = true;
    };
  });

  afterEach(() => {
    closeServer();
  });

  it('should only not close a server that is managed externally', async () => {
    const transport = new WebsocketServerTransport({
      wsCreator: () => server
    });

    const rSocketServer = new RSocketServer({
      transport,
      acceptor: {
        accept: async () => {
          return {};
        }
      }
    });

    const closer = await rSocketServer.bind();

    const closeableSpy = vi.spyOn(closer, 'close');

    // Register a listener for when the RSocketServer has been closed
    const isClosedPromise = new Promise<void>((resolve) => {
      closer.onClose(() => resolve());
    });

    // This will be triggered externally when the HTTP(s) server closes
    // linked to the internal WS server.
    closeServer();
    await isClosedPromise;
    expect(closeableSpy).toBeCalledTimes(1);
  });

  /**
   * Anyone can connect to the WebSocket port and send any data. Frame decoding should handle
   * invalid WebSocket data events.
   */
  it('should handle incorrect initial frames', async () => {
    const transport = new WebsocketServerTransport({
      wsCreator: () => server
    });

    const rSocketServer = new RSocketServer({
      transport,
      acceptor: {
        accept: async () => {
          return {};
        }
      }
    });

    await rSocketServer.bind();

    const duplexSpy = vi.spyOn(WebsocketDuplexConnection, 'create');

    // Connect a client WebSocket to the server
    const client = new WebSocket.WebSocket(WS_ADDRESS);
    await new Promise<void>((resolve) => {
      client.once('open', () => resolve());
    });

    /**
     * The connection should be closed if the client sends random data instead
     * of a valid frame
     */
    client.send('random text');

    // it should try to create a duplex socket, but fail
    await vi.waitFor(() => expect(duplexSpy.mock.calls.length).equals(1), { timeout: 3000 });

    // It should perform cleanup. Sockets should be closed
    const duplex: Duplex = duplexSpy.mock.calls[0][0];
    const rawSocket: WebSocket.WebSocket = duplexSpy.mock.calls[0][3];
    await vi.waitFor(() => expect(duplex.closed).equals(true), { timeout: 3000 });
    await vi.waitFor(() => expect(rawSocket.readyState).equals(rawSocket.CLOSED), { timeout: 3000 });
  });

  /**
   * The server should handle cases where the client closes the WebSocket connection
   * at any point in the handshaking process. This test will create 100 connections which
   * have their socket closed as soon as the connection has started. The standard RSocket
   * WebSocket transport and Duplex connection will throw unhandled exceptions in this case.
   * This package's custom implementations should handle exceptions correctly.
   */
  it('should handle closed client connections correctly', async () => {
    const transport = new WebsocketServerTransport({
      wsCreator: () => server
    });

    // Create a simple server which will spam a lot of data to any connection
    const rSocketServer = new RSocketServer({
      transport,
      acceptor: {
        accept: async () => {
          return {
            requestStream: (payload, initialN, responder) => {
              let stop = false;

              setImmediate(async () => {
                while (!stop) {
                  responder.onNext({ data: Buffer.from('some payload') }, false);
                  await new Promise((r) => setTimeout(r, 10));
                }
              });
              return {
                request: () => {},
                onExtension: () => {},
                cancel: () => {
                  stop = true;
                }
              };
            }
          };
        }
      }
    });
    rSocketServer.bind();

    // Try and connect 100 times, closing the socket as soon as it is available
    for (let i = 0; i < 100; i++) {
      const testSocket = new WebSocket.WebSocket(WS_ADDRESS);

      const connector = new RSocketConnector({
        transport: new WebsocketClientTransport({
          url: WS_ADDRESS,
          wsCreator: (url) => testSocket
        }),

        setup: {
          dataMimeType: 'application/bson',
          metadataMimeType: 'application/bson',
          payload: {
            data: null
          }
        }
      });

      const connection = await connector.connect();
      connection.requestStream({ data: null }, 1, {
        onNext() {},
        onComplete: () => {},
        onExtension: () => {},
        onError: () => {}
      });

      // The socket closing here should not throw any unhandled errors
      testSocket.close();
    }
  });
});
