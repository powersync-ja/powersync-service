/*
 * Adapted from https://github.com/rsocket/rsocket-js/blob/1.0.x-alpha/packages/rsocket-websocket-client/src/WebsocketClientTransport.ts
 * Copyright 2021-2022 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import {
  Closeable,
  Deferred,
  Demultiplexer,
  DuplexConnection,
  Frame,
  FrameHandler,
  Multiplexer,
  Outbound,
  ServerTransport
} from 'rsocket-core';
import * as WebSocket from 'ws';
import { WebsocketDuplexConnection } from './WebsocketDuplexConnection.js';
import * as micro from '@journeyapps-platform/micro';

export type SocketFactory = (options: SocketOptions) => WebSocket.WebSocketServer;

export type SocketOptions = {
  host?: string;
  port?: number;
};

export type ServerOptions = SocketOptions & {
  wsCreator?: SocketFactory;
  debug?: boolean;
};

const defaultFactory: SocketFactory = (options: SocketOptions) => {
  return new WebSocket.WebSocketServer({
    host: options.host,
    port: options.port
  });
};

export class WebsocketServerTransport implements ServerTransport {
  private readonly host: string | undefined;
  private readonly port: number | undefined;
  private readonly factory: SocketFactory;

  constructor(options: ServerOptions) {
    this.host = options.host;
    this.port = options.port;
    this.factory = options.wsCreator ?? defaultFactory;
  }

  async bind(
    connectionAcceptor: (frame: Frame, connection: DuplexConnection) => Promise<void>,
    multiplexerDemultiplexerFactory: (
      frame: Frame,
      outbound: Outbound & Closeable
    ) => Multiplexer & Demultiplexer & FrameHandler
  ): Promise<Closeable> {
    const websocketServer: WebSocket.WebSocketServer = await this.connectServer();
    const serverCloseable = new ServerCloseable(websocketServer);

    const connectionListener = (websocket: WebSocket.WebSocket) => {
      try {
        websocket.binaryType = 'nodebuffer';
        const duplex = WebSocket.createWebSocketStream(websocket);
        WebsocketDuplexConnection.create(duplex, connectionAcceptor, multiplexerDemultiplexerFactory, websocket);
      } catch (ex) {
        micro.logger.error(`Could not create duplex connection`, ex);
        if (websocket.readyState == websocket.OPEN) {
          websocket.close();
        }
      }
    };

    const closeListener = (error?: Error) => {
      serverCloseable.close(error);
    };

    websocketServer.addListener('connection', connectionListener);
    websocketServer.addListener('close', closeListener);
    websocketServer.addListener('error', closeListener);

    return serverCloseable;
  }

  private connectServer(): Promise<WebSocket.WebSocketServer> {
    return new Promise((resolve, reject) => {
      const websocketServer = this.factory({
        host: this.host,
        port: this.port
      });

      const earlyCloseListener = (error?: Error) => {
        reject(error);
      };

      websocketServer.addListener('close', earlyCloseListener);
      websocketServer.addListener('error', earlyCloseListener);
      websocketServer.addListener('listening', () => resolve(websocketServer));
    });
  }
}

class ServerCloseable extends Deferred {
  constructor(private readonly server: WebSocket.WebSocketServer) {
    super();
  }

  close(error?: Error) {
    if (this.done) {
      super.close(error);
      return;
    }

    // For this package's use case the server is externally closed

    super.close();
  }
}
