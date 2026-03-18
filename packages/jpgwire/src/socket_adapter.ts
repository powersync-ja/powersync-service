// Fork of pgwire/index.js, customized to handle additional TLS options

import { once } from 'node:events';
import net from 'node:net';
import tls from 'node:tls';
import { recordBytesRead } from './metrics.js';

// pgwire doesn't natively support configuring timeouts, but we just hardcode a default.
// Timeout idle connections after 6 minutes (we ping at least every 5 minutes).
const POWERSYNC_SOCKET_DEFAULT_TIMEOUT = 360_000;

// Timeout for the initial connection (pre-TLS)
// Must be less than the timeout for a HTTP request
const POWERSYNC_SOCKET_CONNECT_TIMEOUT = 20_000;

// TCP keepalive delay in milliseconds.
// This can help detect dead connections earlier.
const POWERSYNC_SOCKET_KEEPALIVE_INITIAL_DELAY = 40_000;

export interface ConnectOptions {
  host: string;
  port: number;
  tlsOptions?: tls.ConnectionOptions | false;
  lookup?: net.LookupFunction;

  /**
   * Connection timeout in milliseconds.
   * If set, overrides the default POWERSYNC_SOCKET_CONNECT_TIMEOUT.
   */
  connect_timeout_ms?: number;
}

export class SocketAdapter {
  static async connect(options: ConnectOptions) {
    // Custom timeout handling
    const socket = net.connect({
      host: options.host,
      port: options.port,
      lookup: options.lookup,

      // This closes the connection if no data was sent or received for the given time,
      // even if the connection is still actaully alive.
      timeout: POWERSYNC_SOCKET_DEFAULT_TIMEOUT,

      // This configures TCP keepalive.
      keepAlive: true,
      keepAliveInitialDelay: POWERSYNC_SOCKET_KEEPALIVE_INITIAL_DELAY
      // Unfortunately it is not possible to set tcp_keepalive_intvl or
      // tcp_keepalive_probes here.
    });
    try {
      const connectTimeout = options.connect_timeout_ms ?? POWERSYNC_SOCKET_CONNECT_TIMEOUT;
      const timeout = setTimeout(() => {
        socket.destroy(new Error(`Timeout while connecting to ${options.host}:${options.port}`));
      }, connectTimeout);
      await once(socket, 'connect');
      clearTimeout(timeout);
      return new SocketAdapter(socket, options);
    } catch (e) {
      socket.destroy();
      throw e;
    }
    // END POWERSYNC
  }

  _socket: net.Socket;
  _error: Error | null;

  constructor(
    socket: net.Socket,
    private options: ConnectOptions
  ) {
    this._error = null;
    this._socket = socket;
    this._socket.on('readable', (_) => this._readResume());
    this._socket.on('end', () => this._readResume());
    // Custom timeout handling
    this._socket.on('timeout', () => {
      this._socket.destroy(new Error('Socket idle timeout'));
    });
    this._socket.on('error', (error) => {
      this._error = error;
      this._readResume();
      this._writeResume();
    });
  }

  _readResume = () => {
    // noop
    return;
  };
  _writeResume = () => {
    // noop
    return;
  };

  _readPauseAsync = (resolve: () => void) => {
    this._readResume = resolve;
  };
  _writePauseAsync = (resolve: () => void) => {
    this._writeResume = resolve;
  };

  setTimeout(timeout: number) {
    this._socket.setTimeout(timeout);
  }

  async startTls(host: string, ca: any) {
    // START POWERSYNC CUSTOM OPTIONS HANDLING
    const tlsOptions = this.options.tlsOptions;

    // https://nodejs.org/docs/latest-v14.x/api/tls.html#tls_tls_connect_options_callback
    const socket = this._socket;
    const tlsSocket = tls.connect({ socket, host, ...tlsOptions });
    // END POWERSYNC CUSTOM OPTIONS HANDLING
    await once(tlsSocket, 'secureConnect');
    // TODO check tlsSocket.authorized

    // if secure connection succeeded then we take underlying socket ownership,
    // otherwise underlying socket should be closed outside.
    tlsSocket.on('close', (_) => socket.destroy());
    return new SocketAdapter(tlsSocket, this.options);
  }

  async read(out: Uint8Array) {
    let buf;
    for (;;) {
      if (this._error) throw this._error; // TODO callstack
      if (this._socket.readableEnded) return null;
      // POWERSYNC FIX: Read only as much data as available, instead of reading everything and
      // unshifting back onto the socket
      const toRead = Math.min(out.length, this._socket.readableLength);
      buf = this._socket.read(toRead);

      if (buf?.length) break;
      if (!buf) await new Promise<void>(this._readPauseAsync);
    }

    if (buf.length > out.length) {
      throw new Error('Read more data than expected');
    }
    out.set(buf);
    // POWERSYNC: Add metrics
    recordBytesRead(buf.length);
    return buf.length;
  }
  async write(data: Uint8Array) {
    // TODO assert Uint8Array
    // TODO need to copy data?
    if (this._error) throw this._error; // TODO callstack
    const p = new Promise<void>(this._writePauseAsync);
    this._socket.write(data, this._writeResume);
    await p;
    if (this._error) throw this._error; // TODO callstack
    return data.length;
  }
  // async closeWrite() {
  //   if (this._error) throw this._error; // TODO callstack
  //   const socket_end = promisify(cb => this._socket.end(cb));
  //   await socket_end();
  // }
  close() {
    this._socket.destroy(Error('socket destroyed'));
  }
}
