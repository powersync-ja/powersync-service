// Fork of pgwire/index.js, customized to handle additional TLS options
// Based on the version in commit 9532a395d6fa03a59c2231f0ec690806c90bd338
// Modifications marked with `START POWERSYNC ...`

import net from 'net';
import tls from 'tls';
import { createHash, pbkdf2 as _pbkdf2, randomFillSync } from 'crypto';
import { once } from 'events';
import { promisify } from 'util';
import { _net, SaslScramSha256 } from 'pgwire/mod.js';
import { recordBytesRead } from './metrics.js';

// START POWERSYNC
// pgwire doesn't natively support configuring timeouts, but we just hardcode a default.
// Timeout idle connections after 6 minutes (we ping at least every 5 minutes).
const POWERSYNC_SOCKET_DEFAULT_TIMEOUT = 360_000;
// Timeout for the initial connection (pre-TLS)
// Must be less than the timeout for a HTTP request
const POWERSYNC_SOCKET_CONNECT_TIMEOUT = 20_000;
// END POWERSYNC

const pbkdf2 = promisify(_pbkdf2);

Object.assign(SaslScramSha256.prototype, {
  _b64encode(bytes) {
    return Buffer.from(bytes).toString('base64');
  },
  _b64decode(b64) {
    return Uint8Array.from(Buffer.from(b64, 'base64'));
  },
  _randomBytes(n) {
    return randomFillSync(new Uint8Array(n));
  },
  async _hash(val) {
    return Uint8Array.from(createHash('sha256').update(val).digest());
  },
  async _hi(pwd, salt, iterations) {
    const buf = await pbkdf2(pwd, salt, iterations, 32, 'sha256');
    return Uint8Array.from(buf);
  }
});

Object.assign(_net, {
  connect({ hostname, port }) {
    return SocketAdapter.connect(hostname, port);
  },
  reconnectable(err) {
    return ['ENOTFOUND', 'ECONNREFUSED', 'ECONNRESET'].includes(err?.code);
  },
  startTls(sockadapt, { hostname, caCerts }) {
    return sockadapt.startTls(hostname, caCerts);
  },
  read(sockadapt, out) {
    return sockadapt.read(out);
  },
  write(sockadapt, data) {
    return sockadapt.write(data);
  },
  closeNullable(sockadapt) {
    if (!sockadapt) return;
    return sockadapt.close();
  }
});

class SocketAdapter {
  static async connect(host, port) {
    // START POWERSYNC
    // Custom timeout handling
    const socket = net.connect({ host, port, timeout: POWERSYNC_SOCKET_DEFAULT_TIMEOUT });
    try {
      const timeout = setTimeout(() => {
        socket.destroy(new Error(`Timeout while connecting to ${host}:${port}`));
      }, POWERSYNC_SOCKET_CONNECT_TIMEOUT);
      await once(socket, 'connect');
      clearTimeout(timeout);
      return new this(socket);
    } catch (e) {
      socket.destroy();
      throw e;
    }
    // END POWERSYNC
  }
  constructor(socket) {
    this._readResume = Boolean; // noop
    this._writeResume = Boolean; // noop
    this._readPauseAsync = (resolve) => (this._readResume = resolve);
    this._writePauseAsync = (resolve) => (this._writeResume = resolve);
    this._error = null;
    this._socket = socket;
    this._socket.on('readable', (_) => this._readResume());
    this._socket.on('end', (_) => this._readResume());
    // START POWERSYNC
    // Custom timeout handling
    this._socket.on('timeout', (_) => {
      this._socket.destroy(new Error('Socket idle timeout'));
    });
    // END POWERSYNC
    this._socket.on('error', (error) => {
      this._error = error;
      this._readResume();
      this._writeResume();
    });
  }
  async startTls(host, ca) {
    // START POWERSYNC CUSTOM OPTIONS HANDLING
    if (!Array.isArray(ca) && typeof ca[0] == 'object') {
      throw new Error('Invalid PowerSync TLS options');
    }
    const tlsOptions = ca[0];

    // https://nodejs.org/docs/latest-v14.x/api/tls.html#tls_tls_connect_options_callback
    const socket = this._socket;
    const tlsSocket = tls.connect({ socket, host, ...tlsOptions });
    // END POWERSYNC CUSTOM OPTIONS HANDLING
    await once(tlsSocket, 'secureConnect');
    // TODO check tlsSocket.authorized

    // if secure connection succeeded then we take underlying socket ownership,
    // otherwise underlying socket should be closed outside.
    tlsSocket.on('close', (_) => socket.destroy());
    return new this.constructor(tlsSocket);
  }
  /** @param {Uint8Array} out */
  async read(out) {
    let buf;
    for (;;) {
      if (this._error) throw this._error; // TODO callstack
      if (this._socket.readableEnded) return null;
      buf = this._socket.read();
      if (buf?.length) break;
      if (!buf) await new Promise(this._readPauseAsync);
    }
    if (buf.length > out.length) {
      out.set(buf.subarray(0, out.length));
      this._socket.unshift(buf.subarray(out.length));
      // POWERSYNC: Add metrics
      recordBytesRead(out.length);
      return out.length;
    }
    out.set(buf);
    // POWERSYNC: Add metrics
    recordBytesRead(buf.length);
    return buf.length;
  }
  async write(data) {
    // TODO assert Uint8Array
    // TODO need to copy data?
    if (this._error) throw this._error; // TODO callstack
    const p = new Promise(this._writePauseAsync);
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
