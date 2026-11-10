import net from 'node:net';
import tls from 'node:tls';
import { createHash, pbkdf2 as _pbkdf2, randomFillSync } from 'node:crypto';
import { once } from 'node:events';
import { promisify } from 'node:util';
import { _net, SaslScramSha256 } from './mod.js';
export * from './mod.js';

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
  },
});

Object.assign(_net, {
  connectTcp({ host, port, keepAlive }) {
    return SocketAdapter.connect({ host, port, keepAlive });
  },
  connectUnix({ path }) {
    return SocketAdapter.connect({ path });
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
    return sockadapt?.close();
  },
});

class SocketAdapter {
  static async connect(options) {
    const socket = net.connect(options);
    await once(socket, 'connect');
    return new this(socket);
  }
  constructor(socket) {
    this._readResume = Boolean; // noop
    this._writeResume = Boolean; // noop
    this._readPauseAsync = resolve => this._readResume = resolve;
    this._writePauseAsync = resolve => this._writeResume = resolve;
    this._error = null;
    this._socket = socket;
    this._socket.on('readable', _ => this._readResume());
    this._socket.on('end', _ => this._readResume());
    this._socket.on('error', error => {
      this._error = error;
      this._readResume();
      this._writeResume();
    });
  }
  async startTls(host, ca) {
    // https://nodejs.org/docs/latest-v14.x/api/tls.html#tls_tls_connect_options_callback
    const socket = this._socket;
    const secureContext = tls.createSecureContext({ ca });
    const tlsSocket = tls.connect({ socket, host, secureContext });
    await once(tlsSocket, 'secureConnect');
    // TODO check tlsSocket.authorized

    // if secure connection succeeded then we take underlying socket ownership,
    // otherwise underlying socket should be closed outside.
    tlsSocket.on('close', _ => socket.destroy());
    return new this.constructor(tlsSocket);
  }
  /** @param {Uint8Array} out */
  async read(out) {
    let buf;
    for (;;) {
      if (this._error) throw this._error; // TODO callstack
      if (this._socket.readableEnded) return null;
      const toRead = Math.min(out.length, this._socket.readableLength);
      buf = this._socket.read(toRead);
      if (buf?.length) break;
      if (!buf) await new Promise(this._readPauseAsync);
    }
    if (buf.length > out.length) { // should never happen
      throw Error('read more data than expected');
    }
    out.set(buf);
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
