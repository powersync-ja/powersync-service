// Fork of pgwire/index.js, customized to handle additional TLS options
// Based on the version in commit 9532a395d6fa03a59c2231f0ec690806c90bd338
// Modifications marked with `START POWERSYNC ...`

import { createHash, pbkdf2 as _pbkdf2, randomFillSync } from 'crypto';

import { promisify } from 'util';
import { _net, SaslScramSha256 } from 'pgwire';
import { SocketAdapter } from './socket_adapter.js';

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
  connect(options) {
    return SocketAdapter.connect(options);
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

export * from 'pgwire';
