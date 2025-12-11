import { pbkdf2 as _pbkdf2 } from 'crypto';

import { _net } from 'pgwire';
import { SocketAdapter } from './socket_adapter.js';

Object.assign(_net, {
  connectTcp(options) {
    return SocketAdapter.connect(options);
  },
  connectUnix(_options) {
    throw `Unused and unsupported in PowerSync`;
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
