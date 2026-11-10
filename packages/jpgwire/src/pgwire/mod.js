/* Copyright (c) 2026 exe-dealer@yandex.kz

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be included
in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

/** @deprecated */
export async function pgconnect(...optionsChain) {
  const conn = pgconnection(...optionsChain);
  await conn.query();
  return conn;
}

// https://github.com/kagis/pgwire/issues/15
export function pgconnection(...optionsChain) {
  return new PgConnection(computeConnectionOptions(optionsChain));
}

export function pgpool(...optionsChain) {
  return new PgPool(computeConnectionOptions(optionsChain));
}

export function pgliteral(s) {
  if (s == null) return 'NULL';
  // TODO array
  return `'` + String(s).replace(/'/g, `''`) + `'`;
}

export function pgident(...segments) {
  return segments.map(it => '"' + it.replace(/"/g, '""') + '"').join('.');
}

// TODO avoid class export
export class PgError extends Error {
  constructor(errorResponse) {
    super(errorResponse.message, { cause: errorResponse });
    this.name = 'PgError.' + errorResponse.code;
    Object.defineProperty(this, 'name', { enumerable: false });
  }
}

function computeConnectionOptions(optionsChain) {
  return optionsChain.reduceRight(reduceConnectionOptions, {
    host: '127.0.0.1',
    port: '5432',
  });
}
function reduceConnectionOptions(result, uriOrObj) {
  if (typeof uriOrObj == 'string') {
    uriOrObj = new URL(uriOrObj);
  }
  if (uriOrObj instanceof URL) {
    uriOrObj = {
      host: decodeURIComponent(uriOrObj.hostname) || undefined,
      port: uriOrObj.port || undefined,
      password: decodeURIComponent(uriOrObj.password) || undefined,
      'user': decodeURIComponent(uriOrObj.username) || undefined,
      'database': decodeURIComponent(uriOrObj.pathname).replace(/^[/]/, '') || undefined,
      ...Object.fromEntries(uriOrObj.searchParams)
    };
  }
  for (const [k, v] of Object.entries(uriOrObj)) {
    if (v === undefined) continue;
    result[k] = v;
  }
  return result;
}

class PgPool {
  // TODO? .pending, .queryable

  constructor(options) {
    /** @type {Set<PgConnection>} */
    this._connections = new Set();
    this._endReason = null;
    this._options = { ...options, _noExplicitTransactions: true };
    this._poolSize = Math.max(options._poolSize, 0);
    // postgres v14 has `idle_session_timeout` option.
    // But if .query will be called when socket is closed by server
    // (but Connection is not yet deleted from pool)
    // then query will be rejected with error.
    // We should be ready to repeat query in this case
    // and this is more difficult to implement than client side timeout
    this._poolIdleTimeout = Math.max(millis(options._poolIdleTimeout), 0);
  }
  query(...args) {
    return PgResult.fromStream(this.stream(...args));
  }
  async * stream(...args) {
    if (this._endReason) {
      throw Error('postgres query late', { cause: this._endReason });
    }
    const conn = this._getConnection();
    try {
      yield * conn.stream(...args);
    } finally {
      await this._recycleConnection(conn);
    }
  }
  async end() {
    this._endReason ||= Error('postgres pool ended');
    await Promise.all(Array.from(this._connections, conn => conn.end()));
  }
  async [Symbol.asyncDispose]() {
    await this.end();
  }
  destroy(cause) {
    this._endReason ||= Error('postgres pool destroyed', { cause });
    this._connections.forEach(it => it.destroy(cause));
  }
  [Symbol.dispose]() {
    this.destroy();
  }
  _getConnection() {
    // try to reuse connection
    if (this._poolSize > 0) {
      const queryableConns = Array.from(this._connections).filter(conn => conn.queryable);
      const full = queryableConns.length >= this._poolSize;
      const [leastBusyConn] = queryableConns.sort((a, b) => a.pending - b.pending);
      if (leastBusyConn?.pending == 0 || full) {
        clearTimeout(leastBusyConn._poolTimeoutId);
        return leastBusyConn;
      }
    }
    const newConn = new PgConnection(this._options);
    newConn._poolTimeoutId = null; // TODO check whether it disables v8 hidden class optimization
    this._connections.add(newConn);
    newConn.whenDestroyed.then(this._onConnectionDestroyed.bind(this, newConn));
    return newConn;
  }
  async _recycleConnection(conn) {
    if (!(this._poolSize > 0)) {
      // TODO not wait
      // but pgbouncer does not allow async Terminate anyway.
      // May be other poolers do
      await conn.end();
      return;
    }
    if (this._poolIdleTimeout && conn.queryable && !conn.pending /* is idle */) {
      conn._poolTimeoutId = setTimeout(this._onConnectionTimeout, this._poolIdleTimeout, this, conn);
    }
  }
  _onConnectionTimeout(_self, conn) {
    conn.end();
    // TODO conn.destroy() if .end() stuck?
  }
  _onConnectionDestroyed(conn) {
    clearTimeout(conn._poolTimeoutId);
    this._connections.delete(conn);
  }
}

class PgConnection {
  constructor({
    host, // https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-HOST
    port, // https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-PORT
    password, // https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-PASSWORD
    keepalives = '1', // https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-KEEPALIVES
    sslmode, // https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-SSLMODE
    sslrootcert, // TODO accept filename as psql do
    _connectRetry = 0, // TODO connect_timeout
    _wakeInterval = 2000,
    _noExplicitTransactions = false,
    _maxReadBuf = 50 << 20, // 50 MiB
    _debug = false,
    ...options
  }) {
    this.onnotification = null;
    this.parameters = Object.create(null);
    this._debug = /^(true|on|yes|1)$/i.test(_debug);
    this._socketOptions = {
      host,
      port: Number(port),
      keepAlive: /^(true|on|yes|1)$/i.test(keepalives),
      // TODO windows support
      // https://github.com/postgres/postgres/blob/c37267162e889fe783786b9e28d1b65b82365a00/src/include/libpq/pqcomm.h#L67
      path: /^[/]/.test(host) && host.replace(/[/]*$/g, '/.s.PGSQL.' + port),
    };
    this._user = options.user;
    this._password = password || '';
    this._sslmode = sslmode;
    this._sslrootcert = sslrootcert;
    this._ssl = false;
    // TODO ioloop starts not now
    this._connectDeadline = performance.now() + (Math.max(millis(_connectRetry), 0) || NaN);
    this._socket = null;
    this._backendKeyData = null;
    this._lastErrorResponse = null;
    this._queuedResponseChannels = [];
    /** @type {'start'|'idle'|'responding'|'end'} */
    this._readyState = 'start';
    this._transactionStatus = -1;
    this._wakeSupress = true;
    this._wakeInterval = Math.max(millis(_wakeInterval), 0);
    this._wakeTimer = null;
    this._maxReadBuf = _maxReadBuf;
    this._rowProto = null;
    this._rowTextDecoder = new TextDecoder('utf-8', { fatal: true });
    this._copyingOut = false;
    this._femsgw = null;
    this._femsgq = new Channel();
    this._endReason = null;
    this._sasl = null;
    this._noExplicitTransactions = _noExplicitTransactions;
    // create StartupMessage here to validate options in constructor call stack
    this._startupmsg = new FrontendMessage.StartupMessage({
      // Underscored props are used for client only,
      // Underscore prefix is not supported by postgres anyway
      // https://github.com/postgres/postgres/blob/REL_14_2/src/backend/utils/misc/guc.c#L5398
      ...Object.fromEntries(
        Object.entries(options)
        .filter(([k]) => !k.startsWith('_'))
      ),
      'client_encoding': 'UTF8', // TextEncoder supports UTF8 only, so hardcode and force UTF8
    });
    this._pipeFemsgqPromise = null;
    this._ioloopPromise = null;
  }
  get whenDestroyed() {
    return this._femsgq._whenEnded.then(_ => this._ioloopPromise);
  }
  /** Number of pending queries. */
  get pending() {
    return this._queuedResponseChannels.length;
  }
  get queryable() {
    return !this._endReason;
  }
  /** ID of postgres backend process. */
  get pid() {
    if (!this._backendKeyData) return null;
    return this._backendKeyData.pid;
  }
  get ssl() {
    return this._ssl;
  }
  /** Truthy if connection is idle in transaction. */
  get inTransaction() {
    const s = this._transactionStatus;
    return this._readyState == 'idle' && s != 0x49 ? s : 0;
  }
  // TODO executemany?
  // Parse
  // Describe(portal)
  // Bind
  // Execute
  // Bind
  // Execute
  query(...args) {
    return PgResult.fromStream(this.stream(...args));
  }
  async * stream(...args) {
    if (this._endReason) {
      throw Error('postgres query late', this._endReason);
    }
    // TODO test .query after .end should throw stable error ? what if auth failed after .end

    const stdinAborter = new AbortController();
    const responseEndLock = new Channel();
    const responseChannel = new Channel();
    // Collect outgoing messages into intermediate array
    // to let errors occur before any message enqueued.
    const frontendMessages = [];
    // To cancel query we should send CancelRequest _after_ query is received
    // by server and started executing. We going to prepend query with Sync
    // message to detect response start boundary by receiving ReadyForQuery.
    // ReadyForQuery is flushed immediatly after Sync is handled by server
    // so we can know when query is started and can be cancelled.
    // Also it makes possible to determine whether NoticeResponse/ErrorResponse
    // is asyncronous server message or belongs to query.
    // Seems that its ok to do Sync during simple protocol even when replication=database.
    // TODO Maybe there is a small window between first ReadyForQuery and actual
    // query execution, when async messages can be received. Need more source digging
    frontendMessages.push(new FrontendMessage.Sync());
    let signal;
    if (typeof args[0] == 'string') {
      const [simpleSql, simpleOptions] = args;
      signal = simpleOptions?.signal;
      simpleQuery(frontendMessages, stdinAborter.signal, simpleSql, simpleOptions, responseEndLock);
    } else {
      signal = extendedQuery(frontendMessages, stdinAborter.signal, args);
    }
    // this._throwIfQueryAborted(signal);
    const onabort = this._wake.bind(this);
    // TODO if next two steps throw error then abort handler will not be removed.
    signal?.addEventListener('abort', onabort);

    // TODO This two steps should be executed atomiсally.
    // If one fails then connection instance
    // will be desyncronized and should be destroyed.
    // But I see no cases when this can throw error.
    frontendMessages.forEach(this._femsgq.push, this._femsgq);
    this._queuedResponseChannels.push(responseChannel);

    this._ioloopPromise ||= this._ioloop();

    try {
      let value, done, errorResponse;
      for (;;) {
        // Important to call responseChannel.next() as first step of try block
        // to maximize chance that finally block will be executed after
        // query is received by server. So CancelRequest will be emited
        // in case of error, and skip-waiting duration will be minimized.
        ({ value, done } = await responseChannel.next());
        if (done) break;
        if (value.tag == 'ErrorResponse') {
          errorResponse = value.payload;
        }
        yield value;
        if (signal?.aborted) {
          throw Error('postgres query aborted', { cause: signal.reason });
        }
      }
      if (errorResponse) {
        throw new PgError(errorResponse);
      }
      if (value) { // TODO Error('postgres connection destroyed during query') ?
        throw Error('postgres query failed', value);
      }
    } catch (ex) {
      stdinAborter.abort(ex);
      throw ex;
    } finally {
      // if query completed successfully then all stdins are drained,
      // so abort will have no effect and will not break anything.
      // Оtherwise
      // - if error occured
      // - or iter was .return()'ed
      // - or no COPY FROM STDIN was queried
      // then we should abort all pending stdins
      stdinAborter.abort();

      // https://github.com/kagis/pgwire/issues/17
      if ( // going to do CancelRequest
        // if response is started and not ended
        // and there are no other pending responses
        // (to prevent miscancel of next queued query)
        this._readyState == 'responding' &&
        this._queuedResponseChannels.length == 1 &&
        this._queuedResponseChannels[0] == responseChannel
      ) {
        if (!this._endReason) {
          // new queries should not be emitted during CancelRequest
          this._femsgq.push(responseEndLock);
        }
        // TODO there is a small window between first Sync and Query message
        // when query is not actually executing, so there is small chance
        // that CancelRequest may be ignored. So need to repeat CancelRequest until
        // responseChannel.return() is resolved
        await this._cancelRequest().catch(this._log);
      }
      await responseChannel.return(); // wait ReadyForQuery
      responseEndLock.end();
      // signal is user-provided object, so it can throw and override query error.
      // TODO Its not clear how should we handle this errors, so just do this step
      // at the end of function to minimize impact on code above.
      signal?.removeEventListener('abort', onabort);
    }
  }

  // should immediately close connection for new queries
  // should terminate connection gracefully
  // should be idempotent
  // should never throw, at least when used in finally block.

  /** terminates connection gracefully if possible and waits until termination complete */
  async end() {
    // TODO implement options.timeout ?
    if (!this._endReason) {
      // TODO
      // if (pgbouncer) {
      //   const lock = new Channel();
      //   this._whenIdle.then(_ => lock.end());
      //   this._tx.push(lock);
      // }
      this._femsgq.push(new FrontendMessage.Terminate());
      this._femsgq.end();
      this._endReason = { cause: Error('postgres connection ended') };
    }
    // TODO _net.close can throw
    await this._ioloopPromise;
  }
  async [Symbol.asyncDispose]() {
    await this.end();
  }
  destroy(cause = Error('postgres connection destroyed')) {
    clearInterval(this._wakeTimer);
    // TODO it can be overwritten in ioloop.
    // It used for PgConnection.pid which is used to
    // determine if connection is alive.
    this._backendKeyData = null;
    this._msgw = null;
    _net.closeNullable(this._socket); // TODO can throw ?
    this._socket = null;

    if (!this._endReason) {
      this._femsgq.end();
      this._endReason = { cause };
      this._log('connection destroyed', cause);
    }
    // TODO await _flushData
    this._readyState = 'end';
    while (this._queuedResponseChannels.length) {
      this._queuedResponseChannels.shift().end({ cause });
    }
    // TODO do CancelRequest to wake stuck connection which can continue executing ?
  }
  [Symbol.dispose]() {
    this.destroy();
  }
  logicalReplication(options) {
    return new ReplicationStream(this, options);
  }

  async _cancelRequest() {
    if (!this._backendKeyData) {
      throw Error('CancelRequest attempt before BackendKeyData received');
    }
    const socket = await _net.connect({ ...this._socketOptions, keepAlive: false });
    try {
      // TODO ssl support
      // if (this._ssl) {
      //   socket = await this._sslNegotiate(socket);
      // }
      const m = new FrontendMessage.CancelRequest(this._backendKeyData);
      this._logFemsg(m, null);
      await MessageWriter.writeMessage(socket, m);
    } finally {
      _net.closeNullable(socket);
    }
  }
  async _ioloop() {
    let caughtException;
    try {
      while (await this._ioloopAttempt());
    } catch (ex) {
      caughtException = ex || Error('ioloop failed', { cause: ex });
    }

    if (this._lastErrorResponse && this._readyState != 'responding') {
      // broadcast _lastErrorResponse if it not belongs to current query
      // e.g. ErrorResponse during startup
      const errorResponseChunk = new PgChunk();
      errorResponseChunk.tag = 'ErrorResponse';
      errorResponseChunk.payload = this._lastErrorResponse;
      this._queuedResponseChannels.forEach(it => it.push(errorResponseChunk));
    }

    // TODO If we have _lastErrorResponse when socket closed abruptly
    // then we should report _lastErrorResponse instead of io error
    // because _lastErrorResponse will contain more usefull information
    // about socket destroy reason.
    this.destroy(
      caughtException ||
      this._lastErrorResponse ||
      Error('postgres eof unexpectedly')
    );
    await this._pipeFemsgqPromise;
  }
  async _ioloopAttempt() {
    this.parameters = Object.create(null);
    this._lastErrorResponse = null;
    this._ssl = false;
    this._sasl = null;
    this._femsgw = null;
    _net.closeNullable(this._socket);
    this._socket = null;

    try {
      this._socket = await _net.connect(this._socketOptions);
    } catch (ex) {
      if (_net.reconnectable(ex) && await this._connectRetryPause()) {
        this._log('retrying connection', ex);
        return true; // restart
      }
      throw ex;
    }

    // TODO tcp only? which host should be used in case of unix socket?
    if (/^(try_ssl|require|prefer)$/.test(this._sslmode)) {
      // https://www.postgresql.org/docs/16/protocol-flow.html#PROTOCOL-FLOW-SSL
      const sslReq = new FrontendMessage.SSLRequest();
      this._logFemsg(sslReq, null);
      await MessageWriter.writeMessage(this._socket, sslReq);
      const sslResp = await MessageReader.readSSLResponse(this._socket);
      this._log('n-> ssl available %o', sslResp);
      if (sslResp) {
        try {
          this._socket = await _net.startTls(this._socket, {
            hostname: this._socketOptions.host,
            caCerts: this._sslrootcert && [].concat(this._sslrootcert),
          });
        } catch (ex) {
          if (this._sslmode == 'prefer') {
            this._log('retrying connection without ssl because', ex);
            this._sslmode = 'try_nossl';
            return true; // restart
          }
          throw ex;
        }
        this._ssl = true; // ssl established
      } else if (this._sslmode == 'require') {
        throw Error('postgres does not support ssl');
      } else if (this._sslmode == 'try_ssl') {
        // TODO throw error which caused connection retry
        throw Error('postgres does not support ssl (retry)');
      }
    }

    this._femsgw = new MessageWriter(this._socket);
    await this._writeFemsg(this._startupmsg);

    for await (const chunk of MessageReader.readMessages(this._socket, this._maxReadBuf)) {
      await this._recvMessages(chunk);
    }

    // retry connection if ErrorResponse received before authenticated
    if (this._lastErrorResponse && this._readyState == 'start') {
      const { code } = this._lastErrorResponse;
      // 57P03 cannot_connect_now
      if (code == '57P03' && await this._connectRetryPause()) {
        this._log('retrying connection');
        return true; // restart
      }
      // 28000 invalid_authorization_specification
      if (code == '28000' && this._sslmode == 'prefer' && this._ssl) {
        this._log('retrying connection without ssl');
        this._sslmode = 'try_nossl';
        return true; // restart
      }
      // TODO can this._ssl be true when this._sslmode == 'allow' ?
      if (code == '28000' && this._sslmode == 'allow' && !this._ssl) {
        this._log('retrying connection with ssl');
        this._sslmode = 'try_ssl';
        return true; // restart
      }
      // TODO if error during this._sslmode = try_nossl
      // then should throw original error which caused connection retry
    }
  }
  async _connectRetryPause() {
    return (
      this._connectDeadline - performance.now() > 0 &&
      new Promise(resolve => setTimeout(resolve, 2000, true))
    );
  }
  async _recvMessages({ nparsed, messages }) {
    // we are going to batch DataRows and CopyData synchronously
    const batch = {
      rows: [],
      copies: [],
      pos: 0,
      buf: null,
      // Use last backend messages chunk size as buf size hint.
      bufsize: nparsed,
    };
    for (const m of messages) {
      this._logBemsg(m);
      // TODO check if connection is destroyed to prevent errors?

      switch (m.tag) {
        case 'DataRow': this._recvDataRow(m, m.payload, batch); continue;
        case 'CopyData': this._recvCopyData(m, m.payload, batch); continue;
      }
      await this._flushBatch(batch);

      // TODO sync push to responseChannel and wait for currResponseChannel drained in the end
      switch (m.tag) {
        case 'CopyInResponse': await this._recvCopyInResponse(m, m.payload); break;
        case 'CopyOutResponse': await this._recvCopyOutResponse(m, m.payload); break;
        case 'CopyBothResponse': await this._recvCopyBothResponse(m, m.payload); break;
        case 'CopyDone': await this._recvCopyDone(m, m.payload); break;
        case 'RowDescription': await this._recvRowDescription(m, m.payload); break;
        case 'CommandComplete': await this._recvCommandComplete(m, m.payload); break;
        case 'ParameterStatus': await this._recvParameterStatus(m, m.payload); break;
        case 'BackendKeyData': await this._recvBackendKeyData(m, m.payload); break;
        case 'NoticeResponse': await this._recvNoticeResponse(m, m.payload); break;
        case 'ErrorResponse': await this._recvErrorResponse(m, m.payload); break;
        case 'ReadyForQuery': await this._recvReadyForQuery(m, m.payload); break;
        case 'NotificationResponse': this._recvNotificationResponse(m, m.payload); break;

        case 'NoData':
        case 'ParameterDescription':
        case 'ParseComplete':
        case 'BindComplete':
        case 'PortalSuspended':
        case 'CloseComplete':
        case 'EmptyQueryResponse':
          await this._fwdBemsg(m);
          break;

        case 'AuthenticationOk': await this._recvAuthenticationOk(m, m.payload); break;
        case 'AuthenticationMD5Password': await this._recvAuthenticationMD5Password(m, m.payload); break;
        case 'AuthenticationCleartextPassword': await this._recvAuthenticationCleartextPassword(m, m.payload); break;
        case 'AuthenticationSASL': await this._recvAuthenticationSASL(m, m.payload); break;
        case 'AuthenticationSASLContinue': await this._recvAuthenticationSASLContinue(m, m.payload); break;
        case 'AuthenticationSASLFinal': await this._recvAuthenticationSASLFinal(m, m.payload); break;

        default:
          throw Error('postgres sent unsupported message', { case: m });
      }
    }
    await this._flushBatch(batch);
  }
  async _pipeFemsgq() {
    const stack = [];
    try {
      // we are going to buffer messages from source iterator until
      // .next returns resolved promises, and then flush buffered
      // messages when .next goes to do real asynchronous work.
      const stub = { pending: true };
      for (let pnext = { value: this._femsgq };;) {
        const { pending, done, value } = await Promise.race([pnext, stub]);
        if (pending) {
          await this._femsgw.flush();
          await pnext;
          continue;
        }
        if (done) {
          await this._femsgw.flush();
          await stack.shift().return();
          if (!stack.length) break;
        } else if (value[Symbol.asyncIterator]) {
          stack.unshift(value[Symbol.asyncIterator]());
        } else {
          this._logFemsg(value);
          // TODO flush if buffer size reached threshold
          this._femsgw.writeMessage(value);
          // msgbuf.count = msgbuf.count + 1 || 1;
        }
        pnext = stack[0].next();
      }
    } catch (ex) {
      // _femsgq.return() will wait until _femsgq.end()
      // so we need to destroy connection here to prevent deadlock.
      this.destroy(ex);
    } finally {
      await Promise.all(stack.map(it => it.return()));
    }
  }
  async _writeFemsg(m) {
    this._logFemsg(m);
    this._femsgw.writeMessage(m);
    await this._femsgw.flush();
  }
  _logFemsg({ tag, payload }, pid = this.pid) {
    if (!this._debug) return;
    pid = pid || 'n';
    const tagStyle = 'font-weight: bold; color: magenta';
    if (typeof payload == 'undefined') {
      return console.error('%s<- %c%s', pid, tagStyle, tag);
    }
    console.error('%s<- %c%s%c %o', pid, tagStyle, tag, '', payload);
  }
  _logBemsg({ tag, payload }) {
    if (!this._debug) return;
    const pid = this.pid || 'n';
    if (typeof payload == 'undefined') {
      return console.error('%s-> %s', pid, tag);
    }
    console.error('%s-> %s %o', pid, tag, payload);
  }
  async _recvRowDescription(m, columns) {
    const kRawTuple = Symbol('RawTuple');
    function PgRow(raw) {
      this[kRawTuple] = raw;
    }
    Object.defineProperties(PgRow.prototype, {
      length: { enumerable: false, value: columns.length },
      columns: { enumerable: false, value: columns },
      raw: { enumerable: false, get() { return this[kRawTuple]; } },
      [Symbol.iterator]: { value: Array.prototype.values },
    });
    for (const [i, { typeOid }] of columns.entries()) {
      Object.defineProperty(PgRow.prototype, i, {
        enumerable: true,
        get() {
          const val = this[kRawTuple][i];
          return typeof val == 'string' ? PgType.decode(val, typeOid) : val;
        },
      });
    }
    /** @type {any} Required for TypeScript declaration emit of this private row constructor. */
    this._rowCtor = PgRow;
    await this._fwdBemsg(m);
  }
  _recvDataRow(_, /** @type {Array<Uint8Array>} */ row, batch) {
    const { columns } = this._rowCtor.prototype;
    for (let i = 0; i < columns.length; i++) {
      const valbuf = row[i];
      if (!valbuf) continue;
      const { binary } = columns[i];
      // TODO avoid this._rowTextDecoder.decode for bytea
      // TODO maybe allocate buffer per socket.read will be faster
      // then allocating and copying buffer per cell in binary case?
      row[i] = binary ? valbuf.slice() : this._rowTextDecoder.decode(valbuf);
    }
    batch.rows.push(new this._rowCtor(row));
  }
  _recvCopyData(_, /** @type {Uint8Array} */ data, batch) {
    if (!this._copyingOut) {
      // postgres can send CopyData when not in copy out mode, steps to reproduce:
      //
      // <- StartupMessage({ database: 'postgres', user: 'postgres', replication: 'database' })
      // <- Query('CREATE_REPLICATION_SLOT a LOGICAL test_decoding')
      // wait for ReadyForQuery
      // <- Query('START_REPLICATION SLOT a LOGICAL 0/0');
      // <- CopyDone() before wal_sender_timeout expires
      // -> CopyData keepalive message can be received here after CopyDone but before ReadyForQuery
      //
      // <- Query('CREATE_REPLICATION_SLOT b LOGICAL test_decoding')
      // -> CopyData keepalive message sometimes can be received here before RowDescription message
      return;
    }
    batch.buf ||= new Uint8Array(batch.bufsize);
    batch.buf.set(data, batch.pos);
    batch.copies.push(batch.buf.subarray(batch.pos, batch.pos + data.length));
    batch.pos += data.length;
  }
  async _flushBatch(batch) {
    if (batch.copies.length) {
      const copyChunk = new PgChunk(batch.buf.buffer, batch.buf.byteOffset, batch.pos);
      copyChunk.tag = 'CopyData';
      copyChunk.copies = batch.copies;
      batch.buf = batch.buf.subarray(batch.pos);
      batch.pos = 0;
      batch.copies = [];
      this._wakeSupress = true;
      await this._queuedResponseChannels[0].push(copyChunk);
    }
    if (batch.rows.length) {
      const rowsChunk = new PgChunk();
      rowsChunk.tag = 'DataRow';
      rowsChunk.rows = batch.rows;
      this._wakeSupress = true;
      await this._queuedResponseChannels[0].push(rowsChunk);
      batch.rows = [];
    }
  }
  async _recvCopyInResponse(m) {
    await this._fwdBemsg(m);
  }
  async _recvCopyOutResponse(m) {
    this._copyingOut = true;
    await this._fwdBemsg(m);
  }
  async _recvCopyBothResponse(m) {
    this._copyingOut = true;
    await this._fwdBemsg(m);
  }
  async _recvCopyDone(m) {
    this._copyingOut = false;
    await this._fwdBemsg(m);
  }
  async _recvCommandComplete(m) {
    // when call START_REPLICATION second time then replication is not started,
    // but CommandComplete received right after CopyBothResponse without CopyDone.
    // I cannot find any documentation about this postgres behavior.
    // Seems that this line is responsible for this
    // https://github.com/postgres/postgres/blob/0266e98c6b865246c3031bbf55cb15f330134e30/src/backend/replication/walsender.c#L2307
    // streamingDoneReceiving and streamingDoneSending not reset to false before replication start
    this._copyingOut = false;
    await this._fwdBemsg(m);
  }
  async _recvAuthenticationCleartextPassword() {
    // should be always encoded as utf8 even when server_encoding is win1251
    await this._writeFemsg(new FrontendMessage.PasswordMessage(this._password));
  }
  async _recvAuthenticationMD5Password(_, { salt }) {
    // should use server_encoding, but there is
    // no way to know server_encoding before authentication.
    // So it should be possible to provide password as Uint8Array
    const pwduser = Uint8Array.of(...utf8Encode(this._password), ...utf8Encode(this._user));
    const a = utf8Encode(hexEncode(md5(pwduser)));
    const b = 'md5' + hexEncode(md5(Uint8Array.of(...a, ...salt)));
    await this._writeFemsg(new FrontendMessage.PasswordMessage(b));

    function hexEncode(/** @type {Uint8Array} */ bytes) {
      return Array.from(bytes, b => b.toString(16).padStart(2, '0')).join('');
    }
    function utf8Encode(inp) {
      if (inp instanceof Uint8Array) return inp;
      return new TextEncoder().encode(inp);
    }
  }
  async _recvAuthenticationSASL(_, { mechanism }) {
    // TODO mechanism is suggestion list:
    // the server sends an AuthenticationSASL message.
    // It includes a list of SASL authentication mechanisms that the server can accept, in the server's preferred order.
    // if (mechanism != 'SCRAM-SHA-256') {
    //   throw new PgError({ message: 'Unsupported SASL mechanism ' + JSON.stringify(mechanism) });
    // }
    this._sasl = new SaslScramSha256();
    const data = await this._sasl.start();
    await this._writeFemsg(new FrontendMessage.SASLInitialResponse({
      mechanism: 'SCRAM-SHA-256',
      data,
    }));
  }
  async _recvAuthenticationSASLContinue(_, data) {
    const finmsg = await this._sasl.continue(data, this._password);
    await this._writeFemsg(new FrontendMessage.SASLResponse(finmsg));
  }
  _recvAuthenticationSASLFinal(_, data) {
    this._sasl.finish(data);
    this._sasl = null;
  }
  _recvAuthenticationOk() {}
  async _recvErrorResponse(m, payload) {
    this._copyingOut = false;
    this._lastErrorResponse = payload;
    if (this._readyState == 'responding') {
      await this._fwdBemsg(m);
    }
  }
  async _recvReadyForQuery(m, { transactionStatus }) {
    switch (this._readyState) {
      case 'start':
        this._startupComplete();
        this._readyState = 'idle';
        break;

      case 'idle': // response start
        this._readyState = 'responding';
        await this._fwdBemsg(m);
        break;

      case 'responding': // response end
        if (this._noExplicitTransactions && transactionStatus != 0x49 /*I*/) {
          // TODO how to prevent next query from executing in transaction of previous query?
          throw Error('postgres connection left in transaction', { cause: this._lastErrorResponse });
        }
        await this._fwdBemsg(m);
        this._readyState = 'idle';
        this._lastErrorResponse = null;
        this._queuedResponseChannels.shift().end();
        break;
    }
    this._transactionStatus = transactionStatus;
  }
  _startupComplete() {
    // we dont need password anymore, its more secure to forget it
    this._password = null;
    // check if SASL auth is complete and server is verified
    if (this._sasl) {
      throw Error('incomplete SASL authentication');
    }
    // TODO impl require_auth
    // https://www.postgresql.org/docs/16/libpq-connect.html#LIBPQ-CONNECT-REQUIRE-AUTH
    this._pipeFemsgqPromise = this._pipeFemsgq();
    if (this._wakeInterval > 0) {
      this._wakeTimer = setInterval(
        this._wakeIfStuck.bind(this),
        this._wakeInterval,
      );
    }
  }
  async _recvNoticeResponse(m) {
    if (this._readyState == 'responding') {
      await this._fwdBemsg(m);
    }
    // TODO dispatchEvent for nonquery NoticeResponse
  }
  _recvParameterStatus(_, { parameter, value }) {
    this.parameters[parameter] = value;
    // TODO emit event ?
  }
  _recvBackendKeyData(_, backendKeyData) {
    this._backendKeyData = backendKeyData;
  }
  _recvNotificationResponse(_, payload) {
    // Detach onnotification call from _ioloop to avoid error swallowing.
    Promise.resolve(payload).then(this.onnotification);
  }
  async _fwdBemsg({ tag, payload }) {
    const chunk = new PgChunk();
    chunk.tag = tag;
    chunk.payload = payload;
    this._wakeSupress = true;
    await this._queuedResponseChannels[0].push(chunk);
  }
  _wakeIfStuck() {
    // We will send 'wake' chunk if response
    // is stuck longer then _wakeInterval
    // so user have a chance to break loop
    if (this._wakeSupress) {
      this._wakeSupress = false;
      return;
    }
    this._wake();
  }
  _wake() {
    const resp = this._queuedResponseChannels[0];
    if (this._readyState == 'responding' && resp.drained) {
      const wakeChunk = new PgChunk();
      wakeChunk.tag = 'wake';
      resp.push(wakeChunk);
    }
  }
  _log = (...args) => {
    if (!this._debug) return;
    console.error(...args);
  }
}

class PgChunk extends Uint8Array {
  tag;
  payload = null;
  rows = [];
  copies = [];
}

function simpleQuery(out, stdinSignal, script, { stdin, stdins = [] } = 0, responseEndLock) {
  out.push(new FrontendMessage.Query(script));
  // TODO handle case when number of stdins is unknown
  // should block tx and wait for CopyInResponse/CopyBothResponse
  if (stdin) {
    out.push(wrapCopyData(stdin, stdinSignal));
  }
  for (const stdin of stdins) {
    out.push(wrapCopyData(stdin, stdinSignal));
  }
  // when CREATE_REPLICATION_SLOT or START_REPLICATION is emitted
  // then no other queries should be emmited until ReadyForQuery is received.
  // Seems that its a postgres server bug.
  // This workaround looks like fragile hack but its not.
  // Its dirty but safe enough because no comments or other statements can
  // precede CREATE_REPLICATION_SLOT or START_REPLICATION
  if (/^\s*(CREATE_REPLICATION_SLOT|START_REPLICATION)\b/.test(script)) {
    out.push(responseEndLock);
  } else {
    out.push(new FrontendMessage.CopyFail('no stdin provided'));
  }
}
function extendedQuery(out, stdinSignal, blocks) {
  let signalBlock;
  for (const m of blocks) {
    if (signalBlock !== undefined) {
      throw Error('signal block must be last block');
    }
    if ('signal' in m) {
      signalBlock = m;
      continue;
    }
    switch (m.message) {
      case undefined:
      case null: extendedQueryStatement(out, m, stdinSignal); break;
      case 'Parse': extendedQueryParse(out, m); break;
      case 'Bind': extendedQueryBind(out, m); break;
      case 'Execute': extendedQueryExecute(out, m, stdinSignal); break;
      case 'DescribeStatement': extendedQueryDescribeStatement(out, m); break;
      case 'CloseStatement': extendedQueryCloseStatement(out, m); break;
      case 'DescribePortal': extendedQueryDescribePortal(out, m); break;
      case 'ClosePortal': extendedQueryClosePortal(out, m); break;
      case 'Flush': out.push(new FrontendMessage.Flush()); break;
      default: throw Error('unknown extended message');
    }
  }
  out.push(new FrontendMessage.Sync());
  return signalBlock?.signal;
}
function extendedQueryStatement(out, { statement, params = [], limit, binary, stdin, noBuffer }, stdinSignal) {
  const paramTypes = params.map(({ type }) => type);
  extendedQueryParse(out, { statement, paramTypes });
  extendedQueryBind(out, { params, binary });
  extendedQueryExecute(out, { limit, stdin, noBuffer }, stdinSignal);
  // if (noBuffer) {
  //   out.push(new Flush());
  // }
}
function extendedQueryParse(out, { statement, statementName, paramTypes = [] }) {
  const paramTypeOids = paramTypes.map(PgType.resolve, PgType);
  out.push(new FrontendMessage.Parse({ statement, statementName, paramTypeOids }));
}
function extendedQueryBind(out, { portal, statementName, binary, params = [] }) {
  params = params.map(encodeParam);
  out.push(new FrontendMessage.Bind({ portal, statementName, binary, params }));

  function encodeParam({ value, type}) {
    if (value == null) return null;
    // treat Uint8Array values as already encoded,
    // so user can receive value with unknown type as Uint8Array
    // from extended .query and pass it back as parameter
    // it also "encodes" bytea in efficient way instead of hex
    if (value instanceof Uint8Array) return value;
    return PgType.encode(value, PgType.resolve(type));
  }
}
function extendedQueryExecute(out, { portal, stdin, limit, noBuffer }, stdinSignal) {
  // TODO write test to explain why
  // we need unconditional DescribePortal
  // before Execute
  out.push(new FrontendMessage.DescribePortal(portal));
  out.push(new FrontendMessage.Execute({ portal, limit }));
  // if (noBuffer) {
  //   out.push(new Flush());
  // }
  if (stdin) {
    out.push(wrapCopyData(stdin, stdinSignal));
  } else {
    // CopyFail message ignored by postgres
    // if there is no COPY FROM statement
    out.push(new FrontendMessage.CopyFail('no stdin provided'));
  }
  // TODO nobuffer option
  // yield new Flush();
}
function extendedQueryDescribeStatement(out, { statementName }) {
  out.push(new FrontendMessage.DescribeStatement(statementName));
}
function extendedQueryCloseStatement(out, { statementName }) {
  out.push(new FrontendMessage.CloseStatement(statementName));
}
function extendedQueryDescribePortal(out, { portal }) {
  out.push(new FrontendMessage.DescribePortal(portal));
}
function extendedQueryClosePortal({ portal }) {
  out.push(new FrontendMessage.ClosePortal(portal));
}
async function * wrapCopyData(source, signal) {
  // TODO dry
  // if (abortSignal.aborted) {
  //   return;
  // }
  try {
    // TODO find a way to abort source
    for await (const chunk of source) {
      if (signal.aborted) {
        throw signal.reason;
      }
      yield new FrontendMessage.CopyData(chunk);
    }
    yield new FrontendMessage.CopyDone();
  } catch (ex) {
    // TODO err.stack lost
    // store err
    // do CopyFail (copy_error_key)
    // rethrow stored err when ErrorResponse received
    yield new FrontendMessage.CopyFail(String(ex));
  }
}

class PgResult {
  static async fromStream(stream) {
    const notices = [];
    const results = [];
    let result = new PgSubResult();
    for await (const chunk of stream) {
      result.rows.push(...chunk.rows);
      // lastResult.copies.push(...chunk.copies); // TODO chunk.copies
      switch (chunk.tag) {
        // TODO NoData
        // TODO ParameterDescription
        // TODO CopyOutResponse
        case 'NoticeResponse':
          notices.push(chunk.payload);
          break;
        // case 'CopyOutResponse':
        // case 'CopyBothResponse':
        //   lastResult.columns = chunk.payload.columns;
        //   break;
        case 'RowDescription':
          result.columns = chunk.payload;
          break;

        // statement result boundaries
        case 'CommandComplete':
        case 'PortalSuspended':
        case 'EmptyQueryResponse':
          result.status = chunk.payload || chunk.tag;
          results.push(result);
          result = new PgSubResult();
          break;
      }
    }
    return new PgResult(results, notices);
  }
  constructor(results, notices) {
    /** @type {PgSubResult[]} */
    this.results = results;
    /** @type {ErrorOrNotice[]} */
    this.notices = notices;
  }
  /** @deprecated */
  get scalar() {
    const [scalar] = this;
    return scalar;
  }
  get rows() {
    return this._lastSelect?.rows || [];
  }
  get columns() {
    return this._lastSelect?.columns || [];
  }
  get status() {
    // TODO return last statement status?
    return this.results.length == 1 ? this.results[0].status : null;
  }
  * [Symbol.iterator]() {
    const [row] = this.rows;
    if (row) yield * row;
  }
  get _lastSelect() {
    for (let i = this.results.length - 1; i >= 0; i--) {
      if (!this.results[i].columns.length) continue;
      return this.results[i];
    }
  }
}

class PgSubResult {
  /** @type {Row[]} */
  rows = [];
  /** @type {ColumnDescription[]} */
  columns = [];
  /** @type {string} */
  status = null;
  /** @deprecated */
  get scalar() { return this.rows[0]?.[0]; }
}

// https://www.postgresql.org/docs/14/protocol-message-types.html
class BinaryReader {
  /** @type {Uint8Array} */
  _b = null;
  _p = 0;
  // should not use { fatal: true } because ErrorResponse can use invalid utf8 chars
  static defaultTextDecoder = new TextDecoder();
  _textDecoder = BinaryReader.defaultTextDecoder;

  _reset(/** @type {Uint8Array} */ b) {
    this._b = b;
    this._p = 0;
  }
  _readUint8() {
    this._checkSize(1);
    return this._b[this._p++];
  }
  _readInt16() {
    this._checkSize(2);
    return (this._b[this._p++] << 8 | this._b[this._p++]) << 16 >> 16;
  }
  _readInt32() {
    this._checkSize(4);
    return ((this._b[this._p++] << 8 | this._b[this._p++]) << 8 |
      this._b[this._p++]) << 8 | this._b[this._p++];
  }
  _readString() {
    const endIdx = this._b.indexOf(0x00, this._p);
    if (endIdx < 0) {
      throw Error('unexpected end of message');
    }
    const strbuf = this._b.subarray(this._p, endIdx);
    this._p = endIdx + 1;
    return this._textDecoder.decode(strbuf);
  }
  _read(n) {
    this._checkSize(n);
    return this._b.subarray(this._p, this._p += n);
  }
  _readToEnd() {
    const p = this._p;
    this._p = this._b.length;
    return this._b.subarray(p);
  }
  _checkSize(n) {
    if (this._b.length < this._p + n) {
      throw Error('unexpected end of message');
    }
  }
  /**
   * @template T
   * @param {() => T} fn
   * @return {T[]} */
  _array(length, fn) {
    return Array.from({ length }, fn, this);
  }
  // replication helpers
  _readLsn() {
    const h = this._readUint32(), l = this._readUint32();
    if (h == 0 && l == 0) return null;
    return (
      h.toString(16).padStart(8, '0') + '/' +
      l.toString(16).padStart(8, '0')
    ).toUpperCase();
  }
  _readTime() {
    // (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * USECS_PER_DAY == 946684800000000
    return this._readUint64() + 946684800000000n;
  }
  _readUint64() {
    return BigInt(this._readUint32()) << 32n | BigInt(this._readUint32());
  }
  _readUint32() {
    return this._readInt32() >>> 0;
  }
}

// https://www.postgresql.org/docs/14/protocol-message-formats.html
class MessageReader extends BinaryReader {
  static async * readMessages(socket, maxReadBuf) {
    let buf = new Uint8Array(0x80000); // 512k
    maxReadBuf = Math.max(buf.length, maxReadBuf) | 0;
    let nbuf = 0;
    const msgr = new this();
    for (;;) {
      if (nbuf >= maxReadBuf) {
        throw Error('postgres sent too big message'); // TODO limit message size instead?
      }
      if (nbuf >= buf.length) { // grow buffer
        const oldbuf = buf;
        buf = new Uint8Array(Math.min(buf.length * 2, maxReadBuf));
        buf.set(oldbuf);
      }
      const nread = await _net.read(socket, buf.subarray(nbuf));
      if (nread == null) break; // EOF
      nbuf += nread;

      let nparsed = 0;
      const messages = [];
      for (;;) {
        const itag = nparsed;
        const isize = itag + 1;
        const ipayload = isize + 4;
        if (nbuf < ipayload) break; // incomplete message
        msgr._reset(buf.subarray(isize, ipayload));
        const size = msgr._readInt32();
        if (size < 4) {
          throw Error('postgres sent invalid message size');
        }
        const inext = isize + size;
        // TODO use grow hint
        if (nbuf < inext) break; // incomplete message
        msgr._reset(buf.subarray(ipayload, inext));
        const message = msgr._readBackendMessage(buf[itag]);
        messages.push(message);
        // TODO batch DataRow here
        nparsed = inext;
      }
      if (nparsed) {
        yield { nparsed, messages };
        buf.copyWithin(0, nparsed, nbuf); // move unconsumed bytes to begining of buffer
        nbuf -= nparsed;
      }
    }
    if (nbuf > 0) {
      throw Error('postgres eof before message complete'); // TODO report { cause: buf }
    }
  }

  /**
   * @template {string} T
   * @template P
   * @param {T} tag
   * @param {P} payload
   */
  _m(tag, payload) {
    return { tag, payload };
  }
  _readBackendMessage(asciiTag) {
    const m = this._m;
    switch (asciiTag) {
      case 0x64 /*d*/: return m('CopyData', this._b);
      case 0x44 /*D*/: return m('DataRow', this._readDataRow());
      case 0x76 /*v*/: return m('NegotiateProtocolVersion', this._readNegotiateProtocolVersion());
      case 0x53 /*S*/: return m('ParameterStatus', this._readParameterStatus());
      case 0x4b /*K*/: return m('BackendKeyData', this._readBackendKeyData());
      case 0x5a /*Z*/: return m('ReadyForQuery', this._readReadyForQuery());
      case 0x48 /*H*/: return m('CopyOutResponse', this._readCopyResponse());
      case 0x47 /*G*/: return m('CopyInResponse', this._readCopyResponse());
      case 0x57 /*W*/: return m('CopyBothResponse', this._readCopyResponse());
      case 0x63 /*c*/: return m('CopyDone');
      case 0x54 /*T*/: return m('RowDescription', this._readRowDescription());
      case 0x74 /*t*/: return m('ParameterDescription', this._readParameterDescription());
      case 0x43 /*C*/: return m('CommandComplete', this._readString());
      case 0x31 /*1*/: return m('ParseComplete');
      case 0x32 /*2*/: return m('BindComplete');
      case 0x33 /*3*/: return m('CloseComplete');
      case 0x73 /*s*/: return m('PortalSuspended');
      case 0x49 /*I*/: return m('EmptyQueryResponse');
      case 0x4e /*N*/: return m('NoticeResponse', this._readErrorOrNotice());
      case 0x45 /*E*/: return m('ErrorResponse', this._readErrorOrNotice());
      case 0x6e /*n*/: return m('NoData');
      case 0x41 /*A*/: return m('NotificationResponse', this._readNotificationResponse());
      case 0x52 /*R*/: {
        const authTag = this._readInt32();
        switch (authTag) {
          case 0     : return m('AuthenticationOk');
          case 2     : return m('AuthenticationKerberosV5');
          case 3     : return m('AuthenticationCleartextPassword');
          case 5     : return m('AuthenticationMD5Password', { salt: this._read(4) });
          case 6     : return m('AuthenticationSCMCredential');
          case 7     : return m('AuthenticationGSS');
          case 8     : return m('AuthenticationGSSContinue', this._readToEnd());
          case 9     : return m('AuthenticationSSPI');
          case 10    : return m('AuthenticationSASL', { mechanism: this._readString() });
          case 11    : return m('AuthenticationSASLContinue', this._readToEnd());
          case 12    : return m('AuthenticationSASLFinal', this._readToEnd());
          default    : throw Error('postgres sent unknown auth message', { cause: { authTag } });
        }
      }
      default: throw Error('postgres sent unknown message', { cause: { asciiTag } });
    }
  }
  // https://www.postgresql.org/docs/16/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-DATAROW
  _readDataRow() {
    const nfields = this._readInt16();
    const row = Array(nfields);
    for (let i = 0; i < nfields; i++) {
      const valsize = this._readInt32();
      row[i] = valsize < 0 ? null : this._read(valsize);
    }
    return row;
  }
  // https://www.postgresql.org/docs/16/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-NEGOTIATEPROTOCOLVERSION
  _readNegotiateProtocolVersion() {
    const version = this._readInt32();
    const unrecognizedOptions = this._array(this._readInt32(), this._readString);
    return { version, unrecognizedOptions };
  }
  // https://www.postgresql.org/docs/16/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-PARAMETERDESCRIPTION
  _readParameterDescription() {
    return this._array(this._readInt16(), this._readInt32);
  }
  // https://www.postgresql.org/docs/16/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-BACKENDKEYDATA
  _readBackendKeyData() {
    const pid = this._readInt32();
    const secretKey = this._readInt32();
    return { pid, secretKey };
  }
  // https://www.postgresql.org/docs/16/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-READYFORQUERY
  _readReadyForQuery() {
    return { transactionStatus: this._readUint8() };
  }
  // https://www.postgresql.org/docs/16/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-COPYINRESPONSE
  // https://www.postgresql.org/docs/16/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-COPYOUTRESPONSE
  // https://www.postgresql.org/docs/16/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-COPYBOTHRESPONSE
  _readCopyResponse() {
    const binary = this._readUint8();
    const columns = this._array(this._readInt16(), _ => ({
      // TODO make consistent with _readParameterDescription, object[] or number[]
      binary: this._readInt16(),
    }));
    return { binary, columns };
  }
  // https://www.postgresql.org/docs/16/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-ROWDESCRIPTION
  _readRowDescription() {
    return this._array(this._readInt16(), _ => ({
      name: this._readString(),
      tableOid: this._readInt32(),
      tableColumn: this._readInt16(),
      typeOid: this._readInt32(),
      typeSize: this._readInt16(),
      typeMod: this._readInt32(),
      binary: this._readInt16(),
    }));
  }
  // https://www.postgresql.org/docs/16/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-PARAMETERSTATUS
  _readParameterStatus() {
    const parameter = this._readString();
    const value = this._readString();
    return { parameter, value };
  }
  // https://www.postgresql.org/docs/16/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-ERRORRESPONSE
  _readErrorOrNotice() {
    const fields = /** @type {string[]} */ (Array(256));
    for (;;) {
      const fieldCode = this._readUint8();
      if (!fieldCode) break;
      fields[fieldCode] = this._readString();
    }
    // https://www.postgresql.org/docs/16/protocol-error-fields.html
    return {
      __proto__: pgErrorOrNoticeProto, // https://github.com/denoland/deno/issues/23416
      severity: fields[0x53], // S
      code: fields[0x43], // C
      message: fields[0x4d], // M
      detail: fields[0x44], // D
      hint: fields[0x48], // H
      position: fields[0x50] && Number(fields[0x50]), // P
      internalPosition: fields[0x70] && Number(fields[0x70]), // p
      internalQuery: fields[0x71], // q
      where: fields[0x57], // W
      file: fields[0x46], // F
      // TODO not parse L number? doc says no details about number format
      line: fields[0x4c] && Number(fields[0x4c]), // L
      routine: fields[0x52], // R
      schema: fields[0x73], // s
      table: fields[0x74], // t
      column: fields[0x63], // c
      datatype: fields[0x64], // d
      constraint: fields[0x6e], // n
      severityEn: fields[0x56], // V
    };
  }
  // https://www.postgresql.org/docs/16/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-NOTIFICATIONRESPONSE
  _readNotificationResponse() {
    const pid = this._readInt32();
    const channel = this._readString();
    const payload = this._readString();
    return { pid, channel, payload };
  }

  static async readSSLResponse(socket) {
    const readbuf = new Uint8Array(1);
    // Deno doc says that nread never equals 0, so don't need to repeat
    const nread = await _net.read(socket, readbuf);
    if (nread == null) {
      throw Error('postgres eof during ssl response');
    }
    const [resp] = readbuf;
    if (resp == 0x53 /*S*/) return true;
    if (resp == 0x4e /*N*/) return false;
    // TODO postgres doc says that we can receive
    // ErrorResponse in case of ancient server
    throw Error('postgres sent bad ssl response', { cause: { resp } });
  }
}

const pgErrorOrNoticeProto = {
  toString() {
    return JSON.stringify(this, null, 2);
  }
};

// https://www.postgresql.org/docs/14/protocol-replication.html#id-1.10.5.9.7.1.5.1.8
class ReplicationStream extends BinaryReader {
  constructor(conn, options) {
    super();
    this._tx = new Channel();
    this._ackingLsn = '00000000/00000000';
    this._ackmsg = Uint8Array.of(
      0x72, // r
      0, 0, 0, 0, 0, 0, 0, 0, // 1 - written lsn
      0, 0, 0, 0, 0, 0, 0, 0, // 9 - flushed lsn
      0, 0, 0, 0, 0, 0, 0, 0, // 17 - applied lsn
      0, 0, 0, 0, 0, 0, 0, 0, // 25 - time
      0, // 33 - should reply
    );
    this._ackmsgWrittenLsn = new DataView(this._ackmsg.buffer, 1, 8);
    this._ackmsgFlushedLsn = new DataView(this._ackmsg.buffer, 9, 8);
    this._ackmsgAppliedLsn = new DataView(this._ackmsg.buffer, 17, 8);
    this._iterator = this._start(conn, options);
  }
  pgoutputDecode() {
    return PgoutputReader.decodeStream(this);
  }
  [Symbol.asyncIterator]() {
    return this._iterator;
  }
  // TODO signal
  async * _start(conn, { slot, startLsn = '0/0', options = {}, ackInterval = 10e3 }) {
    this.ack(startLsn);

    const optionsSql = (
      Object.entries(options)
      .map(([k, v]) => pgident(k) + ' ' + pgliteral(v))
      .join(',')
      .replace(/.+/s, '($&)')
    );
    // TODO get wal_sender_timeout,
    // but user may have no access to wal_sender_timeout setting
    const startReplSql = `START_REPLICATION SLOT ${pgident(slot)} LOGICAL ${startLsn} ${optionsSql}`;
    const rx = conn.stream(startReplSql, { stdin: this._tx });

    const ackTimer = setInterval(this._ackImmediate.bind(this), ackInterval);
    let lastLsn = '00000000/00000000';
    let lastTime = 0n;
    try {
      for (;;) {
        const { value: chunk, done } = await rx.next();
        if (done) break;
        const messages = [];
        let shouldAck = false;
        for (const copyData of chunk.copies) {
          this._reset(copyData);
          const msg = this._readReplicationMessage();
          switch (msg.tag) {
            case 'XLogData':
              messages.push(msg);
              break;
            case 'PrimaryKeepaliveMessage':
              shouldAck = shouldAck || msg.shouldReply;
              // TODO messages.push(msg); ?
              break;
          }
          if (lastLsn < msg.lsn) lastLsn = msg.lsn;
          if (lastLsn < msg.endLsn) lastLsn = msg.endLsn;
          if (lastTime < msg.time) lastTime = msg.time;
        }
        if (shouldAck) {
          this._ackImmediate();
        }
        yield { lastLsn, lastTime, messages };
      }
    } finally {
      clearInterval(ackTimer);
      this._ackImmediate();
      this._tx.end();
      // TODO handle errors?
      for await (const _ of rx); // drain rx until end
    }
  }
  ack(lsn) {
    if (!/^[0-9a-f]{1,8}[/][0-9a-f]{1,8}$/i.test(lsn)) {
      throw RangeError('invalid lsn');
    }
    lsn = lsnMakeComparable(lsn);
    if (lsn > this._ackingLsn) {
      this._ackingLsn = lsn;
    }
    const nlsn = BigInt('0x' + this._ackingLsn.replace('/', ''));
    // https://github.com/postgres/postgres/blob/0526f2f4c38cb50d3e2a6e0aa5d51354158df6e3/src/backend/replication/logical/worker.c#L2473-L2478
    // https://github.com/postgres/postgres/blob/0526f2f4c38cb50d3e2a6e0aa5d51354158df6e3/src/backend/replication/walsender.c#L2021-L2023
    // TODO accept { written, flushed, applied, immediate }
    // Comments say that flushed/written are used for synchronous replication.
    // What walsender does with flushed/written?
    this._ackmsgWrittenLsn.setBigUint64(0, nlsn);
    this._ackmsgFlushedLsn.setBigUint64(0, nlsn);
    this._ackmsgAppliedLsn.setBigUint64(0, nlsn);
  }
  _ackImmediate() {
    if (!this._tx.drained) return;
    this._tx.push(this._ackmsg);
  }

  _readReplicationMessage() {
    const tag = this._readUint8();
    switch (tag) {
      case 0x77 /*w*/: return this._readXLogData();
      case 0x6b /*k*/: return this._readPrimaryKeepaliveMessage();
      default: throw Error('unknown replication message');
    }
  }
  _readXLogData() {
    return {
      tag: /** @type {const} */ ('XLogData'),
      lsn: this._readLsn(),
      // `endLsn` is always the same as `lsn` in case of logical replication.
      // https://github.com/postgres/postgres/blob/0a455b8d61d8fc5a7d1fdc152667f9ba1fd27fda/src/backend/replication/walsender.c#L1240
      endLsn: this._readLsn(),
      // https://github.com/postgres/postgres/blob/0a455b8d61d8fc5a7d1fdc152667f9ba1fd27fda/src/backend/replication/walsender.c#L1270-L1271
      time: this._readTime(),
      data: this._readToEnd(),
    };
  }
  _readPrimaryKeepaliveMessage() {
    return {
      tag: /** @type {const} */ ('PrimaryKeepaliveMessage'),
      lsn: null, // hidden class opt
      endLsn: this._readLsn(),
      time: this._readTime(),
      shouldReply: this._readUint8(),
    };
  }
}

// https://www.postgresql.org/docs/14/protocol-logicalrep-message-formats.html
class PgoutputReader extends BinaryReader {
  static async * decodeStream(replstream) {
    const pgoreader = new PgoutputReader();
    for await (const chunk of replstream) {
      for (const msg of chunk.messages) {
        pgoreader._reset(msg.data);
        pgoreader._upgradeMsg(msg);
        msg.data = null; // free XLogData buffer
      }
      yield chunk;
    }
  }

  /** @type {Map<number, { typeSchema: string, typeName: string }>} */
  _typeCache = new Map();
  _relationCache = new Map();

  _upgradeMsg(out) {
    const tag = this._readUint8();
    switch (tag) {
      case 0x42 /*B*/: return this._upgradeMsgBegin(out);
      case 0x4f /*O*/: return this._upgradeMsgOrigin(out);
      case 0x59 /*Y*/: return this._upgradeMsgType(out);
      case 0x52 /*R*/: return this._upgradeMsgRelation(out);
      case 0x49 /*I*/: return this._upgradeMsgChange(out, 'insert', true);
      case 0x55 /*U*/: return this._upgradeMsgChange(out, 'update', true);
      case 0x44 /*D*/: return this._upgradeMsgChange(out, 'delete', false);
      case 0x54 /*T*/: return this._upgradeMsgTruncate(out);
      case 0x4d /*M*/: return this._upgradeMsgMessage(out);
      case 0x43 /*C*/: return this._upgradeMsgCommit(out);
      default: throw Error('unknown pgoutput message', { cause: { tag } });
    }
  }
  _upgradeMsgBegin(out) {
    // TODO lsn can be null if origin sended
    // https://github.com/postgres/postgres/blob/85c61ba8920ba73500e1518c63795982ee455d14/src/backend/replication/pgoutput/pgoutput.c#L409
    out.tag = 'begin';
    // https://github.com/postgres/postgres/blob/27b77ecf9f4d5be211900eda54d8155ada50d696/src/include/replication/reorderbuffer.h#L275
    out.commitLsn = this._readLsn();
    out.commitTime = this._readTime();
    // https://github.com/postgres/postgres/blob/4d5105a684ba20bf6a98e6ae48224fe93382c040/src/backend/replication/logical/proto.c#LL67C30-L67C30
    // https://github.com/postgres/postgres/blob/4d5105a684ba20bf6a98e6ae48224fe93382c040/src/include/libpq/pqformat.h#L143-L149
    // pq_sendint32 handles both int32 and uint32, xid is uint32
    out.xid = this._readUint32();
  }
  _upgradeMsgOrigin(out) {
    out.tag = 'origin';
    out.originLsn = this._readLsn();
    out.originName = this._readString();
  }
  _upgradeMsgType(out) {
    out.tag = 'type';
    out.typeOid = this._readInt32();
    out.typeSchema = this._readString();
    out.typeName = this._readString();
    // mem leak not likely to happen because amount of types is usually small
    this._typeCache.set(out.typeOid, {
      typeSchema: out.typeSchema,
      typeName: out.typeName,
    });
  }
  _upgradeMsgRelation(out) {
    // lsn expected to be null
    // https://github.com/postgres/postgres/blob/27b77ecf9f4d5be211900eda54d8155ada50d696/src/backend/replication/walsender.c#L1342
    out.tag = 'relation';
    out.relationOid = this._readInt32();
    out.schema = this._readString();
    out.name = this._readString();
    out.replicaIdentity = this._readRelationReplicaIdentity();
    out.columns = this._array(this._readInt16(), this._readRelationColumn);
    out._tupleCtor = PgoTuple;

    const kRawTuple = Symbol('RawTuple'); // TODO global static
    function PgoTuple(raw) {
      this[kRawTuple] = raw;
    }
    PgoTuple.prototype = {
      __proto__: null,
      * [Symbol.iterator]() { for (const k in this) yield [k, this[k]]; },
    };
    for (const { name, typeOid } of out.columns) {
      Object.defineProperty(PgoTuple.prototype, name, {
        enumerable: true,
        get() {
          const val = this[kRawTuple][name];
          // TODO is !typeOid possible?
          return typeof val == 'string' && typeOid ? PgType.decode(val, typeOid) : val;
        },
      });
    }
    // mem leak not likely to happen because amount of relations is usually small
    this._relationCache.set(out.relationOid, out);
  }
  _readRelationReplicaIdentity() {
    // https://www.postgresql.org/docs/14/catalog-pg-class.html
    const relreplident = this._readUint8();
    switch (relreplident) {
      case 0x64 /*d*/: return 'default';
      case 0x6e /*n*/: return 'nothing';
      case 0x66 /*f*/: return 'full';
      case 0x69 /*i*/: return 'index';
      default: return relreplident;
    }
  }
  _readRelationColumn() {
    const col = {
      flags: this._readUint8(),
      name: this._readString(),
      typeOid: this._readInt32(),
      typeMod: this._readInt32(),
      typeSchema: null,
      typeName: null, // TODO resolve builtin type names?
    };
    Object.assign(col, this._typeCache.get(col.typeOid));
    return col;
  }
  _upgradeMsgChange(out, tag, mustHaveNewTuple) {
    const relid = this._readInt32();
    const relation = this._relationCache.get(relid);
    let before = Object.create(null);
    let after = null;
    const isNewTuple = this._readTuple(relation, before);
    if (isNewTuple) {
      after = before;
      before = null;
    } else if (mustHaveNewTuple) {
      after = Object.create(null);
      this._readTuple(relation, after, before);
      // TODO assert isNewTuple
    }
    out.tag = tag;
    out.relation = relation;
    // https://github.com/kagis/pgwire/issues/16
    // https://github.com/kagis/pgwire/issues/27
    out.beforeRaw = before;
    out.before = before && new relation._tupleCtor(before);
    out.afterRaw = after;
    out.after = after && new relation._tupleCtor(after);
    // avoid use out.new to make message destructuring easy
  }
  _readTuple({ columns }, tuple, unchangedToastFallback) {
    const kon = this._readUint8();
    // Treat nulls in key-only tuple (K) as placeholders rather then actual column values.
    // Key cannot have nulls by documentation anyway.
    const nullval = kon == 0x4b /*K*/ ? undefined : null;
    const nfields = this._readInt16();
    for (let i = 0; i < nfields; i++) {
      const { name } = columns[i];
      const kind = this._readUint8();
      let val;
      switch (kind) {
        case 0x62: // 'b' binary
        case 0x74: // 't' text
          // no need to clone bytes because new buffer
          // is created for each replication chunk
          val = this._read(this._readInt32());
          if (kind == 0x74 /* t */) {
            val = this._textDecoder.decode(val); // TODO fatal: true, lossless utf8 decoding
          }
          break;
        case 0x6e: // 'n' null
          val = nullval;
          break;
        case 0x75: // 'u' unchanged toast datum
          val = unchangedToastFallback?.[name];
          break;
        default:
          throw Error('unknown attribute kind', { cause: { kind } });
      }
      tuple[name] = val;
    }
    return kon == 0x4e /*N*/;
  }
  _upgradeMsgTruncate(out) {
    const nrels = this._readInt32();
    out.tag = 'truncate';
    out.flags = this._readUint8();
    out.cascade = Boolean(out.flags & 0b1);
    out.restartIdentity = Boolean(out.flags & 0b10);
    out.relations = this._array(nrels, _ => this._relationCache.get(this._readInt32()));
  }
  _upgradeMsgMessage(out) {
    out.tag = 'message';
    out.flags = this._readUint8();
    out.transactional = Boolean(out.flags & 0b1);
    out.messageLsn = this._readLsn();
    out.prefix = this._readString();
    out.content = this._read(this._readInt32());
  }
  _upgradeMsgCommit(out) {
    out.tag = 'commit';
    out.flags = this._readUint8(); // reserved unused
    // should be the same as begin.commitLsn,
    // postgres somehow uses it to synchronize initial dump with slot position.
    out.commitLsn = this._readLsn();
    // `out.commitEndLsn` is redundant because it always the same as `out.lsn` .
    // Here we see that ctx->write_location = txn->end_lsn
    // https://github.com/postgres/postgres/blob/0a455b8d61d8fc5a7d1fdc152667f9ba1fd27fda/src/backend/replication/logical/logical.c#L819
    // Here we see that ctx->write_location is used for `out.lsn` field.
    // https://github.com/postgres/postgres/blob/0a455b8d61d8fc5a7d1fdc152667f9ba1fd27fda/src/backend/replication/logical/logical.c#L634
    // https://github.com/postgres/postgres/blob/0a455b8d61d8fc5a7d1fdc152667f9ba1fd27fda/src/backend/replication/walsender.c#L1239
    // And here we see that txn->end_lsn is used for `out.commitEndLsn` field.
    // https://github.com/postgres/postgres/blob/0a455b8d61d8fc5a7d1fdc152667f9ba1fd27fda/src/backend/replication/logical/proto.c#L87
    // Seems that they include `out.commitEndLsn` into pgoutput message to simplify `apply_handle_commit` code
    // so it can be independent of `out.lsn`, which is lower level XLogData message field.
    // https://github.com/postgres/postgres/blob/0a455b8d61d8fc5a7d1fdc152667f9ba1fd27fda/src/backend/replication/logical/worker.c#L780
    out.commitEndLsn = this._readLsn();
    out.commitTime = this._readTime();
  }
}

// https://www.postgresql.org/docs/16/protocol-message-formats.html
class FrontendMessage {
  constructor(payload) {
    this.payload = payload;
    // TODO string size depends on encoder
    this.size = MessageSizer.getMessageSize(this);
  }
  get tag() {
    return this.constructor.name; // we will use it only for debug logging
  }
  // https://www.postgresql.org/docs/16/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-STARTUPMESSAGE
  static StartupMessage = class extends this {
    write(w, size, options) {
      w.writeInt32(size);
      w.writeInt32(0x00030000);
      for (const [key, val] of Object.entries(options)) {
        w.writeString(key);
        w.writeString(val);
      }
      w.writeUint8(0);
    }
  };
  // https://www.postgresql.org/docs/16/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-CANCELREQUEST
  static CancelRequest = class extends this {
    write(w, _size, { pid, secretKey }) {
      w.writeInt32(16);
      w.writeInt32(80877102); // (1234 << 16) | 5678
      w.writeInt32(pid);
      w.writeInt32(secretKey);
    }
  };
  // https://www.postgresql.org/docs/16/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-SSLREQUEST
  static SSLRequest = class extends this {
    write(w) {
      w.writeInt32(8);
      w.writeInt32(80877103); // (1234 << 16) | 5679
    }
  };
  // https://www.postgresql.org/docs/16/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-PASSWORDMESSAGE
  static PasswordMessage = class extends this {
    write(w, size, payload) {
      w.writeUint8(0x70); // p
      w.writeInt32(size - 1);
      w.writeString(payload);
    }
  };
  // https://www.postgresql.org/docs/16/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-SASLINITIALRESPONSE
  static SASLInitialResponse = class extends this {
    write(w, size, { mechanism, data }) {
      w.writeUint8(0x70); // p
      w.writeInt32(size - 1);
      w.writeString(mechanism);
      if (data) {
        w.writeInt32(data.byteLength);
        w.write(data);
      } else {
        w.writeInt32(-1);
      }
    }
  };
  // https://www.postgresql.org/docs/16/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-SASLRESPONSE
  static SASLResponse = class extends this {
    write(w, size, data) {
      w.writeUint8(0x70); // p
      w.writeInt32(size - 1);
      w.write(data);
    }
  };
  // https://www.postgresql.org/docs/16/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-QUERY
  static Query = class extends this {
    write(w, size) {
      w.writeUint8(0x51); // Q
      w.writeInt32(size - 1);
      w.writeString(this.payload);
    }
  };
  // https://www.postgresql.org/docs/16/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-PARSE
  static Parse = class extends this {
    write(w, size, { statement, statementName = '', paramTypeOids = [] }) {
      w.writeUint8(0x50); // P
      w.writeInt32(size - 1);
      w.writeString(statementName);
      w.writeString(statement);
      w.writeInt16(paramTypeOids.length);
      for (const typeOid of paramTypeOids) {
        w.writeUint32(typeOid || 0);
      }
    }
  };
  // https://www.postgresql.org/docs/16/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-BIND
  static Bind = class extends this {
    write(w, size, { portal = '', statementName = '', params = [], binary = [] }) {
      w.writeUint8(0x42); // B
      w.writeInt32(size - 1);
      w.writeString(portal);
      w.writeString(statementName);
      w.writeInt16(params.length);
      for (const p of params) {
        w.writeInt16(Number(p instanceof Uint8Array));
      }
      w.writeInt16(params.length);
      for (const p of params) {
        if (p == null) {
          w.writeInt32(-1);
          continue;
        }
        w.writeCounted(p);
      }
      w.writeInt16(binary.length);
      for (const fmt of binary) {
        w.writeInt16(fmt);
      }
    }
  };
  // https://www.postgresql.org/docs/16/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-EXECUTE
  static Execute = class extends this {
    write(w, size, { portal = '', limit = 0 }) {
      w.writeUint8(0x45); // E
      w.writeInt32(size - 1);
      w.writeString(portal);
      w.writeUint32(limit);
    }
  };
  // https://www.postgresql.org/docs/16/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-DESCRIBE
  static DescribeStatement = class extends this {
    write(w, size, statementName = '') {
      w.writeUint8(0x44); // D
      w.writeInt32(size - 1);
      w.writeUint8(0x53); // S
      w.writeString(statementName);
    }
  };
  static DescribePortal = class extends this {
    write(w, size, portal = '') {
      w.writeUint8(0x44); // D
      w.writeInt32(size - 1);
      w.writeUint8(0x50); // P
      w.writeString(portal);
    }
  };
  // https://www.postgresql.org/docs/16/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-CLOSE
  static ClosePortal = class extends this {
    write(w, size, portal = '') {
      w.writeUint8(0x43); // C
      w.writeInt32(size - 1);
      w.writeUint8(0x50); // P
      w.writeString(portal);
    }
  };
  static CloseStatement = class extends this {
    write(w, size, statementName = '') {
      w.writeUint8(0x43); // C
      w.writeInt32(size - 1);
      w.writeUint8(0x53); // S
      w.writeString(statementName);
    }
  };
  // https://www.postgresql.org/docs/16/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-SYNC
  static Sync = class extends this {
    write(w) {
      w.writeUint8(0x53); // S
      w.writeInt32(4);
    }
  };
  // https://www.postgresql.org/docs/16/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-FLUSH
  static Flush = class extends this {
    write(w) {
      w.writeUint8(0x48); // H
      w.writeInt32(4);
    }
  };
  // https://www.postgresql.org/docs/16/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-COPYDATA
  static CopyData = class extends this {
    write(w, size, data) {
      w.writeUint8(0x64); // d
      w.writeInt32(size - 1);
      w.write(data);
    }
  };
  // https://www.postgresql.org/docs/16/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-COPYDONE
  static CopyDone = class extends this {
    write(w) {
      w.writeUint8(0x63); // c
      w.writeInt32(4);
    }
  };
  // https://www.postgresql.org/docs/16/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-COPYFAIL
  static CopyFail = class extends this {
    write(w, size, cause) {
      w.writeUint8(0x66); // f
      w.writeInt32(size - 1);
      w.writeString(cause);
    }
  };
  // https://www.postgresql.org/docs/16/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-TERMINATE
  static Terminate = class extends this {
    write(w) {
      w.writeUint8(0x58); // X
      w.writeInt32(4);
    }
  };
}

class MessageWriter {
  static defaultTextEncoder = new TextEncoder();

  static async writeMessage(socket, m) {
    // TODO too big allocation
    const msgw = new this(socket);
    msgw.writeMessage(m);
    await msgw.flush();
  }

  constructor(socket) {
    this._buf = new Uint8Array(32 * 1024);
    this._v = new DataView(this._buf.buffer);
    this._pos = 0;
    this._textEncoder = MessageWriter.defaultTextEncoder;
    this._socket = socket;
  }
  get length() {
    return this._pos;
  }
  writeMessage(m) {
    this._grow(m.size);
    // TODO zero copy CopyData, Bind
    m.write(this, m.size, m.payload);
  }
  _grow(size) {
    const availableSize = this._buf.length - this._pos;
    if (availableSize >= size) return;
    const newSize = Math.max(this._buf.length * 2, this._pos + size);
    const newBuf = new Uint8Array(newSize);
    newBuf.set(this._buf.subarray(0, this._pos));
    this._buf = newBuf;
    this._v = new DataView(this._buf.buffer);
  }
  async flush() {
    if (!this._pos) return;
    await _net.write(this._socket, this._buf.subarray(0, this._pos));
    this._pos = 0;
  }
  writeUint8(val) {
    this._v.setUint8(this._pos, val);
    this._pos++;
  }
  writeInt16(val) {
    this._v.setInt16(this._pos, val);
    this._pos += 2;
  }
  writeInt32(val) {
    this._v.setInt32(this._pos, val);
    this._pos += 4;
  }
  writeUint32(val) {
    this._v.setUint32(this._pos, val);
    this._pos += 4;
  }
  writeString(val) {
    this.write(val);
    this.writeUint8(0);
  }
  writeCounted(val) {
    const start = this._pos;
    this.writeInt32(-1); // size prefix, will update later
    const size = this.write(val);
    this._v.setInt32(start, size); // update size prefix
  }
  write(val) {
    if (val instanceof Uint8Array) {
      return this._writeUint8Array(val);
    }
    if (typeof val == 'string') {
      return this._writeString(val);
    }
    throw TypeError('string or Uint8Array expected');
  }
  _writeUint8Array(val) {
    this._buf.set(val, this._pos);
    this._pos += val.length;
    return val.length;
  }
  _writeString(val) {
    const tail = this._buf.subarray(this._pos);
    const { read, written } = this._textEncoder.encodeInto(val, tail);
    if (read < val.length) {
      throw Error('too small buffer');
    }
    this._pos += written;
    return written;
  }
}

class MessageSizer {
  static _inst = new this();
  static getMessageSize(m) {
    this._inst.result = 0;
    m.write(this._inst, 0, m.payload);
    return this._inst.result;
  }

  result = 0;
  writeUint8() {
    this.result += 1;
  }
  writeInt16() {
    this.result += 2;
  }
  writeInt32() {
    this.result += 4;
  }
  writeUint32() {
    this.result += 4;
  }
  writeString(val) {
    // do write before zero check to validate val type first
    this.write(val);
    const z = val instanceof Uint8Array ? 0x0 : '\0';
    if (~val.indexOf(z)) {
      throw TypeError('zero char is not allowed');
    }
    this.writeUint8(0);
  }
  writeCounted(val) {
    this.writeInt32(0);
    this.write(val);
  }
  write(val) { // TODO accept this._textEncoder
    if (val instanceof Uint8Array) {
      this.result += val.length;
    } else if (typeof val == 'string') {
      // TODO count length with this._textEncoder
      this.result += this._utf8length(val);
    } else {
      throw TypeError('string or Uint8Array expected');
    }
  }
  _utf8length(s) {
    // TODO still not shure that its correct.
    // May be better use the same aproach as writeCounted:
    // seek back and mutate 'size' prefix instead of
    // compute message size ahead of allocation.
    let result = 0;
    for (let i = 0; i < s.length; i++) {
      const c = s.codePointAt(i);
      if (c > 0xffff) i++, result += 4;
      else if (c > 0x7ff) result += 3;
      else if (c > 0x7f) result += 2;
      else result++;
    }
    return result;
  }
}

class PgType {
  static resolve(oidOrName) {
    if (typeof oidOrName == 'number') { // TODO NaN, Infinity, negative, floats
      return oidOrName;
    }
    switch (oidOrName) { // add line here when register new type
      case 'text'    : return   25; case 'text[]'    : return 1009;
      case 'uuid'    : return 2950; case 'uuid[]'    : return 2951;
      case 'varchar' : return 1043; case 'varchar[]' : return 1015;
      case 'bool'    : return   16; case 'bool[]'    : return 1000;
      case 'bytea'   : return   17; case 'bytea[]'   : return 1001;
      case 'int2'    : return   21; case 'int2[]'    : return 1005;
      case 'int4'    : return   23; case 'int4[]'    : return 1007;
      case 'oid'     : return   26; case 'oid[]'     : return 1028;
      case 'float4'  : return  700; case 'float4[]'  : return 1021;
      case 'float8'  : return  701; case 'float8[]'  : return 1022;
      case 'int8'    : return   20; case 'int8[]'    : return 1016;
      case 'json'    : return  114; case 'json[]'    : return  199;
      case 'jsonb'   : return 3802; case 'jsonb[]'   : return 3807;
      case 'pg_lsn'  : return 3220; case 'pg_lsn[]'  : return 3221;
    }
    throw Error('unknown builtin type name ' + JSON.stringify(oidOrName));
  }
  static encode(value, typeOid) {
    switch (typeOid) { // add line here when register new type (optional)
      case  114 /* json    */:
      case 3802 /* jsonb   */: return JSON.stringify(value);
      case   17 /* bytea   */: return this._encodeBytea(value); // bytea encoder is used only for array element encoding
    }
    const elemTypeid = this._elemTypeOid(typeOid);
    if (elemTypeid) {
      return this._encodeArray(value, elemTypeid);
    }
    return String(value);
  }
  static decode(text, typeOid) {
    switch (typeOid) { // add line here when register new type
      case   25 /* text    */:
      case 2950 /* uuid    */:
      case 1043 /* varchar */: return text;
      case   16 /* bool    */: return text == 't';
      case   17 /* bytea   */: return this._decodeBytea(text);
      case   21 /* int2    */:
      case   23 /* int4    */:
      case   26 /* oid     */:
      case  700 /* float4  */:
      case  701 /* float8  */: return Number(text); // TODO Number.parseFloat ? (fix '' -> 0)
      case   20 /* int8    */: return BigInt(text);
      case  114 /* json    */:
      case 3802 /* jsonb   */: return JSON.parse(text);
      case 3220 /* pg_lsn  */: return lsnMakeComparable(text);
    }
    const elemTypeid = this._elemTypeOid(typeOid);
    if (elemTypeid) {
      return this._decodeArray(text, elemTypeid);
    }
    return text; // unknown type
  }
  static _elemTypeOid(arrayTypeOid) {
    switch (arrayTypeOid) { // add line here when register new type
      case 1009: return   25; // text
      case 1000: return   16; // bool
      case 1001: return   17; // bytea
      case 1005: return   21; // int2
      case 1007: return   23; // int4
      case 1028: return   26; // oid
      case 1021: return  700; // float4
      case 1022: return  701; // float8
      case 1016: return   20; // int8
      case  199: return  114; // json
      case 3807: return 3802; // jsonb
      case 3221: return 3220; // pg_lsn
      case 2951: return 2950; // uuid
      case 1015: return 1043; // varchar
    }
  }
  static _decodeArray(text, elemTypeOid) {
    text = text.replace(/^\[.+=/, ''); // skip dimensions
    let result;
    for (let i = 0, inQuotes = false, elStart = 0, stack = []; i < text.length; i++) {
      const ch = text.charCodeAt(i);
      if (ch == 0x5c /*\*/) i++; // escape
      else if (ch == 0x22 /*"*/) inQuotes = !inQuotes;
      else if (inQuotes); // continue
      else if (ch == 0x7b /*{*/) stack.unshift([]), elStart = i + 1;
      else if (ch == 0x7d /*}*/ || ch == 0x2c /*,*/) { // TODO configurable delimiter
        // TODO ensure .slice is cheap enough to do it unconditionally
        const escaped = text.slice(elStart, i); // TODO trim ' \t\n\r\v\f'
        if (result) {
          stack[0].push(result);
        } else if (/^NULL$/i.test(escaped)) {
          stack[0].push(null);
        } else if (escaped.length) {
          const unescaped = escaped.replace(/^"|"$|(?<!\\)\\/g, '');
          // TODO accept decodeFn as argument,
          // extract parseArray logic out of decoder,
          // do benchmark of static vs dynamic dispatch
          const decoded = this.decode(unescaped, elemTypeOid);
          stack[0].push(decoded);
        }
        result = ch == 0x7d /*}*/ && stack.shift();
        elStart = i + 1; // TODO dry
      }
    }
    return result;
  }
  // TODO multi dimension
  // TODO array_nulls https://www.postgresql.org/docs/16/runtime-config-compatible.html#GUC-ARRAY-NULLS
  static _encodeArray(arr, elemTypeOid) {
    const quoteElem = elem => {
      if (elem == null) return 'NULL';
      elem = this.encode(elem, elemTypeOid);
      elem = elem.replace(/(?=\\|")/g, '\\');
      return `"${elem}"`;
    };
    const delim = ','; // TODO box[] uses semicolon
    return `{${arr.map(quoteElem).join(delim)}}`;
  }
  // https://github.com/postgres/postgres/blob/367c989cd8405663bb9a35ec1aa4f79b673a55ff/src/backend/utils/adt/varlena.c#L290
  static _decodeBytea(/** @type {string} */ text) {
    // https://www.postgresql.org/docs/16/datatype-binary.html#DATATYPE-BINARY-BYTEA-HEX-FORMAT
    if (/^\\x/.test(text)) {
      const hexmap = [
        ,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9,,,,,,,,
        10, 11, 12, 13, 14, 15,,,,,,,,,,,,,,,,,,,,,,,,,,,
        10, 11, 12, 13, 14, 15,
      ];
      text = text.slice(2);
      const bytes = new Uint8Array(text.length >> 1);
      for (let i = 0; i < bytes.length; i++) {
        const h = hexmap[text.charCodeAt(i << 1)];
        const l = hexmap[text.charCodeAt(i << 1 | 1)];
        if (isNaN(h + l)) throw Error('bad encoded bytea received');
        bytes[i] = h << 4 | l;
      }
      return bytes;
    }
    // https://www.postgresql.org/docs/16/datatype-binary.html#DATATYPE-BINARY-BYTEA-ESCAPE-FORMAT
    const utf8enc = new TextEncoder();
    const bytes = utf8enc.encode(text);
    let j = 0;
    for (let i = 0; i < bytes.length;) {
      const b0 = bytes[i++];
      if (b0 != 0x5c) {
        bytes[j++] = b0;
        continue;
      }
      const b1 = bytes[i++];
      if (b1 == 0x5c) {
        bytes[j++] = 0x5c;
        continue;
      }
      const b2 = bytes[i++];
      const b3 = bytes[i++];
      if ((b1 & ~0b11) == 0x30 && (b2 & ~0b111) == 0x30 && (b3 & ~0b111) == 0x30) {
        bytes[j++] = (b1 & 0b11) << 6 | (b2 & 0b111) << 3 | (b3 & 0b111);
        continue;
      }
      throw Error('bad encoded bytea received');
    }
    return bytes.subarray(0, j);
  }
  static _encodeBytea(bytes) {
    return '\\x' + Array.from(bytes, b => b.toString(16).padStart(2, '0')).join('');
  }
}

function lsnMakeComparable(text) {
  const [h, l] = text.toUpperCase().split('/');
  return h.padStart(8, '0') + '/' + l.padStart(8, '0');
}

function millis(inp) {
  if (typeof inp == 'number') return inp;
  if (typeof inp != 'string') return;
  inp = inp.trim().toLowerCase();
  const [unit] = /[a-z]*$/i.exec(inp);
  const num = Number.parseFloat(inp);
  if (!unit || unit == 'ms') return num;
  if (unit == 's') return num * 1000;
  if (unit == 'min') return num * 1000 * 60;
  if (unit == 'h') return num * 1000 * 60 * 60;
  if (unit == 'd') return num * 1000 * 60 * 60 * 24;
}

class Channel {
  constructor() {
    this._qnext = [];
    this._qpush = [];
    this._value = undefined;
    this._pausePushAsync = resolve => this._qpush.push({ done: false, value: this._value, resolve });
    this._pauseNextAsync = resolve => this._qnext.push(resolve);
    this._resolveEnded = null;
    this._whenEnded = new Promise(resolve => this._resolveEnded = resolve);
  }
  /** `true` if subsequent .push will immediately pass value to awaiting .next  */
  get drained() {
    return Boolean(this._qnext?.length);
  }
  async push(value) {
    if (!this._qnext) throw Error('push after ended');
    const resumeNext = this._qnext.shift();
    if (resumeNext) return resumeNext({ done: false, value });
    if (!this._qpush) return; // iterator returned
    this._value = value;
    await new Promise(this._pausePushAsync);
  }
  end(value) {
    if (!this._qnext) throw Error('already ended');
    this._value = value;
    for (const resumeNext of this._qnext) {
      resumeNext({ done: true, value });
      // TODO return { value: undefined }
    }
    this._qnext = null; // do not pull anymore
    this._resolveEnded();
  }
  async next() {
    if (!this._qpush) return { done: true, value: undefined };
    const unpushed = this._qpush.shift();
    if (unpushed) return unpushed.resolve(), unpushed;
    if (!this._qnext) return { done: true, value: this._value };
    return new Promise(this._pauseNextAsync);
  }
  async return(value) {
    if (this._qpush) {
      this._qpush.forEach(it => it.resolve());
      this._qpush = null; // do not push anymore
    }
    await this._whenEnded;
    return { done: true, value };
  }
  [Symbol.asyncIterator]() {
    return this;
  }
}

// https://www.postgresql.org/docs/14/sasl-authentication.html
// https://datatracker.ietf.org/doc/html/rfc5802#section-3
/** @internal */
export class SaslScramSha256 {
  _clientFirstMessageBare;
  _serverSignatureB64;

  async start() {
    const clientNonce = this._randomBytes(24);
    this._clientFirstMessageBare = 'n=,r=' + this._b64encode(clientNonce);
    const utf8enc = new TextEncoder();
    return utf8enc.encode('n,,' + this._clientFirstMessageBare);
  }
  async continue(serverFirstMessageUtf8, password) {
    const utf8dec = new TextDecoder();
    const serverFirstMessage = utf8dec.decode(serverFirstMessageUtf8);
    const utf8enc = new TextEncoder();
    const { 'i': iterations, 's': saltB64, 'r': nonceB64 } = this._parseMsg(serverFirstMessage);
    const finalMessageWithoutProof = 'c=biws,r=' + nonceB64;
    const salt = this._b64decode(saltB64);
    const passwordUtf8 = utf8enc.encode(password.normalize());
    const saltedPassword = await this._hi(passwordUtf8, salt, +iterations);
    const clientKey = await this._hmac(saltedPassword, utf8enc.encode('Client Key'));
    const storedKey = await this._hash(clientKey);
    const authMessage = utf8enc.encode(
      this._clientFirstMessageBare + ',' +
      serverFirstMessage + ',' +
      finalMessageWithoutProof
    );
    const clientSignature = await this._hmac(storedKey, authMessage);
    const clientProof = xor(clientKey, clientSignature);
    const serverKey = await this._hmac(saltedPassword, utf8enc.encode('Server Key'));
    const serverSignature = await this._hmac(serverKey, authMessage);
    this._serverSignatureB64 = this._b64encode(serverSignature);
    return utf8enc.encode(finalMessageWithoutProof + ',p=' + this._b64encode(clientProof));

    function xor(a, b) {
      return Uint8Array.from(a, (ai, i) => ai ^ b[i]);
    }
  }
  finish(responseUtf8) {
    const utf8dec = new TextDecoder();
    const response = utf8dec.decode(responseUtf8);
    if (!this._serverSignatureB64) {
      throw Error('unexpected auth finish');
    }
    const { 'v': receivedServerSignatureB64 } = this._parseMsg(response);
    if (this._serverSignatureB64 != receivedServerSignatureB64) {
      throw Error('invalid server SCRAM signature');
    }
  }
  _parseMsg(msg) { // parses `key1=val,key2=val` into object
    return Object.fromEntries(msg.split(',').map(it => /^(.*?)=(.*)$/.exec(it).slice(1)));
  }

  _b64encode(bytes) {
    return btoa(String.fromCharCode(...bytes));
  }
  _b64decode(b64) {
    return Uint8Array.from(atob(b64), x => x.charCodeAt());
  }
  _randomBytes(n) {
    return crypto.getRandomValues(new Uint8Array(n));
  }
  async _hash(val) {
    return new Uint8Array(await crypto.subtle.digest('SHA-256', val));
  }
  async _hmac(key, msg) {
    // https://en.wikipedia.org/wiki/HMAC
    const bskey = new Uint8Array(64);
    bskey.set(key.length > bskey.length ? await this._hash(key) : key);
    const inner = await this._hash(Uint8Array.of(...bskey.map(x => 0x36 ^ x), ...msg));
    return await this._hash(Uint8Array.of(...bskey.map(x => 0x5c ^ x), ...inner));
  }
  async _hi(pwd, salt, iterations) {
    const cryptoKey = await crypto.subtle.importKey('raw', pwd, 'PBKDF2', false, ['deriveBits']);
    const pbkdf2params = { name: 'PBKDF2', hash: 'SHA-256', salt, iterations };
    const buf = await crypto.subtle.deriveBits(pbkdf2params, cryptoKey, 32 * 8);
    return new Uint8Array(buf);
  }
}

function md5(/** @type {Uint8Array} */ input) {
  const padded = new Uint8Array(Math.ceil((input.byteLength + 1 + 8) / 64) * 64);
  const paddedv = new DataView(padded.buffer);
  padded.set(input);
  paddedv.setUint8(input.byteLength, 0b10000000);
  paddedv.setBigUint64(paddedv.byteLength - 8, BigInt(input.byteLength) * 8n, true);

  let a0 = 0x67452301;
  let b0 = 0xefcdab89;
  let c0 = 0x98badcfe;
  let d0 = 0x10325476;
  const m = Array(16);

  for (let i = 0; i < paddedv.byteLength; i += 64) {
    let a = a0, b = b0, c = c0, d = d0;
    for (let j = 0; j < 16; j++) {
      m[j] = paddedv.getUint32(i + j * 4, true);
    }

    // round 1
    a = b + rol32((((c ^ d) & b) ^ d) + a + m[0x0] + 0xd76aa478, 7);
    d = a + rol32((((b ^ c) & a) ^ c) + d + m[0x1] + 0xe8c7b756, 12);
    c = d + rol32((((a ^ b) & d) ^ b) + c + m[0x2] + 0x242070db, 17);
    b = c + rol32((((d ^ a) & c) ^ a) + b + m[0x3] + 0xc1bdceee, 22);
    a = b + rol32((((c ^ d) & b) ^ d) + a + m[0x4] + 0xf57c0faf, 7);
    d = a + rol32((((b ^ c) & a) ^ c) + d + m[0x5] + 0x4787c62a, 12);
    c = d + rol32((((a ^ b) & d) ^ b) + c + m[0x6] + 0xa8304613, 17);
    b = c + rol32((((d ^ a) & c) ^ a) + b + m[0x7] + 0xfd469501, 22);
    a = b + rol32((((c ^ d) & b) ^ d) + a + m[0x8] + 0x698098d8, 7);
    d = a + rol32((((b ^ c) & a) ^ c) + d + m[0x9] + 0x8b44f7af, 12);
    c = d + rol32((((a ^ b) & d) ^ b) + c + m[0xa] + 0xffff5bb1, 17);
    b = c + rol32((((d ^ a) & c) ^ a) + b + m[0xb] + 0x895cd7be, 22);
    a = b + rol32((((c ^ d) & b) ^ d) + a + m[0xc] + 0x6b901122, 7);
    d = a + rol32((((b ^ c) & a) ^ c) + d + m[0xd] + 0xfd987193, 12);
    c = d + rol32((((a ^ b) & d) ^ b) + c + m[0xe] + 0xa679438e, 17);
    b = c + rol32((((d ^ a) & c) ^ a) + b + m[0xf] + 0x49b40821, 22);

    // round 2
    a = b + rol32((((b ^ c) & d) ^ c) + a + m[0x1] + 0xf61e2562, 5);
    d = a + rol32((((a ^ b) & c) ^ b) + d + m[0x6] + 0xc040b340, 9);
    c = d + rol32((((d ^ a) & b) ^ a) + c + m[0xb] + 0x265e5a51, 14);
    b = c + rol32((((c ^ d) & a) ^ d) + b + m[0x0] + 0xe9b6c7aa, 20);
    a = b + rol32((((b ^ c) & d) ^ c) + a + m[0x5] + 0xd62f105d, 5);
    d = a + rol32((((a ^ b) & c) ^ b) + d + m[0xa] + 0x02441453, 9);
    c = d + rol32((((d ^ a) & b) ^ a) + c + m[0xf] + 0xd8a1e681, 14);
    b = c + rol32((((c ^ d) & a) ^ d) + b + m[0x4] + 0xe7d3fbc8, 20);
    a = b + rol32((((b ^ c) & d) ^ c) + a + m[0x9] + 0x21e1cde6, 5);
    d = a + rol32((((a ^ b) & c) ^ b) + d + m[0xe] + 0xc33707d6, 9);
    c = d + rol32((((d ^ a) & b) ^ a) + c + m[0x3] + 0xf4d50d87, 14);
    b = c + rol32((((c ^ d) & a) ^ d) + b + m[0x8] + 0x455a14ed, 20);
    a = b + rol32((((b ^ c) & d) ^ c) + a + m[0xd] + 0xa9e3e905, 5);
    d = a + rol32((((a ^ b) & c) ^ b) + d + m[0x2] + 0xfcefa3f8, 9);
    c = d + rol32((((d ^ a) & b) ^ a) + c + m[0x7] + 0x676f02d9, 14);
    b = c + rol32((((c ^ d) & a) ^ d) + b + m[0xc] + 0x8d2a4c8a, 20);

    // round 3
    a = b + rol32((b ^ c ^ d) + a + m[0x5] + 0xfffa3942, 4);
    d = a + rol32((a ^ b ^ c) + d + m[0x8] + 0x8771f681, 11);
    c = d + rol32((d ^ a ^ b) + c + m[0xb] + 0x6d9d6122, 16);
    b = c + rol32((c ^ d ^ a) + b + m[0xe] + 0xfde5380c, 23);
    a = b + rol32((b ^ c ^ d) + a + m[0x1] + 0xa4beea44, 4);
    d = a + rol32((a ^ b ^ c) + d + m[0x4] + 0x4bdecfa9, 11);
    c = d + rol32((d ^ a ^ b) + c + m[0x7] + 0xf6bb4b60, 16);
    b = c + rol32((c ^ d ^ a) + b + m[0xa] + 0xbebfbc70, 23);
    a = b + rol32((b ^ c ^ d) + a + m[0xd] + 0x289b7ec6, 4);
    d = a + rol32((a ^ b ^ c) + d + m[0x0] + 0xeaa127fa, 11);
    c = d + rol32((d ^ a ^ b) + c + m[0x3] + 0xd4ef3085, 16);
    b = c + rol32((c ^ d ^ a) + b + m[0x6] + 0x04881d05, 23);
    a = b + rol32((b ^ c ^ d) + a + m[0x9] + 0xd9d4d039, 4);
    d = a + rol32((a ^ b ^ c) + d + m[0xc] + 0xe6db99e5, 11);
    c = d + rol32((d ^ a ^ b) + c + m[0xf] + 0x1fa27cf8, 16);
    b = c + rol32((c ^ d ^ a) + b + m[0x2] + 0xc4ac5665, 23);

    // round 4
    a = b + rol32((c ^ (b | ~d)) + a + m[0x0] + 0xf4292244, 6);
    d = a + rol32((b ^ (a | ~c)) + d + m[0x7] + 0x432aff97, 10);
    c = d + rol32((a ^ (d | ~b)) + c + m[0xe] + 0xab9423a7, 15);
    b = c + rol32((d ^ (c | ~a)) + b + m[0x5] + 0xfc93a039, 21);
    a = b + rol32((c ^ (b | ~d)) + a + m[0xc] + 0x655b59c3, 6);
    d = a + rol32((b ^ (a | ~c)) + d + m[0x3] + 0x8f0ccc92, 10);
    c = d + rol32((a ^ (d | ~b)) + c + m[0xa] + 0xffeff47d, 15);
    b = c + rol32((d ^ (c | ~a)) + b + m[0x1] + 0x85845dd1, 21);
    a = b + rol32((c ^ (b | ~d)) + a + m[0x8] + 0x6fa87e4f, 6);
    d = a + rol32((b ^ (a | ~c)) + d + m[0xf] + 0xfe2ce6e0, 10);
    c = d + rol32((a ^ (d | ~b)) + c + m[0x6] + 0xa3014314, 15);
    b = c + rol32((d ^ (c | ~a)) + b + m[0xd] + 0x4e0811a1, 21);
    a = b + rol32((c ^ (b | ~d)) + a + m[0x4] + 0xf7537e82, 6);
    d = a + rol32((b ^ (a | ~c)) + d + m[0xb] + 0xbd3af235, 10);
    c = d + rol32((a ^ (d | ~b)) + c + m[0x2] + 0x2ad7d2bb, 15);
    b = c + rol32((d ^ (c | ~a)) + b + m[0x9] + 0xeb86d391, 21);

    a0 = (a0 + a) >>> 0;
    b0 = (b0 + b) >>> 0;
    c0 = (c0 + c) >>> 0;
    d0 = (d0 + d) >>> 0;
  }

  const hash = new Uint8Array(16);
  const hashv = new DataView(hash.buffer);
  hashv.setUint32(0, a0, true);
  hashv.setUint32(4, b0, true);
  hashv.setUint32(8, c0, true);
  hashv.setUint32(12, d0, true);
  return hash;

  function rol32(x, n) {
    return (x << n) | (x >>> (32 - n));
  }
}

/** @internal */
export const _net = {
  connect(options) {
    return options.path ? this.connectUnix(options) : this.connectTcp(options);
  },
  async connectTcp({ host, port, keepAlive }) {
    const socket = await Deno.connect({ hostname: host, port });
    if (keepAlive) {
      socket.setKeepAlive(keepAlive); // TODO should close socket on error?
    }
    return socket;
  },
  async connectUnix({ path }) {
    return await Deno.connect({ transport: 'unix', path });
  },
  reconnectable(err) {
    return (
      err instanceof Deno.errors.ConnectionRefused ||
      err instanceof Deno.errors.ConnectionReset
    );
  },
  async startTls(socket, options) {
    const tlssock = await Deno.startTls(socket, options);
    try {
      await tlssock.handshake();
    } catch (ex) {
      tlssock.close();
      throw ex;
    }
    return tlssock;
  },
  async write(socket, arr) {
    let nwritten = 0;
    while (nwritten < arr.length) {
      nwritten += await socket.write(arr.subarray(nwritten));
    }
  },
  read(socket, buf) {
    return socket.read(buf);
  },
  closeNullable(socket) {
    if (!socket) return;
    try {
      socket.close();
    } catch (ex) {
      if (ex instanceof Deno.errors.BadResource) return; // already closed
      throw ex;
    }
  },
};


/** @typedef {ArrayLike & Iterable & { raw: (string | Uint8Array | null)[] }} Row */
/** @typedef {ReturnType<typeof MessageReader.prototype._readRowDescription>[0]} ColumnDescription */
/** @typedef {ReturnType<typeof MessageReader.prototype._readErrorOrNotice>} ErrorOrNotice */
