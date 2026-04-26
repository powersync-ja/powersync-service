import { randomUUID } from 'node:crypto';
import * as http from 'node:http';
import * as inspector from 'node:inspector';
import { Session } from 'node:inspector/promises';
import type { AddressInfo } from 'node:net';
import { basename } from 'node:path';
import { pathToFileURL } from 'node:url';
import * as WebSocket from 'ws';

type JsonObject = Record<string, any>;

interface CdpRequest {
  id?: number;
  method?: string;
  params?: JsonObject;
}

export interface BasicCdpTraceServerOptions {
  host?: string;
  port?: number;
  path?: string;
  traceConfig?: {
    recordMode?: string;
    includedCategories: string[];
  };
}

export interface BasicCdpTraceServer {
  readonly targetId: string;
  readonly url: string;
  readonly webSocketDebuggerUrl: string;
  close(): Promise<void>;
}

const DEFAULT_TRACE_CONFIG = {
  recordMode: 'recordContinuously',
  includedCategories: ['node', 'node.async_hooks', 'node.perf', 'node.perf.usertiming', 'v8']
};

export async function startBasicCdpTraceServer(options: BasicCdpTraceServerOptions = {}): Promise<BasicCdpTraceServer> {
  const host = options.host ?? '127.0.0.1';
  const targetId = randomUUID();
  const wsPath = options.path ?? `/${targetId}`;
  const traceConfig = options.traceConfig ?? DEFAULT_TRACE_CONFIG;
  let protocolDescriptor: unknown | undefined;

  let baseUrl = '';
  let webSocketDebuggerUrl = '';

  const inspectorSession = new Session();
  inspectorSession.connect();

  const wss = new WebSocket.WebSocketServer({ noServer: true });

  const server = http.createServer((request, response) => {
    const url = new URL(request.url ?? '/', baseUrl);
    const requestBaseUrl = httpBaseUrl(request, baseUrl);
    const requestWebSocketDebuggerUrl = webSocketUrl(request, wsPath, webSocketDebuggerUrl);

    if (url.pathname == '/json/version') {
      return sendJson(response, {
        Browser: `node.js/${process.version}`,
        'Protocol-Version': '1.1'
      });
    }

    if (url.pathname == '/json' || url.pathname == '/json/list') {
      return sendJson(response, [targetDescriptor(targetId, requestBaseUrl, requestWebSocketDebuggerUrl)]);
    }

    if (url.pathname == '/json/protocol') {
      getProtocolDescriptor(protocolDescriptor)
        .then((descriptor) => {
          protocolDescriptor = descriptor;
          sendJson(response, descriptor);
        })
        .catch((error) => {
          response.writeHead(500, { 'Content-Type': 'text/plain; charset=UTF-8' });
          response.end(error?.message ?? String(error));
        });
      return;
    }

    response.writeHead(404).end();
  });

  server.on('upgrade', (request, socket, head) => {
    const url = new URL(request.url ?? '/', baseUrl);
    if (url.pathname != wsPath) {
      socket.destroy();
      return;
    }

    wss.handleUpgrade(request, socket, head, (ws) => {
      wss.emit('connection', ws, request);
    });
  });

  wss.on('connection', (ws) => {
    const sendEvent = (method: string, params: JsonObject = {}) => {
      if (ws.readyState == WebSocket.WebSocket.OPEN) {
        console.log(JSON.stringify({ method, params }));
        ws.send(JSON.stringify({ method, params }));
      }
    };

    const onInspectorNotification = (message: any) => {
      if (ws.readyState == WebSocket.WebSocket.OPEN) {
        ws.send(JSON.stringify(message));
      }
    };
    const onDataCollected = (message: any) => {
      sendEvent('Tracing.dataCollected', message.params);
    };
    const onTracingComplete = () => {
      sendEvent('Tracing.tracingComplete', { dataLossOccurred: false });
    };

    inspectorSession.on('inspectorNotification', onInspectorNotification);
    inspectorSession.on('NodeTracing.dataCollected', onDataCollected);
    inspectorSession.on('NodeTracing.tracingComplete', onTracingComplete);

    ws.on('message', (raw) => {
      handleMessage(inspectorSession, traceConfig, raw)
        .then((response) => {
          if (response && ws.readyState == WebSocket.WebSocket.OPEN) {
            ws.send(JSON.stringify(response));
          }
        })
        .catch((error) => {
          if (ws.readyState == WebSocket.WebSocket.OPEN) {
            ws.send(JSON.stringify(protocolError(undefined, error)));
          }
        });
    });

    ws.on('close', () => {
      inspectorSession.off('inspectorNotification', onInspectorNotification);
      inspectorSession.off('NodeTracing.dataCollected', onDataCollected);
      inspectorSession.off('NodeTracing.tracingComplete', onTracingComplete);
    });
  });

  await listen(server, options.port ?? 9222, host);

  const address = server.address() as AddressInfo;
  baseUrl = `http://${address.address}:${address.port}`;
  webSocketDebuggerUrl = `ws://${address.address}:${address.port}${wsPath}`;

  return {
    targetId,
    get url() {
      return baseUrl;
    },
    get webSocketDebuggerUrl() {
      return webSocketDebuggerUrl;
    },
    async close() {
      await Promise.all([closeWebSocketServer(wss), closeServer(server)]);
      inspectorSession.disconnect();
    }
  };
}

async function handleMessage(
  session: Session,
  traceConfig: BasicCdpTraceServerOptions['traceConfig'],
  raw: WebSocket.RawData
) {
  const request = JSON.parse(raw.toString()) as CdpRequest;

  if (typeof request.id != 'number' || typeof request.method != 'string') {
    return protocolError(request.id, new Error('Expected a CDP request with numeric id and string method'));
  }

  try {
    const result = await handleMethod(session, traceConfig, request.method, request.params ?? {});
    return { id: request.id, result };
  } catch (error) {
    return protocolError(request.id, error);
  }
}

async function handleMethod(
  session: Session,
  traceConfig: BasicCdpTraceServerOptions['traceConfig'],
  method: string,
  params: JsonObject
) {
  switch (method) {
    case 'Tracing.start':
      await session.post('NodeTracing.start', { traceConfig: params.traceConfig ?? traceConfig });
      return {};
    case 'Tracing.end':
      await session.post('NodeTracing.stop');
      return {};
    case 'Tracing.getCategories':
      return session.post('NodeTracing.getCategories');
    case 'Schema.getDomains':
      return {
        domains: [
          { name: 'Tracing', version: '1.1' },
          { name: 'Schema', version: '1.1' }
        ]
      };
    default:
      return session.post(method as any, params);
  }
}

function targetDescriptor(targetId: string, baseUrl: string, webSocketDebuggerUrl: string) {
  const frontendWebSocketUrl = webSocketDebuggerUrl.replace(/^ws:\/\//, '');
  const entrypoint = process.argv[1];

  return {
    id: targetId,
    type: 'node',
    title: entrypoint ? basename(entrypoint) : 'PowerSync trace target',
    description: 'node.js instance',
    url: entrypoint ? pathToFileURL(entrypoint).href : baseUrl,
    webSocketDebuggerUrl,
    faviconUrl: 'https://nodejs.org/static/images/favicons/favicon.ico',
    devtoolsFrontendUrl: `devtools://devtools/bundled/js_app.html?experiments=true&v8only=true&ws=${frontendWebSocketUrl}`,
    devtoolsFrontendUrlCompat: `devtools://devtools/bundled/inspector.html?experiments=true&v8only=true&ws=${frontendWebSocketUrl}`
  };
}

function httpBaseUrl(request: http.IncomingMessage, fallback: string) {
  const host = request.headers.host;
  return host ? `http://${host}` : fallback;
}

function webSocketUrl(request: http.IncomingMessage, path: string, fallback: string) {
  const host = request.headers.host;
  return host ? `ws://${host}${path}` : fallback;
}

async function getProtocolDescriptor(cached: unknown | undefined) {
  if (cached) {
    return cached;
  }

  const ownInspectorUrl = inspector.url();
  if (ownInspectorUrl) {
    const url = new URL(ownInspectorUrl);
    url.protocol = 'http:';
    url.pathname = '/json/protocol';
    url.search = '';

    const response = await fetch(url);
    if (response.ok) {
      return response.json();
    }
  }

  return {
    version: { major: '1', minor: '0' },
    domains: [
      { domain: 'Runtime', commands: [{ name: 'enable' }, { name: 'disable' }, { name: 'evaluate' }] },
      { domain: 'Debugger', commands: [{ name: 'enable' }, { name: 'disable' }] },
      { domain: 'Profiler', commands: [{ name: 'enable' }, { name: 'disable' }] },
      {
        domain: 'Tracing',
        commands: [{ name: 'start' }, { name: 'end' }, { name: 'getCategories' }],
        events: [{ name: 'dataCollected' }, { name: 'tracingComplete' }]
      },
      { domain: 'Schema', commands: [{ name: 'getDomains' }] }
    ]
  };
}

function sendJson(response: http.ServerResponse, value: unknown) {
  const body = JSON.stringify(value, null, 2);
  response.writeHead(200, {
    'Content-Type': 'application/json; charset=UTF-8',
    'Cache-Control': 'no-cache',
    'Content-Length': Buffer.byteLength(body)
  });
  response.end(body);
}

function protocolError(id: number | undefined, error: any) {
  return {
    id,
    error: {
      code: -32000,
      message: error?.message ?? String(error)
    }
  };
}

function listen(server: http.Server, port: number, host: string) {
  return new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.listen(port, host, () => {
      server.off('error', reject);
      resolve();
    });
  });
}

function closeServer(server: http.Server) {
  return new Promise<void>((resolve, reject) => {
    server.close((error) => {
      if (error) {
        reject(error);
      } else {
        resolve();
      }
    });
  });
}

function closeWebSocketServer(server: WebSocket.WebSocketServer) {
  return new Promise<void>((resolve, reject) => {
    server.close((error) => {
      if (error) {
        reject(error);
      } else {
        resolve();
      }
    });
  });
}
