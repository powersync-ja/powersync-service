import { randomUUID } from 'node:crypto';
import * as http from 'node:http';
import * as inspector from 'node:inspector';
import { Session } from 'node:inspector/promises';
import type { AddressInfo } from 'node:net';
import { basename } from 'node:path';
import { pathToFileURL } from 'node:url';
import * as WebSocket from 'ws';
import { getMetadataTraceEvents, traceEvents, type TraceEvent } from './TraceWriter.js';

type JsonObject = Record<string, any>;

interface CdpRequest {
  id?: number;
  method?: string;
  params?: JsonObject;
}

interface MessageContext {
  tracingActive: boolean;
  traceDomain: 'Tracing' | 'NodeTracing';
  profilerActive: boolean;
  setTracingActive(active: boolean): void;
  setTraceDomain(domain: 'Tracing' | 'NodeTracing'): void;
  setProfilerActive(active: boolean): void;
  resetProfilerEvents(startTime: number): void;
  stopProfiler(): JsonObject;
  sendTraceEvents(events: TraceEvent[]): void;
  sendTracingComplete(): void;
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
    let tracingActive = false;
    let traceDomain: 'Tracing' | 'NodeTracing' = 'Tracing';
    let profilerActive = false;
    let profilerStartTime = nowMicros();
    let profilerEvents: TraceEvent[] = [];

    const sendEvent = (method: string, params: JsonObject = {}) => {
      if (ws.readyState == WebSocket.WebSocket.OPEN) {
        ws.send(JSON.stringify({ method, params }));
      }
    };

    const onInternalTraceEvents = (events: TraceEvent[]) => {
      const validEvents = events.filter(isChromeTraceEvent);
      if (events.length != validEvents.length) {
        console.warn(`Dropped ${events.length - validEvents.length} invalid PowerSync trace event(s)`);
      }
      if (tracingActive && validEvents.length > 0) {
        sendEvent(`${traceDomain}.dataCollected`, { value: validEvents });
      }
      if (profilerActive && validEvents.length > 0) {
        profilerEvents.push(...validEvents.filter((event) => event.ph == 'X'));
      }
    };
    const onInspectorNotification = (message: any) => {
      if (typeof message?.method == 'string' && message.method.startsWith('NodeTracing.')) {
        return;
      }

      if (ws.readyState == WebSocket.WebSocket.OPEN) {
        ws.send(JSON.stringify(message));
      }
    };
    const onTracingComplete = () => {
      sendEvent(`${traceDomain}.tracingComplete`, { dataLossOccurred: false });
    };

    traceEvents.on('events', onInternalTraceEvents);
    inspectorSession.on('inspectorNotification', onInspectorNotification);

    ws.on('message', (raw) => {
      handleMessage(
        inspectorSession,
        traceConfig,
        {
          get tracingActive() {
            return tracingActive;
          },
          get traceDomain() {
            return traceDomain;
          },
          get profilerActive() {
            return profilerActive;
          },
          setTracingActive(active) {
            tracingActive = active;
          },
          setTraceDomain(domain) {
            traceDomain = domain;
          },
          setProfilerActive(active) {
            profilerActive = active;
          },
          resetProfilerEvents(startTime) {
            profilerStartTime = startTime;
            profilerEvents = [];
          },
          stopProfiler() {
            profilerActive = false;
            return { profile: traceEventsToCpuProfile(profilerEvents, profilerStartTime, nowMicros()) };
          },
          sendTraceEvents(events) {
            onInternalTraceEvents(events);
          },
          sendTracingComplete() {
            onTracingComplete();
          }
        },
        raw
      )
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
      traceEvents.off('events', onInternalTraceEvents);
      inspectorSession.off('inspectorNotification', onInspectorNotification);
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
  context: MessageContext,
  raw: WebSocket.RawData
) {
  const request = JSON.parse(raw.toString()) as CdpRequest;

  if (typeof request.id != 'number' || typeof request.method != 'string') {
    return protocolError(request.id, new Error('Expected a CDP request with numeric id and string method'));
  }

  try {
    const result = await handleMethod(session, traceConfig, context, request.method, request.params ?? {});
    return { id: request.id, result };
  } catch (error) {
    return protocolError(request.id, error);
  }
}

async function handleMethod(
  session: Session,
  traceConfig: BasicCdpTraceServerOptions['traceConfig'],
  context: MessageContext,
  method: string,
  params: JsonObject
) {
  switch (method) {
    case 'Tracing.start':
    case 'NodeTracing.start':
      context.setTraceDomain(method.startsWith('NodeTracing.') ? 'NodeTracing' : 'Tracing');
      context.setTracingActive(true);
      context.sendTraceEvents(getMetadataTraceEvents());
      return {};
    case 'Tracing.end':
    case 'NodeTracing.stop':
      context.setTracingActive(false);
      context.sendTracingComplete();
      return {};
    case 'Tracing.getCategories':
    case 'NodeTracing.getCategories':
      return { categories: ['powersync', '__metadata'] };
    case 'Profiler.enable':
    case 'Profiler.disable':
    case 'Profiler.setSamplingInterval':
      return {};
    case 'Profiler.start':
      context.resetProfilerEvents(nowMicros());
      context.setProfilerActive(true);
      return {};
    case 'Profiler.stop':
      return context.stopProfiler();
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

function isChromeTraceEvent(event: TraceEvent) {
  if (typeof event.name != 'string' || typeof event.ph != 'string') {
    return false;
  }

  if (!isInteger(event.pid) || !isInteger(event.tid)) {
    return false;
  }

  switch (event.ph) {
    case 'M':
      return typeof event.args == 'object' && event.args != null;
    case 'X':
      return typeof event.cat == 'string' && isFiniteNumber(event.ts) && isFiniteNumber(event.dur);
    default:
      return isFiniteNumber(event.ts);
  }
}

function isInteger(value: unknown): value is number {
  return Number.isInteger(value);
}

function isFiniteNumber(value: unknown): value is number {
  return typeof value == 'number' && Number.isFinite(value);
}

function traceEventsToCpuProfile(events: TraceEvent[], startTime: number, endTime: number) {
  const sortedEvents = events
    .filter((event) => event.ph == 'X' && isFiniteNumber(event.ts) && isFiniteNumber(event.dur))
    .sort((a, b) => a.ts - b.ts || b.dur - a.dur);

  const nodes: JsonObject[] = [
    {
      id: 1,
      callFrame: {
        functionName: 'PowerSync',
        scriptId: '0',
        url: 'powersync://profile',
        lineNumber: 0,
        columnNumber: 0
      },
      children: []
    }
  ];
  const samples: number[] = [];
  const timeDeltas: number[] = [];
  const stacksByThread = new Map<number, Array<{ event: TraceEvent; nodeId: number; endTime: number }>>();
  let previousTimestamp = startTime;

  for (const event of sortedEvents) {
    const stack = stacksByThread.get(event.tid) ?? [];
    stacksByThread.set(event.tid, stack);

    while (stack.length > 0 && stack[stack.length - 1].endTime <= event.ts) {
      stack.pop();
    }

    const parentId = stack[stack.length - 1]?.nodeId ?? 1;
    const nodeId = addProfileNode(event, parentId, nodes);
    stack.push({ event, nodeId, endTime: event.ts + event.dur });

    samples.push(nodeId);
    timeDeltas.push(Math.max(1, Math.round(event.ts - previousTimestamp || event.dur || 1)));
    previousTimestamp = event.ts;
  }

  if (samples.length == 0) {
    samples.push(1);
    timeDeltas.push(Math.max(1, endTime - startTime));
  }

  return {
    nodes,
    startTime,
    endTime: Math.max(endTime, previousTimestamp),
    samples,
    timeDeltas
  };
}

function addProfileNode(event: TraceEvent, parentId: number, nodes: JsonObject[]) {
  const id = nodes.length + 1;
  const parent = nodes[parentId - 1];
  parent.children ??= [];
  parent.children.push(id);
  nodes.push({
    id,
    callFrame: {
      functionName: event.name,
      scriptId: '0',
      url: `powersync://${event.tid}/${event.cat}`,
      lineNumber: 0,
      columnNumber: 0
    },
    hitCount: 1
  });
  return id;
}

function nowMicros() {
  return Number(process.hrtime.bigint() / 1000n);
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
