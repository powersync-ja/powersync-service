import * as http from 'node:http';
import { Session } from 'node:inspector/promises';
import type { AddressInfo } from 'node:net';
import type { Protocol } from 'devtools-protocol';
import type { ProtocolMapping } from 'devtools-protocol/types/protocol-mapping.js';
import browserProtocol from 'devtools-protocol/json/browser_protocol.json' with { type: 'json' };
import jsProtocol from 'devtools-protocol/json/js_protocol.json' with { type: 'json' };
import * as WebSocket from 'ws';
import pkg from '../../package.json' with { type: 'json' };
import { getMetadataTraceEvents, traceEvents, type TraceEvent } from './TraceWriter.js';

type JsonObject = Record<string, unknown>;
type CdpMethod = keyof ProtocolMapping.Commands;
type CdpEvent = keyof ProtocolMapping.Events;
type NodeTracingMethod = 'NodeTracing.start' | 'NodeTracing.stop' | 'NodeTracing.getCategories';
type TraceDomain = 'Tracing' | 'NodeTracing';
type TraceTransferMode = 'ReportEvents' | 'ReturnAsStream';
interface ProtocolDescriptor {
  version: { major: string; minor: string };
  domains: Array<{ domain: string } & Record<string, unknown>>;
}

interface CdpRequest {
  id?: number;
  method?: CdpMethod | NodeTracingMethod | string;
  params?: JsonObject;
}

interface MessageContext {
  tracingActive: boolean;
  traceDomain: TraceDomain;
  profilerActive: boolean;
  startTracing(params: Protocol.Tracing.StartRequest): Promise<void>;
  stopTracing(): Promise<void>;
  setTraceDomain(domain: TraceDomain): void;
  setProfilerActive(active: boolean): void;
  resetProfilerEvents(startTime: number): void;
  stopProfiler(): Protocol.Profiler.StopResponse;
  sendTraceEvents(events: TraceEvent[]): void;
  sendTracingComplete(): void;
  readStream(params: Protocol.IO.ReadRequest): Protocol.IO.ReadResponse;
  closeStream(params: Protocol.IO.CloseRequest): void;
}

interface NodeCpuProfileResult {
  profile: Protocol.Profiler.Profile;
  traceClockEndTime: number;
}

export interface BasicCdpTraceServerOptions {
  host?: string;
  port?: number;
  path?: string;
  targetId?: string;
  traceConfig?: Protocol.Tracing.TraceConfig;
}

export interface BasicCdpTraceServer {
  readonly targetId: string;
  readonly url: string;
  readonly webSocketDebuggerUrl: string;
  close(): Promise<void>;
}

const DEFAULT_TRACE_CONFIG: Protocol.Tracing.TraceConfig = {
  recordMode: 'recordContinuously',
  includedCategories: ['node', 'node.async_hooks', 'node.perf', 'node.perf.usertiming', 'v8']
};

const DEFAULT_TARGET_ID = 'powersync-service-core';
const CPU_PROFILE_TRACE_CATEGORY = 'disabled-by-default-v8.cpu_profiler';
const CPU_PROFILE_TRACE_SOURCE = 'Internal';
const CPU_PROFILE_THREAD_ID = 1001;
const DEFAULT_STREAM_CHUNK_SIZE = 1024 * 1024;
const SUPPORTED_DOMAINS = new Set(['Console', 'Debugger', 'HeapProfiler', 'IO', 'Profiler', 'Runtime', 'Schema', 'Tracing']);
const CDP_PROTOCOL_VERSION = `${browserProtocol.version.major}.${browserProtocol.version.minor}`;
const PROTOCOL_DESCRIPTOR: ProtocolDescriptor = {
  version: browserProtocol.version,
  domains: ([...browserProtocol.domains, ...jsProtocol.domains] as ProtocolDescriptor['domains']).filter((domain) =>
    SUPPORTED_DOMAINS.has(domain.domain)
  )
};

export async function startBasicCdpTraceServer(options: BasicCdpTraceServerOptions = {}): Promise<BasicCdpTraceServer> {
  const host = options.host ?? '127.0.0.1';
  const targetId = options.targetId ?? DEFAULT_TARGET_ID;
  const wsPath = options.path ?? `/${targetId}`;
  const traceConfig = options.traceConfig ?? DEFAULT_TRACE_CONFIG;

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
        Browser: `PowerSync/${pkg.version}`,
        'Protocol-Version': CDP_PROTOCOL_VERSION
      });
    }

    if (url.pathname == '/json' || url.pathname == '/json/list') {
      return sendJson(response, [targetDescriptor(targetId, requestBaseUrl, requestWebSocketDebuggerUrl)]);
    }

    if (url.pathname == '/json/protocol') {
      return sendJson(response, PROTOCOL_DESCRIPTOR);
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
    let traceDomain: TraceDomain = 'Tracing';
    let profilerActive = false;
    let profilerStartTime = nowMicros();
    let profilerEvents: TraceEvent[] = [];
    let tracingCpuProfilerActive = false;
    let nextCpuProfileId = 1;
    let traceTransferMode: TraceTransferMode = 'ReportEvents';
    let bufferedTraceEvents: TraceEvent[] = [];
    let nextStreamId = 1;
    const streams = new Map<string, { data: string; offset: number }>();

    const sendEvent = (method: CdpEvent | `${TraceDomain}.dataCollected` | `${TraceDomain}.tracingComplete`, params: JsonObject = {}) => {
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
        if (traceTransferMode == 'ReturnAsStream') {
          bufferedTraceEvents.push(...validEvents);
        }
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
      if (traceTransferMode == 'ReturnAsStream') {
        const stream = `trace-${nextStreamId++}`;
        streams.set(stream, {
          data: JSON.stringify({ traceEvents: bufferedTraceEvents, metadata: {} }),
          offset: 0
        });
        bufferedTraceEvents = [];
        sendEvent(`${traceDomain}.tracingComplete`, {
          dataLossOccurred: false,
          stream,
          traceFormat: 'json'
        } satisfies Protocol.Tracing.TracingCompleteEvent);
      } else {
        sendEvent(`${traceDomain}.tracingComplete`, { dataLossOccurred: false } satisfies Protocol.Tracing.TracingCompleteEvent);
      }
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
          async startTracing(params) {
            if (tracingActive) {
              return;
            }
            traceTransferMode = params.transferMode ?? 'ReportEvents';
            bufferedTraceEvents = [];
            await startNodeCpuProfiler(inspectorSession);
            tracingCpuProfilerActive = true;
            tracingActive = true;
          },
          async stopTracing() {
            if (tracingCpuProfilerActive) {
              tracingCpuProfilerActive = false;
              const cpuProfileResult = await stopNodeCpuProfiler(inspectorSession);
              const cpuTraceEvents = cpuProfileToTraceEvents(cpuProfileResult.profile, {
                id: nextCpuProfileId++,
                traceClockEndTime: cpuProfileResult.traceClockEndTime
              });
              onInternalTraceEvents(cpuTraceEvents);
            }
            tracingActive = false;
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
          },
          readStream(params) {
            const stream = streams.get(params.handle);
            if (stream == null) {
              throw invalidStreamHandle(params.handle);
            }

            if (params.offset != null) {
              stream.offset = params.offset;
            }
            const size = params.size ?? DEFAULT_STREAM_CHUNK_SIZE;
            const data = stream.data.slice(stream.offset, stream.offset + size);
            stream.offset += data.length;
            return {
              base64Encoded: false,
              data,
              eof: stream.offset >= stream.data.length
            };
          },
          closeStream(params) {
            streams.delete(params.handle);
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
      if (tracingCpuProfilerActive) {
        tracingCpuProfilerActive = false;
        stopNodeCpuProfiler(inspectorSession).catch((error) => {
          console.warn('Failed to stop Node.js CPU profiler after CDP connection closed', error);
        });
      }
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

  logReceivedCommand(request);

  try {
    const result = await handleMethod(session, traceConfig, context, request.method, request.params ?? {});
    return { id: request.id, result };
  } catch (error) {
    return protocolError(request.id, error);
  }
}

function logReceivedCommand(request: CdpRequest) {
  const params = request.params == null ? '' : ` ${JSON.stringify(request.params)}`;
  console.log(`[CDP] <- ${request.id} ${request.method}${params}`);
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
      await context.startTracing(params);
      context.sendTraceEvents(getMetadataTraceEvents());
      return {};
    case 'Tracing.end':
    case 'NodeTracing.stop':
      await context.stopTracing();
      context.sendTracingComplete();
      return {};
    case 'Tracing.getCategories':
    case 'NodeTracing.getCategories':
      return {
        categories: [
          ...new Set(['powersync', '__metadata', CPU_PROFILE_TRACE_CATEGORY, ...(traceConfig?.includedCategories ?? [])])
        ]
      } satisfies Protocol.Tracing.GetCategoriesResponse;
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
        domains: PROTOCOL_DESCRIPTOR.domains.map((domain) => ({ name: domain.domain, version: CDP_PROTOCOL_VERSION }))
      } satisfies Protocol.Schema.GetDomainsResponse;
    case 'IO.read':
      return context.readStream(params as unknown as Protocol.IO.ReadRequest);
    case 'IO.close':
      context.closeStream(params as unknown as Protocol.IO.CloseRequest);
      return {};
    default:
      if (!isSupportedCdpMethod(method)) {
        throw methodNotFound(method);
      }
      return session.post(method as any, params);
  }
}

function isSupportedCdpMethod(method: string) {
  if (isNodeTracingMethod(method)) {
    return true;
  }

  const domain = method.split('.', 1)[0];
  return SUPPORTED_DOMAINS.has(domain);
}

function isNodeTracingMethod(method: string): method is NodeTracingMethod {
  return method == 'NodeTracing.start' || method == 'NodeTracing.stop' || method == 'NodeTracing.getCategories';
}

function methodNotFound(method: string) {
  return Object.assign(new Error(`Method not found: ${method}`), { code: -32601 });
}

function invalidStreamHandle(handle: string) {
  return Object.assign(new Error(`Invalid stream handle: ${handle}`), { code: -32000 });
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

function traceEventsToCpuProfile(events: TraceEvent[], startTime: number, endTime: number): Protocol.Profiler.Profile {
  const sortedEvents = events
    .filter((event) => event.ph == 'X' && isFiniteNumber(event.ts) && isFiniteNumber(event.dur))
    .sort((a, b) => a.ts - b.ts || b.dur - a.dur);

  const nodes: Protocol.Profiler.ProfileNode[] = [
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

function addProfileNode(event: TraceEvent, parentId: number, nodes: Protocol.Profiler.ProfileNode[]) {
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

async function startNodeCpuProfiler(session: Session) {
  await session.post('Profiler.enable');
  await session.post('Profiler.start');
}

async function stopNodeCpuProfiler(session: Session): Promise<NodeCpuProfileResult> {
  const beforeStop = nowMicros();
  const result = await session.post('Profiler.stop');
  const afterStop = nowMicros();

  return {
    profile: result.profile as Protocol.Profiler.Profile,
    traceClockEndTime: Math.round((beforeStop + afterStop) / 2)
  };
}

function cpuProfileToTraceEvents(
  profile: Protocol.Profiler.Profile,
  options: { id: number; traceClockEndTime: number }
): TraceEvent[] {
  const id = `0x${options.id.toString(16)}`;
  const clockOffset = options.traceClockEndTime - profile.endTime;
  const translatedProfileStartTime = profile.startTime + clockOffset;
  const startTime = translatedProfileStartTime;
  const endTime = Math.max(options.traceClockEndTime, profile.endTime + clockOffset);
  const timeDeltas = profile.timeDeltas ?? [];
  const filteredProfile = filterIdleProfileSamples(profile, timeDeltas);
  const samples = filteredProfile.samples;
  const cpuProfile: Partial<Protocol.Profiler.Profile> = {
    nodes: filteredProfile.nodes
  };
  const lines = Array(samples?.length ?? 0).fill(0);

  if (samples != null && samples.length > 0) {
    cpuProfile.samples = samples;
  }

  const visibleSampleEvents = cpuProfileToVisibleTraceEvents(profile, {
    startTime,
    timeDeltas,
    samples: profile.samples?.slice(0, timeDeltas.length) ?? [],
    skipIdle: true
  });

  return [
    {
      ph: 'M',
      cat: '__metadata',
      name: 'thread_name',
      pid: process.pid,
      tid: CPU_PROFILE_THREAD_ID,
      args: { name: 'Node.js CPU Profile' }
    },
    {
      ph: 'P',
      cat: CPU_PROFILE_TRACE_CATEGORY,
      name: 'Profile',
      id,
      pid: process.pid,
      tid: CPU_PROFILE_THREAD_ID,
      ts: startTime,
      args: {
        data: {
          startTime,
          source: CPU_PROFILE_TRACE_SOURCE
        }
      }
    },
    {
      ph: 'P',
      cat: CPU_PROFILE_TRACE_CATEGORY,
      name: 'ProfileChunk',
      id,
      pid: process.pid,
      tid: CPU_PROFILE_THREAD_ID,
      ts: endTime,
      args: {
        data: {
          cpuProfile,
          timeDeltas: filteredProfile.timeDeltas ?? [],
          lines,
          source: CPU_PROFILE_TRACE_SOURCE
        }
      }
    },
    ...visibleSampleEvents
  ];
}

function filterIdleProfileSamples(profile: Protocol.Profiler.Profile, timeDeltas: number[]): Protocol.Profiler.Profile {
  const idleNodeIds = new Set(profile.nodes.filter((node) => isIdleProfileNode(node)).map((node) => node.id));
  const samples = profile.samples ?? [];
  const normalizedSamples = samples.slice(0, timeDeltas.length);
  const normalizedTimeDeltas = timeDeltas.slice(0, normalizedSamples.length);

  if (idleNodeIds.size == 0 || samples.length == 0) {
    return {
      ...profile,
      samples: normalizedSamples,
      timeDeltas: normalizedTimeDeltas
    };
  }

  const replacementNodeId = findIdleReplacementNodeId(profile.nodes, idleNodeIds);

  return {
    ...profile,
    nodes: profile.nodes.filter((node) => !idleNodeIds.has(node.id)),
    samples: normalizedSamples.map((sample) => (idleNodeIds.has(sample) ? replacementNodeId : sample)),
    timeDeltas: normalizedTimeDeltas
  };
}

function isIdleProfileNode(node: Protocol.Profiler.ProfileNode) {
  return node.callFrame.functionName == '(idle)';
}

function findIdleReplacementNodeId(nodes: Protocol.Profiler.ProfileNode[], idleNodeIds: Set<number>) {
  return (
    nodes.find((node) => !idleNodeIds.has(node.id) && node.callFrame.functionName == '(program)')?.id ??
    nodes.find((node) => !idleNodeIds.has(node.id) && node.callFrame.functionName == '(root)')?.id ??
    nodes.find((node) => !idleNodeIds.has(node.id))?.id ??
    1
  );
}

function cpuProfileToVisibleTraceEvents(
  profile: Protocol.Profiler.Profile,
  options: { startTime: number; timeDeltas: number[]; samples: number[]; skipIdle?: boolean }
): TraceEvent[] {
  const nodesById = new Map(profile.nodes.map((node) => [node.id, node]));
  const parentById = new Map<number, number>();

  for (const node of profile.nodes) {
    for (const child of node.children ?? []) {
      parentById.set(child, node.id);
    }
  }

  const events: TraceEvent[] = [];
  let timestamp = options.startTime;

  for (let i = 0; i < options.samples.length; i++) {
    const sample = options.samples[i];
    const delta = options.timeDeltas[i] ?? 1;
    const sampleStart = timestamp;
    timestamp += delta;

    const node = nodesById.get(sample);
    if (node == null) {
      continue;
    }
    if (options.skipIdle && isIdleProfileNode(node)) {
      continue;
    }

    const stackTrace = profileStackTrace(sample, nodesById, parentById);
    const functionName = node.callFrame.functionName || '(anonymous)';

    events.push(
      {
        ph: 'I',
        s: 't',
        cat: 'devtools.timeline',
        name: 'JSSample',
        pid: process.pid,
        tid: CPU_PROFILE_THREAD_ID,
        ts: sampleStart,
        args: {
          data: {
            stackTrace
          }
        }
      },
      {
        ph: 'X',
        cat: 'devtools.timeline,v8,cpu_profiler',
        name: functionName,
        pid: process.pid,
        tid: CPU_PROFILE_THREAD_ID,
        ts: sampleStart,
        dur: Math.max(1, delta),
        args: {
          data: {
            callFrame: node.callFrame,
            stackTrace
          }
        }
      }
    );
  }

  return events;
}

function profileStackTrace(
  nodeId: number,
  nodesById: Map<number, Protocol.Profiler.ProfileNode>,
  parentById: Map<number, number>
) {
  const stack: Protocol.Runtime.CallFrame[] = [];
  let current: number | undefined = nodeId;

  while (current != null) {
    const node = nodesById.get(current);
    if (node == null) {
      break;
    }

    if (node.callFrame.functionName != '(root)') {
      stack.push(node.callFrame);
    }
    current = parentById.get(current);
  }

  return stack;
}

function nowMicros() {
  return Number(process.hrtime.bigint() / 1000n);
}

function targetDescriptor(targetId: string, _baseUrl: string, webSocketDebuggerUrl: string) {
  const frontendWebSocketUrl = webSocketDebuggerUrl.replace(/^ws:\/\//, '');

  return {
    id: targetId,
    type: 'page',
    title: 'PowerSync trace target',
    description: 'PowerSync trace target',
    url: `powersync://${targetId}`,
    webSocketDebuggerUrl,
    devtoolsFrontendUrl: `devtools://devtools/bundled/inspector.html?experiments=true&ws=${frontendWebSocketUrl}`,
    devtoolsFrontendUrlCompat: `devtools://devtools/bundled/inspector.html?experiments=true&ws=${frontendWebSocketUrl}`
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
      code: error?.code ?? -32000,
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
