import * as diagnostics_channel from 'node:diagnostics_channel';

export const HTTP_CLIENT_REQUEST_CHANNEL_PREFIX = 'http.client.request';
export const httpClientRequestStartChannel = diagnostics_channel.channel(`${HTTP_CLIENT_REQUEST_CHANNEL_PREFIX}.start`);
export const httpClientRequestFinishChannel = diagnostics_channel.channel(`${HTTP_CLIENT_REQUEST_CHANNEL_PREFIX}.finish`);
export const httpClientRequestErrorChannel = diagnostics_channel.channel(`${HTTP_CLIENT_REQUEST_CHANNEL_PREFIX}.error`);

export type DebugHttpEvent = DebugHttpRequestStarted | DebugHttpRequestFinished | DebugHttpRequestFailed;

export interface DebugHttpRequestStarted {
  requestId: string;
  timestamp: number;
  wallTime: number;
  method: string;
  url: string;
  headers?: Record<string, string>;
  postData?: string;
}

export interface DebugHttpRequestFinished {
  requestId: string;
  timestamp: number;
  url: string;
  status: number;
  statusText: string;
  headers?: Record<string, string>;
  encodedDataLength?: number;
}

export interface DebugHttpRequestFailed {
  requestId: string;
  timestamp: number;
  errorText: string;
  canceled?: boolean;
}

let nextMongoClientId = 1;
const instrumentedMongoClients = new WeakSet<object>();

export function instrumentMongoClientDebugHttpEvents(client: any, options: { label?: string } = {}) {
  if (client == null || typeof client.on != 'function' || instrumentedMongoClients.has(client)) {
    return;
  }

  instrumentedMongoClients.add(client);
  const clientId = nextMongoClientId++;
  const label = options.label ?? 'mongodb';

  client.on('commandStarted', (event: any) => {
    const requestId = mongoRequestId(clientId, event);
    const commandName = String(event.commandName ?? 'command');
    const databaseName = String(event.databaseName ?? 'admin');
    const command = safeJson(event.command);
    httpClientRequestStartChannel.publish({
      requestId,
      timestamp: nowSeconds(),
      wallTime: Date.now() / 1000,
      method: 'POST',
      url: `mongodb://${label}/${databaseName}/${commandName}`,
      headers: {
        'x-powersync-debug-source': 'mongodb',
        'x-mongodb-command': commandName,
        'x-mongodb-database': databaseName
      },
      postData: command
    } satisfies DebugHttpRequestStarted);
  });

  client.on('commandSucceeded', (event: any) => {
    httpClientRequestFinishChannel.publish({
      requestId: mongoRequestId(clientId, event),
      timestamp: nowSeconds(),
      url: `mongodb://${label}/${String(event.commandName ?? 'command')}`,
      status: 200,
      statusText: 'OK',
      headers: {
        'x-mongodb-command': String(event.commandName ?? 'command')
      },
      encodedDataLength: approximateSize(event.reply)
    } satisfies DebugHttpRequestFinished);
  });

  client.on('commandFailed', (event: any) => {
    httpClientRequestErrorChannel.publish({
      requestId: mongoRequestId(clientId, event),
      timestamp: nowSeconds(),
      errorText: event.failure?.message ?? event.failure?.errmsg ?? String(event.failure ?? 'MongoDB command failed'),
      canceled: false
    } satisfies DebugHttpRequestFailed);
  });
}

function mongoRequestId(clientId: number, event: any) {
  return `mongodb-${clientId}-${String(event.requestId ?? 'unknown')}`;
}

function nowSeconds() {
  return Number(process.hrtime.bigint()) / 1_000_000_000;
}

function safeJson(value: unknown) {
  try {
    return JSON.stringify(value);
  } catch {
    return undefined;
  }
}

function approximateSize(value: unknown) {
  const json = safeJson(value);
  return json == null ? 0 : Buffer.byteLength(json);
}
