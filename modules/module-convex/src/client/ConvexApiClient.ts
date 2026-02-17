import { setTimeout as delay } from 'timers/promises';
import { JSONBig } from '@powersync/service-jsonbig';
import { NormalizedConvexConnectionConfig } from '../types/types.js';
import { CONVEX_CHECKPOINT_TABLE } from '../common/ConvexCheckpoints.js';

export interface ConvexRawDocument {
  _id?: string;
  _table?: string;
  _deleted?: boolean;
  [key: string]: any;
}

export interface ConvexTableSchema {
  tableName: string;
  schema: Record<string, any>;
}

export interface ConvexJsonSchemasResult {
  tables: ConvexTableSchema[];
  raw: Record<string, any>;
}

export interface ConvexListSnapshotOptions {
  snapshot?: string;
  cursor?: string;
  tableName?: string;
  signal?: AbortSignal;
}

export interface ConvexListSnapshotResult {
  snapshot: string;
  cursor: string | null;
  hasMore: boolean;
  values: ConvexRawDocument[];
}

export interface ConvexDocumentDeltasOptions {
  cursor?: string;
  signal?: AbortSignal;
}

export interface ConvexDocumentDeltasResult {
  cursor: string;
  hasMore: boolean;
  values: ConvexRawDocument[];
}

export interface ConvexImportTable {
  jsonSchema: Record<string, unknown>;
}

export interface ConvexImportMessage {
  tableName: string;
  data: Record<string, unknown>;
}

export class ConvexApiError extends Error {
  readonly status?: number;
  readonly retryable: boolean;
  readonly body?: unknown;

  constructor(options: {
    message: string;
    status?: number;
    retryable: boolean;
    body?: unknown;
    cause?: unknown;
  }) {
    super(options.message, options.cause !== undefined ? { cause: options.cause } : undefined);
    this.name = 'ConvexApiError';
    this.status = options.status;
    this.retryable = options.retryable;
    this.body = options.body;
  }
}

export class ConvexApiClient {
  constructor(private readonly config: NormalizedConvexConnectionConfig) {}

  async getJsonSchemas(options?: { signal?: AbortSignal }): Promise<ConvexJsonSchemasResult> {
    const payload = await this.requestJson({
      endpoint: 'json_schemas',
      signal: options?.signal,
      allowStreamingFallback: true
    });

    return {
      tables: extractTableSchemas(payload),
      raw: payload
    };
  }

  async listSnapshot(options: ConvexListSnapshotOptions): Promise<ConvexListSnapshotResult> {
    const payload = await this.requestJson({
      endpoint: 'list_snapshot',
      params: {
        snapshot: options.snapshot,
        cursor: options.cursor,
        tableName: options.tableName
      },
      signal: options.signal,
      allowStreamingFallback: true
    });

    const values = parseValues(payload);
    const snapshot = parseSnapshot(payload, options.snapshot);
    const cursor = payload.cursor == null ? null : stringifyCursor(payload.cursor);

    return {
      snapshot,
      cursor,
      hasMore: parseHasMore(payload),
      values
    };
  }

  async documentDeltas(options: ConvexDocumentDeltasOptions): Promise<ConvexDocumentDeltasResult> {
    const payload = await this.requestJson({
      endpoint: 'document_deltas',
      params: {
        cursor: options.cursor
      },
      signal: options.signal,
      allowStreamingFallback: true
    });

    return {
      cursor: stringifyCursor(payload.cursor),
      hasMore: parseHasMore(payload),
      values: parseValues(payload)
    };
  }

  async getGlobalSnapshotCursor(options?: { signal?: AbortSignal }): Promise<string> {
    const page = await this.listSnapshot({
      signal: options?.signal
    });
    return page.snapshot;
  }

  async getHeadCursor(options?: { signal?: AbortSignal }): Promise<string> {
    return this.getGlobalSnapshotCursor({ signal: options?.signal });
  }

  async importAirbyteRecords(options: {
    tables: Record<string, ConvexImportTable>;
    messages: ConvexImportMessage[];
    signal?: AbortSignal;
  }): Promise<void> {
    await this.performRequest({
      method: 'POST',
      path: '/api/streaming_import/import_airbyte_records',
      body: {
        tables: options.tables,
        messages: options.messages
      },
      signal: options.signal,
      extraHeaders: {
        'Content-Type': 'application/json',
        'Convex-Client': 'streaming-import-1.0.0'
      },
      includeJsonFormat: false
    });
  }

  async createWriteCheckpointMarker(options?: { headCursor?: string; signal?: AbortSignal }): Promise<void> {
    const marker = {
      powersync_lsn: options?.headCursor ?? (await this.getHeadCursor({ signal: options?.signal })),
      powersync_created_at: Date.now(),
      powersync_source: 'powersync'
    };

    await this.importAirbyteRecords({
      tables: {
        [CONVEX_CHECKPOINT_TABLE]: {
          jsonSchema: POWERSYNC_CHECKPOINT_SCHEMA
        }
      },
      messages: [
        {
          tableName: CONVEX_CHECKPOINT_TABLE,
          data: marker
        }
      ],
      signal: options?.signal
    });
  }

  private async requestJson(options: {
    endpoint: string;
    params?: Record<string, unknown>;
    signal?: AbortSignal;
    allowStreamingFallback?: boolean;
  }): Promise<Record<string, any>> {
    await this.assertHostAllowed();

    const primaryPath = `/api/${options.endpoint}`;
    const fallbackPath = `/api/streaming_export/${options.endpoint}`;

    try {
      return await this.performRequest({
        path: primaryPath,
        params: options.params,
        signal: options.signal
      });
    } catch (error) {
      if (
        options.allowStreamingFallback &&
        error instanceof ConvexApiError &&
        error.status == 404 &&
        primaryPath != fallbackPath
      ) {
        return await this.performRequest({
          path: fallbackPath,
          params: options.params,
          signal: options.signal
        });
      }
      throw error;
    }
  }

  private async performRequest(options: {
    method?: 'GET' | 'POST';
    path: string;
    params?: Record<string, unknown>;
    body?: unknown;
    signal?: AbortSignal;
    extraHeaders?: Record<string, string>;
    includeJsonFormat?: boolean;
  }): Promise<Record<string, any>> {
    const url = new URL(options.path, this.config.deploymentUrl);
    if (options.includeJsonFormat ?? true) {
      url.searchParams.set('format', 'json');
    }

    for (const [key, value] of Object.entries(options.params ?? {})) {
      if (value == null) {
        continue;
      }
      url.searchParams.set(key, `${value}`);
    }

    const timeout = new AbortController();
    const timeoutPromise = delay(this.config.requestTimeoutMs, undefined, {
      signal: timeout.signal
    }).then(() => {
      timeout.abort(new Error(`Convex API request timed out after ${this.config.requestTimeoutMs}ms`));
    });

    const signals = [timeout.signal];
    if (options.signal) {
      signals.push(options.signal);
    }

    const signal = AbortSignal.any(signals);

    try {
      const response = await fetch(url, {
        method: options.method ?? 'GET',
        headers: {
          Authorization: `Convex ${this.config.deployKey}`,
          Accept: 'application/json',
          ...(options.extraHeaders ?? {})
        },
        body: options.body == null ? undefined : JSON.stringify(options.body),
        signal
      });

      const text = await response.text();
      const json = text == '' ? {} : safeParseJson(text);

      if (!response.ok) {
        const retryable = response.status == 429 || response.status >= 500;
        throw new ConvexApiError({
          message: `Convex API request failed (${response.status}) for ${url.pathname}`,
          status: response.status,
          retryable,
          body: json,
          cause: new Error(text)
        });
      }

      if (!isRecord(json)) {
        throw new ConvexApiError({
          message: `Convex API response was not an object for ${url.pathname}`,
          retryable: false,
          body: json
        });
      }

      return json;
    } catch (error) {
      if (error instanceof ConvexApiError) {
        throw error;
      }

      const message = error instanceof Error ? error.message : `${error}`;
      const retryable =
        message.includes('timed out') ||
        message.includes('fetch failed') ||
        message.includes('ECONNREFUSED') ||
        message.includes('ECONNRESET') ||
        message.includes('ENOTFOUND') ||
        message.includes('EAI_AGAIN') ||
        message.includes('ETIMEDOUT') ||
        error instanceof DOMException;
      throw new ConvexApiError({
        message: `Convex API request failed for ${url.pathname}: ${message}`,
        retryable,
        cause: error
      });
    } finally {
      timeout.abort();
      await timeoutPromise.catch(() => {
        // no-op
      });
    }
  }

  private async assertHostAllowed(): Promise<void> {
    if (!this.config.lookup) {
      return;
    }

    const hostname = new URL(this.config.deploymentUrl).hostname;

    await new Promise<void>((resolve, reject) => {
      this.config.lookup!(hostname, {}, (error) => {
        if (error) {
          reject(error);
        } else {
          resolve();
        }
      });
    });
  }
}

export function isCursorExpiredError(error: unknown): boolean {
  if (!(error instanceof ConvexApiError)) {
    return false;
  }

  const asString = `${error.message} ${JSON.stringify(error.body ?? {})}`.toLowerCase();

  return (
    asString.includes('cursor') && (asString.includes('expired') || asString.includes('invalid'))
  ) || (asString.includes('snapshot') && asString.includes('expired'));
}

function parseHasMore(payload: Record<string, any>): boolean {
  return Boolean(payload.has_more ?? payload.hasMore ?? false);
}

function parseValues(payload: Record<string, any>): ConvexRawDocument[] {
  const values = payload.values ?? payload.page ?? [];
  if (!Array.isArray(values)) {
    return [];
  }

  return values.filter((value) => isRecord(value)) as ConvexRawDocument[];
}

function extractTableSchemas(payload: Record<string, any>): ConvexTableSchema[] {
  const resolved = new Map<string, ConvexTableSchema>();

  const tableList = payload.tables;
  if (Array.isArray(tableList)) {
    for (const table of tableList) {
      if (!isRecord(table)) {
        continue;
      }
      const tableName = table.tableName ?? table.table_name ?? table.name;
      if (typeof tableName != 'string' || tableName.length == 0) {
        continue;
      }
      resolved.set(tableName, {
        tableName,
        schema: isRecord(table.schema) ? table.schema : {}
      });
    }
  } else if (isRecord(tableList)) {
    for (const [tableName, schema] of Object.entries(tableList)) {
      resolved.set(tableName, {
        tableName,
        schema: isRecord(schema) ? schema : {}
      });
    }
  }

  const schemaMap = payload.schema;
  if (isRecord(schemaMap)) {
    for (const [tableName, schema] of Object.entries(schemaMap)) {
      if (!resolved.has(tableName)) {
        resolved.set(tableName, {
          tableName,
          schema: isRecord(schema) ? schema : {}
        });
      }
    }
  }

  // Self-hosted Convex may return table schemas as the top-level object:
  // { "table_a": { ...json schema... }, "table_b": { ...json schema... } }.
  for (const [tableName, schema] of Object.entries(payload)) {
    if (resolved.has(tableName) || RESERVED_SCHEMA_KEYS.has(tableName)) {
      continue;
    }

    if (!looksLikeJsonSchema(schema)) {
      continue;
    }

    resolved.set(tableName, {
      tableName,
      schema: isRecord(schema) ? schema : {}
    });
  }

  return [...resolved.values()].sort((a, b) => a.tableName.localeCompare(b.tableName));
}

function stringifyCursor(value: unknown): string {
  if (typeof value == 'string') {
    if (value.length == 0) {
      throw new ConvexApiError({
        message: 'Convex cursor cannot be empty',
        retryable: false,
        body: value
      });
    }
    return value;
  }

  if (typeof value == 'number' || typeof value == 'bigint') {
    return `${value}`;
  }

  throw new ConvexApiError({
    message: `Convex cursor is missing or invalid: ${JSON.stringify(value)}`,
    retryable: false,
    body: value
  });
}

function parseSnapshot(payload: Record<string, any>, requestedSnapshot?: string): string {
  const responseSnapshot = payload.snapshot ?? payload.snapshot_ts ?? payload.snapshotTs;
  if (responseSnapshot != null) {
    return stringifyCursor(responseSnapshot);
  }

  if (requestedSnapshot != null && requestedSnapshot.length > 0) {
    return requestedSnapshot;
  }

  throw new ConvexApiError({
    message: 'Convex list_snapshot response is missing snapshot',
    retryable: false,
    body: payload
  });
}

function safeParseJson(value: string): unknown {
  try {
    return JSONBig.parse(value);
  } catch {
    return { raw: value };
  }
}

function isRecord(value: unknown): value is Record<string, any> {
  return typeof value == 'object' && value != null && !Array.isArray(value);
}

const RESERVED_SCHEMA_KEYS = new Set([
  'tables',
  'schema',
  'snapshot',
  'cursor',
  'values',
  'value',
  'page',
  'hasMore',
  'has_more',
  'error',
  'errors'
]);

const POWERSYNC_CHECKPOINT_SCHEMA = {
  type: 'object',
  properties: {
    powersync_lsn: { type: 'string' },
    powersync_created_at: { type: 'number' },
    powersync_source: { type: 'string' }
  },
  additionalProperties: false,
  required: ['powersync_lsn', 'powersync_created_at', 'powersync_source'],
  $schema: 'http://json-schema.org/draft-07/schema#'
} as const;

function looksLikeJsonSchema(value: unknown): boolean {
  if (!isRecord(value)) {
    return false;
  }

  return (
    typeof value.type == 'string' ||
    typeof value.$schema == 'string' ||
    Array.isArray(value.required) ||
    isRecord(value.properties) ||
    isRecord(value.schema)
  );
}
