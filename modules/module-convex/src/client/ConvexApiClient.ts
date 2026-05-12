import { JSONBig } from '@powersync/service-jsonbig';
import { setTimeout as delay } from 'timers/promises';
import { CONVEX_CHECKPOINT_TABLE } from '../common/ConvexCheckpoints.js';
import { NormalizedConvexConnectionConfig } from '../types/types.js';
import {
  ConvexDocumentDeltasOptions,
  ConvexDocumentDeltasResult,
  ConvexJsonSchemasResult,
  ConvexListSnapshotOptions,
  ConvexListSnapshotResult,
  ensureConvexDocumentDeltasResult,
  ensureConvexListSnapshotResult,
  ensureRawJsonSchemaResponse
} from './ConvexAPITypes.js';

const CONVEX_REQUEST_TIMEOUT_MS = 60_000;

type GetRequestParams = {
  path: string;
  params?: Record<string, unknown>;
  signal?: AbortSignal;
  extraHeaders?: Record<string, string>;
  includeJsonFormat?: boolean;
};

export class ConvexApiError extends Error {
  readonly status?: number;
  readonly retryable: boolean;
  readonly body?: unknown;

  constructor(options: { message: string; status?: number; retryable: boolean; body?: unknown; cause?: unknown }) {
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
    const raw = await this.performTypedGetRequest(
      {
        path: '/api/json_schemas',
        signal: options?.signal
      },
      ensureRawJsonSchemaResponse
    );
    // Convex returns this as a map of {[tableName]: JSONSchema} for tables
    return {
      tables: Object.entries(raw).map(([tableName, schema]) => ({
        tableName,
        schema
      }))
    };
  }

  async listSnapshot(options: ConvexListSnapshotOptions): Promise<ConvexListSnapshotResult> {
    return await this.performTypedGetRequest(
      {
        path: '/api/list_snapshot',
        params: {
          snapshot: options.snapshot,
          cursor: options.cursor,
          table_name: options.tableName
        },
        signal: options.signal
      },
      ensureConvexListSnapshotResult
    );
  }

  async documentDeltas(options: ConvexDocumentDeltasOptions): Promise<ConvexDocumentDeltasResult> {
    return await this.performTypedGetRequest(
      {
        path: '/api/document_deltas',
        params: {
          cursor: options.cursor
        },
        signal: options.signal
      },
      ensureConvexDocumentDeltasResult
    );
  }

  async getGlobalSnapshotCursor(options?: { signal?: AbortSignal }): Promise<string> {
    const page = await this.listSnapshot({
      signal: options?.signal
    });
    return page.snapshot.toString();
  }

  async getHeadCursor(options?: { signal?: AbortSignal }): Promise<string> {
    return this.getGlobalSnapshotCursor({ signal: options?.signal });
  }

  async createWriteCheckpointMarker(options?: { signal?: AbortSignal }): Promise<void> {
    await this.assertHostAllowed();

    await this.performRequest({
      method: 'POST',
      url: new URL('/api/mutation', this.config.deployment_url),
      body: {
        path: `${CONVEX_CHECKPOINT_TABLE}:createCheckpoint`,
        args: {},
        format: 'json'
      },
      signal: options?.signal,
      extraHeaders: {
        'Content-Type': 'application/json'
      }
    });
  }

  private async performGetRequest(options: GetRequestParams) {
    const { path, params = {}, signal, extraHeaders, includeJsonFormat } = options;

    // `params` are mapped to url search params for GET requests
    const url = new URL(path, this.config.deployment_url);
    if (includeJsonFormat ?? true) {
      url.searchParams.set('format', 'json');
    }

    for (const [key, value] of Object.entries(params)) {
      if (value == null) {
        continue;
      }
      url.searchParams.set(key, `${value}`);
    }

    return this.performRequest({
      method: 'GET',
      url,
      extraHeaders,
      signal
    });
  }

  private async performTypedGetRequest<ResponseType>(
    options: GetRequestParams,
    validator: (data: unknown) => ResponseType
  ) {
    const rawResponse = await this.performGetRequest(options);
    try {
      return validator(rawResponse);
    } catch (ex) {
      throw new ConvexApiError({
        message: `Failed to validate Convex API request response format`,
        retryable: false,
        cause: ex
      });
    }
  }

  /**
   * Performs a request which expects a JSON response
   */
  private async performRequest(options: {
    method: 'GET' | 'POST';
    url: URL;
    body?: unknown;
    signal?: AbortSignal;
    extraHeaders?: Record<string, string>;
  }): Promise<Record<string, any>> {
    const { method, url, body, extraHeaders = {}, signal: requestSignal } = options;

    const timeout = new AbortController();
    const timeoutPromise = delay(CONVEX_REQUEST_TIMEOUT_MS, undefined, {
      signal: timeout.signal
    }).then(() => {
      timeout.abort(new Error(`Convex API request timed out after ${CONVEX_REQUEST_TIMEOUT_MS}ms`));
    });

    const signals = [timeout.signal];
    if (requestSignal) {
      signals.push(requestSignal);
    }

    const signal = AbortSignal.any(signals);

    try {
      const response = await fetch(url, {
        method,
        headers: {
          Authorization: `Convex ${this.config.deploy_key}`,
          Accept: 'application/json',
          ...extraHeaders
        },
        body: body == null ? undefined : JSON.stringify(options.body),
        signal
      });

      const text = await response.text();
      let json: unknown;
      try {
        json = JSONBig.parse(text);
      } catch (ex) {
        // The response could not be json, this should only happen for !ok responses.
      }

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

    const hostname = new URL(this.config.deployment_url).hostname;

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
    (asString.includes('cursor') && (asString.includes('expired') || asString.includes('invalid'))) ||
    (asString.includes('snapshot') && asString.includes('expired'))
  );
}

function isRecord(value: unknown): value is Record<string, any> {
  return typeof value == 'object' && value != null && !Array.isArray(value);
}
