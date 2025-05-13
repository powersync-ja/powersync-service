export const isAsyncIterable = (iterable: any): iterable is AsyncIterable<any> => {
  return iterable != null && typeof iterable === 'object' && Symbol.asyncIterator in iterable;
};

export type RouterResponseParams<T> = {
  status?: number;
  data: T;
  headers?: Record<string, string>;
  /**
   * Hook to be called after the response has been sent
   */
  afterSend?: (details: { clientClosed: boolean }) => Promise<void>;
};

/**
 * Wrapper for router responses.
 * A {@link EndpointHandler} can return a raw JSON object to be used as the response data
 * payload or a [RouterResponse] which allows for more granular control of what is sent.
 */
export class RouterResponse<T = unknown> {
  status: number;
  data: T;
  headers: Record<string, string>;
  afterSend: (details: { clientClosed: boolean }) => Promise<void>;

  __micro_router_response = true;

  static isRouterResponse(input: any): input is RouterResponse {
    return input instanceof RouterResponse || input?.__micro_router_response == true;
  }

  constructor(params: RouterResponseParams<T>) {
    this.status = params.status || 200;
    this.data = params.data;
    this.headers = params.headers || {};
    this.afterSend = params.afterSend ?? (() => Promise.resolve());

    if (!this.headers['Content-Type']) {
      if (isAsyncIterable(this.data)) {
        this.headers['Content-Type'] = 'application/octet-stream';
      } else if (Buffer.isBuffer(this.data)) {
        this.headers['Content-Type'] = 'application/octet-stream';
      } else {
        this.headers['Content-Type'] = 'application/json';
      }
    }
  }
}
