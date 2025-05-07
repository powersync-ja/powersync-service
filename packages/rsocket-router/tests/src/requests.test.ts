import { describe, expect, it, vi } from 'vitest';
import { createMockObserver, createMockResponder } from './utils/mock-responder.js';
import { handleReactiveStream, ReactiveStreamRequest } from '../../src/router/ReactiveSocketRouter.js';
import { deserialize, serialize } from 'bson';
import { RS_ENDPOINT_TYPE, ReactiveEndpoint, RequestMeta, SocketResponder } from '../../src/router/types.js';
import { EndpointHandlerPayload, ErrorCode } from '@powersync/lib-services-framework';

/**
 * Mocks the process of handling reactive routes
 * @param path A route path to trigger with a dummy payload
 * @param endpoints The router's configured endpoints
 * @param responder a mock responder
 * @returns
 */
async function handleRoute(
  path: string,
  endpoints: ReactiveEndpoint[],
  responder: SocketResponder,
  request?: Partial<ReactiveStreamRequest>
) {
  return handleReactiveStream<{}>(
    {},
    {
      payload: {
        data: Buffer.from(serialize({})),
        metadata: Buffer.from(serialize({ path }))
      },
      initialN: 1,
      dataMimeType: 'application/bson',
      metadataMimeType: 'application/bson',
      responder,
      ...request
    },
    createMockObserver(),
    new AbortController(),
    {
      contextProvider: async () => ({}),
      endpoints,
      metaDecoder: async (buffer) => deserialize(buffer.contents) as RequestMeta,
      payloadDecoder: async (buffer) => buffer && deserialize(buffer.contents)
    }
  );
}

describe('Requests', () => {
  it('should get successful response from route', async () => {
    const responder = createMockResponder();
    const spy = vi.spyOn(responder, 'onNext');

    const path = '/test-route';

    await handleRoute(
      path,
      [
        {
          path,
          type: RS_ENDPOINT_TYPE.STREAM,
          handler: async (p) => {
            // Send data to client
            p.responder.onNext({ data: Buffer.from(serialize({})) }, true);
          }
        }
      ],
      responder
    );

    // The onNext() method should have been called to send data to client
    expect(spy).toHaveBeenCalledTimes(1);
  });

  it('should get validation error response from route', async () => {
    const responder = createMockResponder();
    const spy = vi.spyOn(responder, 'onError');

    const path = '/test-route';

    const validationError = 'Test validation error';

    await handleRoute(
      path,
      [
        {
          path,
          type: RS_ENDPOINT_TYPE.STREAM,
          handler: async () => {},
          // This will always return an invalid error
          validator: {
            validate: () => {
              return {
                valid: false,
                errors: [validationError]
              };
            }
          }
        }
      ],
      responder
    );

    // Should be a validation error
    expect(JSON.stringify(spy.mock.calls[0])).includes(validationError);
  });

  it('should get authorization error response from route', async () => {
    const responder = createMockResponder();
    const spy = vi.spyOn(responder, 'onError');

    const path = '/test-route';

    await handleRoute(
      path,
      [
        {
          path,
          type: RS_ENDPOINT_TYPE.STREAM,
          handler: async () => {},
          // This will always return unauthorized
          authorize: async () => {
            return {
              authorized: false
            };
          }
        }
      ],
      responder
    );

    // Should be a validation error
    expect(JSON.stringify(spy.mock.calls[0])).includes(ErrorCode.PSYNC_S2101);
  });

  it('should get invalid route error', async () => {
    const responder = createMockResponder();
    const spy = vi.spyOn(responder, 'onError');

    const path = '/test-route';

    // Providing no endpoints means there won't be any matching route
    await handleRoute(path, [], responder);

    // Should be a validation error
    expect(JSON.stringify(spy.mock.calls[0])).includes(ErrorCode.PSYNC_S2002);
  });

  it('should forward mime types', async () => {
    const encoder = new TextEncoder();
    const decoder = new TextDecoder();
    const responder = createMockResponder();
    const encodeJson = (value: any) => encoder.encode(JSON.stringify(value));
    const path = '/test-route';

    const fn = vi.fn(async (p: EndpointHandlerPayload<any, any>) => {
      expect(p.params).toStrictEqual({ hello: 'world' });
      return undefined;
    });

    await handleReactiveStream<{}>(
      {},
      {
        payload: {
          data: Buffer.from(encodeJson({ hello: 'world' })),
          metadata: Buffer.from(encodeJson({ path }))
        },
        metadataMimeType: 'application/json',
        dataMimeType: 'application/json',
        initialN: 1,
        responder
      },
      createMockObserver(),
      new AbortController(),
      {
        contextProvider: async () => ({}),
        endpoints: [
          {
            path,
            type: RS_ENDPOINT_TYPE.STREAM,
            handler: fn
          }
        ],
        metaDecoder: async (buffer) => {
          expect(buffer.mimeType, 'application/json');
          return JSON.parse(decoder.decode(buffer.contents));
        },
        payloadDecoder: async (buffer) => {
          expect(buffer!.mimeType, 'application/json');
          return JSON.parse(decoder.decode(buffer!.contents));
        }
      }
    );

    expect(fn).toHaveBeenCalled();
  });
});
