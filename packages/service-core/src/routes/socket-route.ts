import { serialize } from 'bson';
import { SyncParameters, normalizeTokenParameters } from '@powersync/service-sync-rules';
import * as micro from '@journeyapps-platform/micro';

import * as util from '@/util/util-index.js';
import { streamResponse } from '../sync/sync.js';
import { SyncRoutes } from './sync-stream.js';
import { SocketRouteGenerator } from './router-socket.js';
import { Metrics } from '@/metrics/Metrics.js';

export const sync_stream_reactive: SocketRouteGenerator = (router) =>
  router.reactiveStream<util.StreamingSyncRequest, any>(SyncRoutes.STREAM, {
    authorize: ({ context }) => {
      return {
        authorized: !!context.token_payload,
        errors: ['Authentication required'].concat(context.token_errors ?? [])
      };
    },
    validator: micro.schema.createTsCodecValidator(util.StreamingSyncRequest, { allowAdditional: true }),
    handler: async ({ context, params, responder, observer, initialN }) => {
      const { system } = context;

      if (system.closed) {
        responder.onError(
          new micro.errors.JourneyError({
            status: 503,
            code: 'SERVICE_UNAVAILABLE',
            description: 'Service temporarily unavailable'
          })
        );
        responder.onComplete();
        return;
      }

      const controller = new AbortController();

      const syncParams: SyncParameters = normalizeTokenParameters(
        context.token_payload?.parameters ?? {},
        params.parameters ?? {}
      );

      const storage = system.storage;
      // Sanity check before we start the stream
      const cp = await storage.getActiveCheckpoint();
      if (!cp.hasSyncRules()) {
        responder.onError(
          new micro.errors.JourneyError({
            status: 500,
            code: 'NO_SYNC_RULES',
            description: 'No sync rules available'
          })
        );
        responder.onComplete();
        return;
      }

      let requestedN = initialN;
      const disposer = observer.registerListener({
        request(n) {
          requestedN += n;
        },
        cancel: () => {
          controller.abort();
        }
      });

      const removeStopHandler = system.addStopHandler(() => {
        observer.triggerCancel();
      });

      Metrics.getInstance().concurrent_connections.add(1);
      try {
        for await (const data of streamResponse({
          storage,
          params: {
            ...params,
            binary_data: true // always true for web sockets
          },
          syncParams,
          token: context!.token_payload!,
          tokenStreamOptions: {
            // RSocket handles keepalive events by default
            keep_alive: false
          },
          signal: controller.signal
        })) {
          if (data == null) {
            // Empty value just to flush iterator memory
            continue;
          } else if (typeof data == 'string') {
            // Should not happen with binary_data: true
            throw new Error(`Unexpected string data: ${data}`);
          }

          {
            // On NodeJS, serialize always returns a Buffer
            const serialized = serialize(data) as Buffer;
            responder.onNext({ data: serialized }, false);
            requestedN--;
            Metrics.getInstance().data_synced_bytes.add(serialized.length);
          }

          if (requestedN <= 0) {
            await new Promise<void>((resolve) => {
              const l = observer.registerListener({
                request() {
                  if (requestedN > 0) {
                    // Management of updating the total requested items is done above
                    resolve();
                    l();
                  }
                },
                cancel: () => {
                  // Don't wait here if the request is cancelled
                  resolve();
                  l();
                }
              });
            });
          }
        }
      } catch (ex) {
        // Convert to our standard form before responding.
        // This ensures the error can be serialized.
        const error = new micro.errors.InternalServerError(ex);
        micro.logger.error('Sync stream error', error);
        responder.onError(error);
      } finally {
        responder.onComplete();
        removeStopHandler();
        disposer();
        Metrics.getInstance().concurrent_connections.add(-1);
      }
    }
  });
