import { errors, logger, schema } from '@powersync/lib-services-framework';
import { RequestParameters } from '@powersync/service-sync-rules';
import { serialize } from 'bson';

import { Metrics } from '../../metrics/Metrics.js';
import { streamResponse } from '../../sync/sync.js';
import * as util from '../../util/util-index.js';
import { SocketRouteGenerator } from '../router-socket.js';
import { SyncRoutes } from './sync-stream.js';
import { RequestTracker } from '../../sync/RequestTracker.js';

export const syncStreamReactive: SocketRouteGenerator = (router) =>
  router.reactiveStream<util.StreamingSyncRequest, any>(SyncRoutes.STREAM, {
    authorize: ({ context }) => {
      return {
        authorized: !!context.token_payload,
        errors: ['Authentication required'].concat(context.token_errors ?? [])
      };
    },
    validator: schema.createTsCodecValidator(util.StreamingSyncRequest, { allowAdditional: true }),
    handler: async ({ context, params, responder, observer, initialN }) => {
      const { system } = context;

      if (system.closed) {
        responder.onError(
          new errors.JourneyError({
            status: 503,
            code: 'SERVICE_UNAVAILABLE',
            description: 'Service temporarily unavailable'
          })
        );
        responder.onComplete();
        return;
      }

      const controller = new AbortController();

      const syncParams = new RequestParameters(context.token_payload!, params.parameters ?? {});

      const storage = system.storage;
      // Sanity check before we start the stream
      const cp = await storage.getActiveCheckpoint();
      if (!cp.hasSyncRules()) {
        responder.onError(
          new errors.JourneyError({
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
      const tracker = new RequestTracker();
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
          tracker,
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
            tracker.addDataSynced(serialized.length);
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
        const error = new errors.InternalServerError(ex);
        logger.error('Sync stream error', error);
        responder.onError(error);
      } finally {
        responder.onComplete();
        removeStopHandler();
        disposer();
        logger.info(`Sync stream complete`, {
          user_id: syncParams.user_id,
          operations_synced: tracker.operationsSynced,
          data_synced_bytes: tracker.dataSyncedBytes
        });
        Metrics.getInstance().concurrent_connections.add(-1);
      }
    }
  });
