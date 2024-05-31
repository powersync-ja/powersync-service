import { Readable } from 'stream';
import * as micro from '@journeyapps-platform/micro';
import { SyncParameters, normalizeTokenParameters } from '@powersync/service-sync-rules';

import * as sync from '@/sync/sync-index.js';
import * as util from '@/util/util-index.js';

import { authUser } from './auth.js';
import { RouteGenerator } from './router.js';
import { Metrics } from '@/metrics/Metrics.js';

export enum SyncRoutes {
  STREAM = '/sync/stream'
}

export const syncStreamed: RouteGenerator = (router) =>
  router.post(SyncRoutes.STREAM, {
    authorize: authUser,
    validator: micro.schema.createTsCodecValidator(util.StreamingSyncRequest, { allowAdditional: true }),
    handler: async (payload) => {
      const userId = payload.context.user_id!;
      const system = payload.context.system;

      if (system.closed) {
        throw new micro.errors.JourneyError({
          status: 503,
          code: 'SERVICE_UNAVAILABLE',
          description: 'Service temporarily unavailable'
        });
      }

      const params: util.StreamingSyncRequest = payload.params;
      const syncParams: SyncParameters = normalizeTokenParameters(payload.context.token_payload!.parameters ?? {});

      const storage = system.storage;
      // Sanity check before we start the stream
      const cp = await storage.getActiveCheckpoint();
      if (!cp.hasSyncRules()) {
        throw new micro.errors.JourneyError({
          status: 500,
          code: 'NO_SYNC_RULES',
          description: 'No sync rules available'
        });
      }

      const res = payload.reply;

      res.status(200).header('Content-Type', 'application/x-ndjson');

      const controller = new AbortController();
      try {
        Metrics.getInstance().concurrent_connections.add(1);
        const stream = Readable.from(
          sync.transformToBytesTracked(
            sync.ndjson(
              sync.streamResponse({
                storage,
                params,
                syncParams,
                token: payload.context.token_payload!,
                signal: controller.signal
              })
            )
          ),
          { objectMode: false, highWaterMark: 16 * 1024 }
        );

        const deregister = system.addStopHandler(() => {
          // This error is not currently propagated to the client
          controller.abort();
          stream.destroy(new Error('Shutting down system'));
        });
        stream.on('close', () => {
          deregister();
        });

        stream.on('error', (error) => {
          controller.abort();
          // Note: This appears as a 200 response in the logs.
          if (error.message != 'Shutting down system') {
            micro.logger.error('Streaming sync request failed', error);
          }
        });
        await res.send(stream);
      } finally {
        controller.abort();
        Metrics.getInstance().concurrent_connections.add(-1);
        // Prevent double-send
        res.hijack();
      }
    }
  });
