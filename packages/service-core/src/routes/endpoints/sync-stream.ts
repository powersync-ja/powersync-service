import { Readable } from 'stream';
import { SyncParameters, normalizeTokenParameters } from '@powersync/service-sync-rules';
import { container, errors, router, schema } from '@powersync/lib-services-framework';

import * as sync from '../../sync/sync-index.js';
import * as util from '../../util/util-index.js';

import { authUser } from '../auth.js';
import { routeDefinition } from '../router.js';
import { Metrics } from '../../metrics/Metrics.js';

export enum SyncRoutes {
  STREAM = '/sync/stream'
}

export const syncStreamed = routeDefinition({
  path: SyncRoutes.STREAM,
  method: router.HTTPMethod.POST,
  authorize: authUser,
  validator: schema.createTsCodecValidator(util.StreamingSyncRequest, { allowAdditional: true }),
  handler: async (payload) => {
    const system = payload.context.system;

    if (system.closed) {
      throw new errors.JourneyError({
        status: 503,
        code: 'SERVICE_UNAVAILABLE',
        description: 'Service temporarily unavailable'
      });
    }

    const params: util.StreamingSyncRequest = payload.params;
    const syncParams: SyncParameters = normalizeTokenParameters(
      payload.context.token_payload!.parameters ?? {},
      payload.params.parameters ?? {}
    );

    const storage = system.storage;
    // Sanity check before we start the stream
    const cp = await storage.getActiveCheckpoint();
    if (!cp.hasSyncRules()) {
      throw new errors.JourneyError({
        status: 500,
        code: 'NO_SYNC_RULES',
        description: 'No sync rules available'
      });
    }
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
          container.logger.error('Streaming sync request failed', error);
        }
      });

      return new router.RouterResponse({
        status: 200,
        headers: {
          'Content-Type': 'application/x-ndjson'
        },
        data: stream,
        afterSend: async () => {
          controller.abort();
          Metrics.getInstance().concurrent_connections.add(-1);
        }
      });
    } catch (ex) {
      controller.abort();
      Metrics.getInstance().concurrent_connections.add(-1);
    }
  }
});

export const SYNC_STREAM_ROUTES = [syncStreamed];
