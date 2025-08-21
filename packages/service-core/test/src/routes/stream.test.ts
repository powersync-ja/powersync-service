import { BasicRouterRequest, Context, MetricsEngine, storage, StorageEngine, SyncContext } from '@/index.js';
import { logger, RouterResponse, ServiceError } from '@powersync/lib-services-framework';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { syncStreamed } from '../../../src/routes/endpoints/sync-stream.js';
import { SqlSyncRules } from '@powersync/service-sync-rules';

describe('Stream Route', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe('compressed stream', () => {
    it('handles missing sync rules', async () => {
      const storageEngine: StorageEngine = {
        activeBucketStorage: {
          async getActiveStorage() {
            return null;
          }
        } as any
      } as any;
      const context: Context = {
        logger: logger,
        service_context: {
          syncContext: new SyncContext({ maxBuckets: 1, maxDataFetchConcurrency: 1, maxParameterQueryResults: 1 }),
          routerEngine: {} as any,
          storageEngine,
          metricsEngine: new MetricsEngine({ disable_telemetry_sharing: true, factory: null as any }),
          // Not used
          configuration: null as any,
          lifeCycleEngine: null as any,
          migrations: null as any,
          replicationEngine: null as any,
          serviceMode: null as any
        }
      };

      const request: BasicRouterRequest = {
        headers: {},
        hostname: '',
        protocol: 'http'
      };

      const error = (await (syncStreamed.handler({ context, params: {}, request }) as Promise<RouterResponse>).catch(
        (e) => e
      )) as ServiceError;

      expect(error.errorData.status).toEqual(500);
      expect(error.errorData.code).toEqual('PSYNC_S2302');
    });

    it('handles a stream error', async () => {
      const storageEngine: StorageEngine = {
        activeBucketStorage: {
          async getActiveStorage() {
            return {
              getParsedSyncRules() {
                return new SqlSyncRules('bucket_definitions: {}');
              }
            };
          }
        }
      } as any;
      const context: Context = {
        logger: logger,
        service_context: {
          syncContext: new SyncContext({ maxBuckets: 1, maxDataFetchConcurrency: 1, maxParameterQueryResults: 1 }),
          routerEngine: {
            getAPI() {
              return {
                getParseSyncRulesOptions() {
                  return { defaultSchema: 'public' };
                }
              } as any;
            }
          } as any,
          storageEngine,
          metricsEngine: new MetricsEngine({ disable_telemetry_sharing: true, factory: null as any }),
          // Not used
          configuration: null as any,
          lifeCycleEngine: null as any,
          migrations: null as any,
          replicationEngine: null as any,
          serviceMode: null as any
        }
      };

      const request: BasicRouterRequest = {
        headers: {},
        hostname: '',
        protocol: 'http'
      };

      const response = await (syncStreamed.handler({ context, params: {}, request }) as Promise<RouterResponse>);
      expect(response.status).toEqual(200);
    });
  });
});
