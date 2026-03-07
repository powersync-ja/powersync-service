import {
  BucketStorageFactory,
  createCoreAPIMetrics,
  MetricsEngine,
  OpenTelemetryMetricsFactory,
  RouteAPI,
  RouterEngine,
  ServiceContext,
  StorageEngine,
  SyncContext,
  SyncRulesBucketStorage
} from '@/index.js';
import { MeterProvider } from '@opentelemetry/sdk-metrics';
import { EventsEngine } from '@/events/EventsEngine.js';

export function mockServiceContext(storage: Partial<SyncRulesBucketStorage> | null) {
  // This is very incomplete - just enough to get the current tests passing.

  const storageEngine: StorageEngine = {
    activeBucketStorage: {
      async getActiveStorage() {
        return storage;
      }
    } as Partial<BucketStorageFactory>
  } as any;

  const meterProvider = new MeterProvider({
    readers: []
  });
  const meter = meterProvider.getMeter('powersync-tests');
  const metricsEngine = new MetricsEngine({
    disable_telemetry_sharing: true,
    factory: new OpenTelemetryMetricsFactory(meter)
  });
  createCoreAPIMetrics(metricsEngine);
  const service_context: Partial<ServiceContext> = {
    syncContext: new SyncContext({ maxBuckets: 1, maxDataFetchConcurrency: 1, maxParameterQueryResults: 1 }),
    eventsEngine: new EventsEngine(),
    routerEngine: {
      getAPI() {
        return {
          getParseSyncRulesOptions() {
            return { defaultSchema: 'public' };
          },
          async getSourceConfig() {
            return {
              tag: 'test_tag',
              id: 'test_id',
              type: 'test_type'
            };
          },
          async getConnectionSchema() {
            return [];
          },
          async getConnectionStatus() {
            return {
              id: 'test_id',
              uri: 'http://example.org/',
              connected: true,
              errors: []
            };
          },
          async getDebugTablesInfo() {
            return [];
          }
        } satisfies Partial<RouteAPI> as unknown as RouteAPI;
      },
      addStopHandler() {
        return () => {};
      }
    } as Partial<RouterEngine> as any,
    storageEngine,
    metricsEngine: metricsEngine,
    // Not used
    configuration: null as any,
    lifeCycleEngine: null as any,
    migrations: null as any,
    replicationEngine: null as any,
    serviceMode: null as any
  };
  return service_context as ServiceContext;
}
