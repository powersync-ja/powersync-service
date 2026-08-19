import { ReplicationChildResourceMonitor } from '../../../monitors/ReplicationChildResourceMonitor.js';
import { ForkedReplicationChildTransport } from '../../../replication/ForkedReplicationChildTransport.js';
import {
  ReplicationChildController,
  ReplicationChildTransport
} from '../../../replication/ReplicationChildController.js';
import {
  ReplicationBenchmarkImplementation,
  ReplicationBenchmarkRunResource
} from '../../../types/ReplicationBenchmark.js';
import { StorageBenchmarkImplementationId } from '../../../types/StorageBenchmark.js';
import { ControlledMongoIterationResource } from './ControlledMongoIterationResource.js';
import { MongoReplicationSourceAdapter } from './MongoReplicationSourceAdapter.js';
import { resolveMongoSourceBenchmarkConfiguration } from './MongoSourceBenchmarkConfiguration.js';

export interface ControlledMongoReplicationBenchmarkImplementationOptions {
  readonly storage: {
    readonly implementation: StorageBenchmarkImplementationId;
    readonly version: number;
    readonly isCI?: boolean;
  };
  readonly environment?: Readonly<Record<string, string | undefined>>;
  readonly transportFactory?: () => ReplicationChildTransport;
}

export class ControlledMongoReplicationBenchmarkImplementation implements ReplicationBenchmarkImplementation {
  readonly sourceId = 'mongodb-source' as const;
  readonly storageId: StorageBenchmarkImplementationId;
  readonly storageVersion: number;
  private activeController?: ReplicationChildController;

  constructor(private readonly options: ControlledMongoReplicationBenchmarkImplementationOptions) {
    this.storageId = options.storage.implementation;
    this.storageVersion = options.storage.version;
  }

  createServiceMonitor(): ReplicationChildResourceMonitor {
    return new ReplicationChildResourceMonitor(() => {
      if (this.activeController == null) throw new Error('Replication child is not active');
      return this.activeController;
    });
  }

  async open(signal: AbortSignal, runId: string): Promise<ReplicationBenchmarkRunResource> {
    signal.throwIfAborted();
    const urls = resolveMongoSourceBenchmarkConfiguration(
      this.options.storage.implementation,
      this.options.environment ?? process.env
    );
    const environment: Record<string, unknown> = {};
    let activeIteration: ControlledMongoIterationResource | undefined;
    let disposed = false;

    const transport = this.options.transportFactory?.() ?? new ForkedReplicationChildTransport();
    const controller = new ReplicationChildController(transport, { runId });
    this.activeController = controller;
    const onAbort = () => transport.kill('SIGTERM');
    signal.addEventListener('abort', onAbort, { once: true });

    try {
      const storage =
        this.options.storage.implementation === 'postgres-storage'
          ? { implementation: 'postgres-storage' as const, url: urls.storageUrl }
          : {
              implementation: 'mongodb-storage' as const,
              url: urls.storageUrl,
              isCI: this.options.storage.isCI ?? process.env.CI === 'true'
            };
      const initialized = await controller.request<{ environment: object; pid: number }>('initialize', { storage });
      Object.assign(environment, initialized.environment, {
        child_pid: initialized.pid,
        source_implementation: this.sourceId,
        storage_implementation: this.storageId
      });
    } catch (error) {
      signal.removeEventListener('abort', onAbort);
      transport.kill('SIGKILL');
      this.activeController = undefined;
      throw error;
    }

    return {
      environment,
      createIteration: async (setup) => {
        if (disposed) throw new Error('Replication benchmark run resource is disposed');
        if (activeIteration != null) throw new Error('A replication benchmark iteration is already active');
        signal.throwIfAborted();

        const source = new MongoReplicationSourceAdapter(urls.sourceUrl);
        let setupSent = false;
        try {
          await source.createSchema(setup.iterationId);
          const snapshotTarget = await source.populateSnapshot(setup.manifest);
          await source.prepareTransactions(setup.manifest);
          Object.assign(environment, await source.collectMetadata());
          setupSent = true;
          await controller.request(
            'setup_iteration',
            {
              manifest: setup.manifest,
              syncRules: createSyncRules(setup.iterationId),
              storageVersion: setup.scenario.storage.version,
              source: { implementation: this.sourceId, ...source.sourceConfig }
            },
            setup.iterationId
          );
          activeIteration = new ControlledMongoIterationResource(
            controller,
            setup,
            source,
            snapshotTarget,
            () => (activeIteration = undefined)
          );
          return activeIteration;
        } catch (error) {
          const errors: unknown[] = [error];
          try {
            await source.cleanup();
          } catch (cleanupError) {
            errors.push(cleanupError);
          }
          if (setupSent) {
            transport.kill('SIGKILL');
            this.activeController = undefined;
          }
          throw errors.length === 1
            ? error
            : new AggregateError(errors, 'MongoDB replication iteration setup and cleanup failed');
        }
      },
      dispose: async () => {
        if (disposed) return;
        disposed = true;
        const errors: unknown[] = [];
        try {
          await activeIteration?.dispose();
        } catch (error) {
          errors.push(error);
        }
        try {
          await controller.shutdown();
        } catch (error) {
          transport.kill('SIGKILL');
          errors.push(error);
        } finally {
          signal.removeEventListener('abort', onAbort);
          this.activeController = undefined;
        }
        if (errors.length > 0) throw new AggregateError(errors, 'Controlled replication run cleanup failed');
      }
    };
  }
}

function createSyncRules(iterationId: string): string {
  return `
# ${iterationId}
bucket_definitions:
  global:
    data:
      - SELECT id, owner_id, category, version, updated_at, payload, is_target FROM benchmark_items
`;
}
