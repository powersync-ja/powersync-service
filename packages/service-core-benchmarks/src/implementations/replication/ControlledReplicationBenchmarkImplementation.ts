import { ReplicationChildResourceMonitor } from '../../monitors/ReplicationChildResourceMonitor.js';
import { ForkedReplicationChildTransport } from '../../replication/ForkedReplicationChildTransport.js';
import { ReplicationChildClassDescriptor } from '../../replication/ReplicationChildClassLoader.js';
import { ReplicationChildController, ReplicationChildTransport } from '../../replication/ReplicationChildController.js';
import {
  ReplicationBenchmarkImplementation,
  ReplicationBenchmarkProducerId,
  ReplicationBenchmarkRunResource,
  ReplicationBenchmarkSourceAdapter
} from '../../types/ReplicationBenchmark.js';
import { StorageBenchmarkImplementationId } from '../../types/StorageBenchmark.js';
import { ControlledReplicationIterationResource } from './ControlledReplicationIterationResource.js';

export interface ReplicationBenchmarkSourceSelection {
  readonly id: ReplicationBenchmarkProducerId;
  resolveIterationFactory(environment: Readonly<Record<string, string | undefined>>): () => {
    readonly adapter: ReplicationBenchmarkSourceAdapter;
    createChildDescriptor(): ReplicationChildClassDescriptor;
  };
}

export interface ReplicationBenchmarkStorageSelection {
  readonly id: StorageBenchmarkImplementationId;
  readonly version: number;
  createChildDescriptor(environment: Readonly<Record<string, string | undefined>>): ReplicationChildClassDescriptor;
}

export interface ControlledReplicationBenchmarkImplementationOptions {
  readonly source: ReplicationBenchmarkSourceSelection;
  readonly storage: ReplicationBenchmarkStorageSelection;
  validateEnvironment?(environment: Readonly<Record<string, string | undefined>>): void;
  readonly environment?: Readonly<Record<string, string | undefined>>;
  readonly transportFactory?: () => ReplicationChildTransport;
}

export class ControlledReplicationBenchmarkImplementation implements ReplicationBenchmarkImplementation {
  readonly sourceId;
  readonly storageId;
  readonly storageVersion;
  private activeController?: ReplicationChildController;

  constructor(private readonly options: ControlledReplicationBenchmarkImplementationOptions) {
    this.sourceId = options.source.id;
    this.storageId = options.storage.id;
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
    const fixtureEnvironment = this.options.environment ?? process.env;
    const createIterationSource = this.options.source.resolveIterationFactory(fixtureEnvironment);
    const storageDescriptor = this.options.storage.createChildDescriptor(fixtureEnvironment);
    this.options.validateEnvironment?.(fixtureEnvironment);
    const environment: Record<string, unknown> = {};
    let activeIteration: ControlledReplicationIterationResource | undefined;
    let disposed = false;

    const transport = this.options.transportFactory?.() ?? new ForkedReplicationChildTransport();
    const controller = new ReplicationChildController(transport, { runId });
    this.activeController = controller;
    const onAbort = () => transport.kill('SIGTERM');
    signal.addEventListener('abort', onAbort, { once: true });

    try {
      const initialized = await controller.request('initialize', {
        storage: storageDescriptor
      });
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

        const iterationSource = createIterationSource();
        const source = iterationSource.adapter;
        let setupSent = false;
        try {
          if (source.id !== this.sourceId) {
            throw new Error(`Replication source fixture ${this.sourceId} created adapter ${source.id}`);
          }
          await source.createSchema(setup.iterationId);
          const snapshotTarget = await source.populateSnapshot(setup.manifest);
          await source.prepareTransactions(setup.manifest);
          Object.assign(environment, await source.collectMetadata());
          const sourceDescriptor = iterationSource.createChildDescriptor();
          setupSent = true;
          const { replicationStreamName } = await controller.request(
            'setup_iteration',
            {
              syncRules: setup.scenario.syncRule(source.sourceTable),
              syncParameters: setup.scenario.sync_parameters,
              storageVersion: setup.scenario.storage.version,
              source: sourceDescriptor
            },
            setup.iterationId
          );
          source.setReplicationStreamName(replicationStreamName);
          activeIteration = new ControlledReplicationIterationResource(
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
            : new AggregateError(errors, 'Replication iteration setup and cleanup failed');
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
