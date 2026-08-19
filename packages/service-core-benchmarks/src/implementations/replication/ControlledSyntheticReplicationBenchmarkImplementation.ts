import { ReplicationChildResourceMonitor } from '../../monitors/ReplicationChildResourceMonitor.js';
import { ForkedReplicationChildTransport } from '../../replication/ForkedReplicationChildTransport.js';
import { ReplicationChildController, ReplicationChildTransport } from '../../replication/ReplicationChildController.js';
import {
  ReplicationBenchmarkImplementation,
  ReplicationBenchmarkRunResource
} from '../../types/ReplicationBenchmark.js';
import { ControlledIterationResource } from './ControlledIterationResourse.js';

type ControlledStorageOptions =
  | {
      readonly implementation: 'postgres-storage';
      readonly version: number;
      readonly url: string;
    }
  | {
      readonly implementation: 'mongodb-storage';
      readonly version: number;
      readonly url: string;
      readonly isCI: boolean;
    };

export interface ControlledSyntheticReplicationBenchmarkImplementationOptions {
  readonly storage: ControlledStorageOptions;
  readonly transportFactory?: () => ReplicationChildTransport;
}

export class ControlledSyntheticReplicationBenchmarkImplementation implements ReplicationBenchmarkImplementation {
  readonly sourceId = 'synthetic-source' as const;
  readonly storageId: ControlledStorageOptions['implementation'];
  readonly storageVersion: number;
  private activeController?: ReplicationChildController;

  constructor(private readonly options: ControlledSyntheticReplicationBenchmarkImplementationOptions) {
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
    const environment: Record<string, unknown> = {};

    let activeIteration: ControlledIterationResource | undefined;
    let disposed = false;

    const transport = this.options.transportFactory?.() ?? new ForkedReplicationChildTransport();
    const controller = new ReplicationChildController(transport, { runId });
    this.activeController = controller;

    const onAbort = () => transport.kill('SIGTERM');

    signal.addEventListener('abort', onAbort, { once: true });

    try {
      const initialized = await controller.request<{ environment: object; pid: number }>('initialize', {
        storage: this.options.storage
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

    //TODO: Move to Util
    return {
      environment,
      createIteration: async (setup) => {
        if (disposed) throw new Error('Replication benchmark run resource is disposed');
        if (activeIteration != null) throw new Error('A replication benchmark iteration is already active');
        signal.throwIfAborted();
        try {
          await controller.request(
            'setup_iteration',
            {
              manifest: setup.manifest,
              syncRules: createSyncRules(setup.iterationId),
              storageVersion: setup.scenario.storage.version
            },
            setup.iterationId
          );
          activeIteration = new ControlledIterationResource(controller, setup, () => {
            activeIteration = undefined;
          });
          return activeIteration;
        } catch (error) {
          transport.kill('SIGKILL');
          this.activeController = undefined;
          throw error;
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

//TODO: Move to Util
function createSyncRules(iterationId: string): string {
  return `
# ${iterationId}
bucket_definitions:
  global:
    data:
      - SELECT id, owner_id, category, version, updated_at, payload, is_target FROM benchmark_items
`;
}
