import { CombinedChildController, CombinedChildTransport } from '../../combined/CombinedChildController.js';
import { ForkedCombinedChildTransport } from '../../combined/ForkedCombinedChildTransport.js';
import { CombinedChildResourceMonitor } from '../../monitors/CombinedChildResourceMonitor.js';
import { CombinedBenchmarkImplementation, CombinedBenchmarkRunResource } from '../../types/CombinedBenchmark.js';
import { ReplicationBenchmarkSourceAdapter } from '../../types/ReplicationBenchmark.js';
import { createBenchmarkKey, createToken, reservePort } from '../../utils/api-utils.js';
import type {
  ReplicationBenchmarkSourceSelection,
  ReplicationBenchmarkStorageSelection
} from '../replication/ControlledReplicationBenchmarkImplementation.js';
import { ControlledCombinedIterationResource } from './ControlledCombinedIterationResource.js';

export interface ControlledCombinedBenchmarkImplementationOptions {
  readonly source: ReplicationBenchmarkSourceSelection;
  readonly storage: ReplicationBenchmarkStorageSelection;
  validateEnvironment?(environment: Readonly<Record<string, string | undefined>>): void;
  readonly environment?: Readonly<Record<string, string | undefined>>;
  readonly transportFactory?: () => CombinedChildTransport;
}

export class ControlledCombinedBenchmarkImplementation implements CombinedBenchmarkImplementation {
  readonly sourceId;
  readonly storageId;
  readonly storageVersion;
  private activeController?: CombinedChildController;

  constructor(private readonly options: ControlledCombinedBenchmarkImplementationOptions) {
    this.sourceId = options.source.id;
    this.storageId = options.storage.id;
    this.storageVersion = options.storage.version;
  }

  createServiceMonitor(): CombinedChildResourceMonitor {
    return new CombinedChildResourceMonitor(() => {
      if (this.activeController == null) throw new Error('Combined child is not active');
      return this.activeController;
    });
  }

  async open(signal: AbortSignal, runId: string): Promise<CombinedBenchmarkRunResource> {
    signal.throwIfAborted();
    const fixtureEnvironment = this.options.environment ?? process.env;
    const createIterationSource = this.options.source.resolveIterationFactory(fixtureEnvironment);
    const storageDescriptor = this.options.storage.createChildDescriptor(fixtureEnvironment);
    this.options.validateEnvironment?.(fixtureEnvironment);
    const environment: Record<string, unknown> = {};
    let activeIteration: ControlledCombinedIterationResource | undefined;
    let orphanedSource: ReplicationBenchmarkSourceAdapter | undefined;
    let cleanupStarted = false;
    let disposed = false;

    const transport = this.options.transportFactory?.() ?? new ForkedCombinedChildTransport();
    const controller = new CombinedChildController(transport, { runId });
    this.activeController = controller;
    const onAbort = () => transport.kill('SIGTERM');
    signal.addEventListener('abort', onAbort, { once: true });

    try {
      const initialized = await controller.request('initialize', { storage: storageDescriptor });
      Object.assign(environment, initialized.environment, {
        child_pid: initialized.pid,
        source_implementation: this.sourceId,
        storage_implementation: this.storageId,
        storage_version: this.storageVersion,
        service_mode: 'unified'
      });
    } catch (error) {
      signal.removeEventListener('abort', onAbort);
      this.activeController = undefined;
      try {
        await controller.shutdown();
      } catch (cleanupError) {
        throw new AggregateError([error, cleanupError], 'Combined run initialization and child cleanup failed');
      }
      throw error;
    }

    return {
      environment,
      createIteration: async (setup) => {
        if (cleanupStarted) throw new Error('Combined benchmark run resource cleanup has started');
        if (activeIteration != null || orphanedSource != null) {
          throw new Error('A combined benchmark iteration is already active');
        }
        signal.throwIfAborted();

        const iterationSource = createIterationSource();
        const source = iterationSource.adapter;
        let setupSent = false;
        try {
          if (source.id !== this.sourceId) {
            throw new Error(`Combined source fixture ${this.sourceId} created adapter ${source.id}`);
          }
          if (setup.scenario.producer !== this.sourceId) {
            throw new Error(
              `Combined source implementation ${this.sourceId} does not match scenario ${setup.scenario.producer}`
            );
          }
          if (
            setup.scenario.storage.implementation !== this.storageId ||
            setup.scenario.storage.version !== this.storageVersion
          ) {
            throw new Error(
              `Combined storage implementation ${this.storageId}@${this.storageVersion} does not match scenario ${setup.scenario.storage.implementation}@${setup.scenario.storage.version}`
            );
          }

          await source.createSchema(setup.iterationId);
          const snapshotTarget = await source.populateSnapshot(setup.manifest);
          Object.assign(environment, await source.collectMetadata());

          const key = await createBenchmarkKey();
          const [port, token] = await Promise.all([reservePort(), createToken(key.signingKey)]);
          const sourceDescriptor = iterationSource.createChildDescriptor();
          setupSent = true;
          const childSetup = await controller.request(
            'setup_iteration',
            {
              syncRules: setup.scenario.syncRule(source.sourceTable),
              syncParameters: setup.scenario.sync_parameters,
              storageVersion: setup.scenario.storage.version,
              source: sourceDescriptor,
              port,
              jwk: key.jwk
            },
            setup.iterationId
          );
          source.setReplicationStreamName(childSetup.replicationStreamName);

          activeIteration = new ControlledCombinedIterationResource(
            controller,
            setup,
            source,
            snapshotTarget,
            childSetup.endpoint,
            token,
            () => (activeIteration = undefined)
          );
          return activeIteration;
        } catch (error) {
          if (setupSent) {
            orphanedSource = source;
            throw error;
          }

          try {
            await source.cleanup();
          } catch (cleanupError) {
            orphanedSource = source;
            throw new AggregateError([error, cleanupError], 'Combined iteration setup and source cleanup failed');
          }
          throw error;
        }
      },
      dispose: async () => {
        if (disposed) return;
        cleanupStarted = true;
        const errors: unknown[] = [];
        const iterationToFinalize = activeIteration;

        if (iterationToFinalize != null && !iterationToFinalize.cleanupAttempted) {
          try {
            await iterationToFinalize.dispose();
          } catch (error) {
            errors.push(error);
          }
        }

        try {
          await controller.shutdown();
        } catch (error) {
          errors.push(error);
        }

        if (controller.hasExited) {
          signal.removeEventListener('abort', onAbort);
          this.activeController = undefined;
          if (iterationToFinalize != null) {
            try {
              await iterationToFinalize.disposeAfterChildStopped();
            } catch (error) {
              errors.push(error);
            }
          }
          if (orphanedSource != null) {
            try {
              await orphanedSource.cleanup();
              orphanedSource = undefined;
            } catch (error) {
              errors.push(error);
            }
          }
        }

        if (errors.length > 0) throw new AggregateError(errors, 'Controlled combined run cleanup failed');
        disposed = true;
      }
    };
  }
}
