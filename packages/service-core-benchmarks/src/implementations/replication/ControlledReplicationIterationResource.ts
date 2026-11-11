import { ReplicationChildController } from '../../replication/ReplicationChildController.js';
import {
  ReplicationBenchmarkEvidence,
  ReplicationBenchmarkIterationResource,
  ReplicationBenchmarkIterationSetup,
  ReplicationBenchmarkObservation,
  ReplicationBenchmarkSourceAdapter,
  ReplicationBenchmarkTarget,
  ReplicationPositionComparison,
  ReplicationReleaseObservation
} from '../../types/ReplicationBenchmark.js';

export class ControlledReplicationIterationResource implements ReplicationBenchmarkIterationResource {
  private disposed = false;

  constructor(
    private readonly controller: ReplicationChildController,
    private readonly setup: ReplicationBenchmarkIterationSetup,
    private readonly source: ReplicationBenchmarkSourceAdapter,
    private readonly snapshotTarget: ReplicationBenchmarkTarget,
    private readonly onDispose: () => void
  ) {}

  async releaseReplication(waitForSnapshot: boolean = false): Promise<ReplicationReleaseObservation> {
    const release = await this.controller.request('release_replication', { waitForSnapshot }, this.setup.iterationId);
    return { ...release, target: this.snapshotTarget };
  }

  async commitTransaction(transactionId: string): Promise<ReplicationBenchmarkTarget> {
    const transaction = this.setup.manifest.transactions.byId(transactionId);
    if (transaction == null) throw new Error(`Unknown benchmark transaction ${transactionId}`);
    return await this.source.commitTransaction(transaction);
  }

  async keepalive(): Promise<ReplicationBenchmarkTarget> {
    return await this.source.keepalive();
  }

  async collectEvidence(): Promise<ReplicationBenchmarkEvidence> {
    return await this.controller.request('collect_evidence', {}, this.setup.iterationId);
  }

  async waitForCheckpoint(options: {
    target: ReplicationBenchmarkTarget;
    releasedAtNs?: string;
    signal: AbortSignal;
  }): Promise<ReplicationBenchmarkObservation> {
    const { signal, target } = options;
    signal.throwIfAborted();
    let cancellation: Promise<unknown> | undefined;
    const cancel = () => {
      cancellation = Promise.resolve().then(() =>
        this.controller.request('cancel_checkpoint_wait', {}, this.setup.iterationId)
      );
      // The pending wait also fails if the child exits.
      void cancellation.catch(() => {});
    };
    signal.addEventListener('abort', cancel, { once: true });
    try {
      let after: string | undefined;
      while (true) {
        const checkpoint = await this.controller.request('wait_checkpoint', { after }, this.setup.iterationId);
        signal.throwIfAborted();
        if (checkpoint == null) throw new Error('Checkpoint watcher ended before reaching the target');
        if (this.source.comparePosition(checkpoint.position, target).reached) {
          return {
            target,
            checkpoint: checkpoint.position,
            checkpointVisibleAtNs: checkpoint.visibleAtNs,
            replicationReleasedAtNs: options.releasedAtNs,
            keepalives: 0,
            retries: 0,
            restarts: 0
          };
        }
        after = checkpoint.position;
      }
    } finally {
      signal.removeEventListener('abort', cancel);
      await cancellation;
    }
  }

  comparePosition(checkpoint: string, target: ReplicationBenchmarkTarget): ReplicationPositionComparison {
    return this.source.comparePosition(checkpoint, target);
  }

  async dispose(): Promise<void> {
    if (this.disposed) return;
    this.disposed = true;
    const errors: unknown[] = [];
    try {
      await this.controller.request('cleanup_iteration', {}, this.setup.iterationId);
    } catch (error) {
      errors.push(error);
      // Stop replication before source cleanup can drop the slot and schema.
      try {
        await this.controller.shutdown();
      } catch (shutdownError) {
        errors.push(shutdownError);
      }
    }
    try {
      await this.source.cleanup();
    } catch (error) {
      errors.push(error);
    } finally {
      this.onDispose();
    }
    if (errors.length > 0) throw new AggregateError(errors, 'Controlled replication iteration cleanup failed');
  }
}
