import { ReplicationChildController } from '../../replication/ReplicationChildController.js';
import {
  ReplicationBenchmarkIterationResource,
  ReplicationBenchmarkIterationSetup,
  ReplicationBenchmarkObservation,
  ReplicationBenchmarkSourceAdapter,
  ReplicationBenchmarkTarget,
  ReplicationBenchmarkTransaction,
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

  async commitTransaction(transaction: ReplicationBenchmarkTransaction): Promise<ReplicationBenchmarkTarget> {
    return this.source.commitTransaction(transaction);
  }

  async keepalive(): Promise<ReplicationBenchmarkTarget> {
    return await this.source.keepalive();
  }

  async beginMeasurement(): Promise<void> {
    await this.controller.request('checkpoint_status', { resetMetrics: true }, this.setup.iterationId);
  }

  async pauseReplication(): Promise<void> {
    await this.controller.request('pause_replication', {}, this.setup.iterationId);
  }

  async resumeReplication(): Promise<void> {
    await this.controller.request('resume_replication', {}, this.setup.iterationId);
  }

  async collectEvidence(targetMarker: string) {
    return this.controller.request('collect_evidence', { targetMarker }, this.setup.iterationId);
  }

  async observeCheckpoint(options: {
    target: ReplicationBenchmarkTarget;
    releasedAtNs?: string;
  }): Promise<ReplicationBenchmarkObservation> {
    while (true) {
      const status = await this.controller.request('checkpoint_status', {}, this.setup.iterationId);
      if (
        status.snapshotDone &&
        status.checkpoint != null &&
        this.source.comparePosition(status.checkpoint, options.target).reached
      ) {
        return {
          target: options.target,
          checkpoint: status.checkpoint,
          checkpointVisibleAtNs: process.hrtime.bigint().toString(),
          snapshotDone: true,
          bucketCount: 0,
          operations: [],
          keepalives: 0,
          retries: 0,
          restarts: 0
        };
      }
      await new Promise((resolve) => setTimeout(resolve, 30));
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
