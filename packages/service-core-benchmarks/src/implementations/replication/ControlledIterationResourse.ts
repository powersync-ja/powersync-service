import { ReplicationChildController } from '../../fixtures/replication/ReplicationChildController.js';
import {
  ReplicationBenchmarkIterationResource,
  ReplicationBenchmarkIterationSetup,
  ReplicationBenchmarkObservation,
  ReplicationBenchmarkTarget,
  ReplicationReleaseObservation
} from '../../types/ReplicationBenchmark.js';

export class ControlledIterationResource implements ReplicationBenchmarkIterationResource {
  private disposed = false;

  constructor(
    private readonly controller: ReplicationChildController,
    private readonly setup: ReplicationBenchmarkIterationSetup,
    private readonly onDispose: () => void
  ) {}

  async releaseReplication(waitForSnapshot: boolean = false): Promise<ReplicationReleaseObservation> {
    return await this.controller.request('release_replication', { waitForSnapshot }, this.setup.iterationId);
  }

  async commitTransaction(transactionId: string): Promise<ReplicationBenchmarkTarget> {
    return await this.controller.request('commit_transaction', { transactionId }, this.setup.iterationId);
  }

  async keepalive(): Promise<ReplicationBenchmarkTarget> {
    const response = await this.controller.request<{ target: ReplicationBenchmarkTarget }>(
      'keepalive',
      {},
      this.setup.iterationId
    );
    return response.target;
  }

  async observeCheckpoint(options: {
    target: ReplicationBenchmarkTarget;
    releasedAtNs?: string;
  }): Promise<ReplicationBenchmarkObservation> {
    return await this.controller.request(
      'observe_checkpoint',
      { markerId: options.target.markerId, ...options },
      this.setup.iterationId
    );
  }

  comparePosition(checkpoint: string, target: ReplicationBenchmarkTarget) {
    if (target.nativePosition == null) return { comparable: false, reached: true };
    return {
      comparable: true,
      reached: checkpoint >= target.nativePosition,
      details: { checkpoint, target: target.nativePosition, semantics: 'ordered-synthetic-position' }
    };
  }

  async dispose(): Promise<void> {
    if (this.disposed) return;
    this.disposed = true;
    try {
      await this.controller.request('cleanup_iteration', {}, this.setup.iterationId);
    } finally {
      this.onDispose();
    }
  }
}
