import { ReplicationChildController } from '../../replication/ReplicationChildController.js';
import { ReplicationChildEvidencePayload } from '../../replication/replication-child-protocol.js';
import {
  ReplicationBenchmarkIterationResource,
  ReplicationBenchmarkIterationSetup,
  ReplicationBenchmarkObservation,
  ReplicationBenchmarkSourceAdapter,
  ReplicationBenchmarkTarget,
  ReplicationPositionComparison,
  ReplicationReleaseObservation
} from '../../types/ReplicationBenchmark.js';

export interface TargetEvidenceOptions {
  readonly target: ReplicationBenchmarkTarget;
  readonly releasedAtNs?: string;
  readonly collectEvidence: () => Promise<ReplicationChildEvidencePayload>;
  readonly comparePosition: (checkpoint: string, target: ReplicationBenchmarkTarget) => ReplicationPositionComparison;
  readonly delay?: () => Promise<void>;
}

export async function waitForTargetEvidence(options: TargetEvidenceOptions): Promise<ReplicationBenchmarkObservation> {
  const delay = options.delay ?? (() => new Promise((resolve) => setTimeout(resolve, 30)));
  while (true) {
    const evidence = await options.collectEvidence();
    if (evidence.checkpoint == null) {
      await delay();
      continue;
    }
    const comparison = options.comparePosition(evidence.checkpoint, options.target);
    const marker = evidence.operations.find((operation) => operation.object_id === options.target.markerId);
    if (comparison.reached && markerContainsTarget(marker)) {
      return {
        target: options.target,
        checkpoint: evidence.checkpoint,
        checkpointVisibleAtNs: process.hrtime.bigint().toString(),
        replicationReleasedAtNs: options.releasedAtNs,
        operations: evidence.operations,
        snapshotDone: evidence.snapshotDone,
        bucketCount: evidence.bucketCount,
        keepalives: 0,
        retries: 0,
        restarts: 0
      };
    }
    await delay();
  }
}

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
    const transaction = this.setup.manifest.transactions.find((candidate) => candidate.id === transactionId);
    if (transaction == null) throw new Error(`Unknown benchmark transaction ${transactionId}`);
    return await this.source.commitTransaction(transaction);
  }

  async keepalive(): Promise<ReplicationBenchmarkTarget> {
    return await this.source.keepalive();
  }

  async observeCheckpoint(options: {
    target: ReplicationBenchmarkTarget;
    releasedAtNs?: string;
  }): Promise<ReplicationBenchmarkObservation> {
    return await waitForTargetEvidence({
      ...options,
      collectEvidence: () => this.controller.request('collect_evidence', {}, this.setup.iterationId),
      comparePosition: (checkpoint, target) => this.source.comparePosition(checkpoint, target)
    });
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

function markerContainsTarget(operation: { op: string; data?: string | null } | undefined): boolean {
  if (operation?.op !== 'PUT' || typeof operation.data !== 'string') return false;
  try {
    return (JSON.parse(operation.data) as { is_target?: number }).is_target === 1;
  } catch {
    return false;
  }
}
