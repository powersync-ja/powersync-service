import { CombinedChildController } from '../../combined/CombinedChildController.js';
import { CombinedChildEvidencePayload } from '../../combined/combined-child-protocol.js';
import {
  CombinedBenchmarkIterationResource,
  CombinedBenchmarkIterationSetup,
  CombinedBenchmarkObservation,
  CombinedReleaseObservation
} from '../../types/CombinedBenchmark.js';
import {
  ReplicationBenchmarkSourceAdapter,
  ReplicationBenchmarkTarget,
  ReplicationPositionComparison,
  ReplicationReleaseObservation
} from '../../types/ReplicationBenchmark.js';
import { SnapshotBenchmarkTarget } from '../../types/SnapshotBenchmark.js';

export interface CombinedTargetEvidenceOptions {
  readonly target: SnapshotBenchmarkTarget;
  readonly collectEvidence: () => Promise<CombinedChildEvidencePayload>;
  readonly comparePosition: (checkpoint: string, target: SnapshotBenchmarkTarget) => ReplicationPositionComparison;
  readonly delay?: () => Promise<void>;
  readonly now?: () => bigint;
}

interface CombinedReplicationReleaseOptions {
  readonly release: () => Promise<ReplicationReleaseObservation>;
  readonly now?: () => bigint;
}

export async function releaseCombinedReplication(
  options: CombinedReplicationReleaseOptions
): Promise<CombinedReleaseObservation> {
  const parentReleasedAtNs = (options.now ?? (() => process.hrtime.bigint()))().toString();
  const childRelease = await options.release();
  return {
    parentReleasedAtNs,
    childReleasedAtNs: childRelease.releasedAtNs,
    snapshot: childRelease.snapshot
  };
}

export async function waitForCombinedTargetEvidence(
  options: CombinedTargetEvidenceOptions
): Promise<CombinedBenchmarkObservation> {
  const delay = options.delay ?? (() => new Promise((resolve) => setTimeout(resolve, 30)));
  const now = options.now ?? (() => process.hrtime.bigint());
  while (true) {
    const evidence = await options.collectEvidence();
    if (evidence.checkpoint == null) {
      await delay();
      continue;
    }
    const comparison = options.comparePosition(evidence.checkpoint, options.target);
    const marker = evidence.operations.find((operation) => operation.object_id === options.target.markerId);
    if (evidence.snapshotDone && comparison.comparable && comparison.reached && markerContainsTarget(marker)) {
      return {
        target: options.target,
        storageCheckpoint: evidence.storageCheckpoint,
        checkpoint: evidence.checkpoint,
        checkpointVisibleAtNs: now().toString(),
        childCheckpointVisibleAtNs: evidence.checkpointVisibleAtNs,
        operations: evidence.operations,
        snapshotDone: evidence.snapshotDone,
        bucketCount: evidence.bucketCount
      };
    }
    await delay();
  }
}

export class ControlledCombinedIterationResource implements CombinedBenchmarkIterationResource {
  private childCleanupComplete = false;
  private cleanupStarted = false;
  private sourceCleanupComplete = false;
  private disposeNotified = false;

  constructor(
    private readonly controller: CombinedChildController,
    private readonly setup: CombinedBenchmarkIterationSetup,
    private readonly source: ReplicationBenchmarkSourceAdapter,
    readonly target: ReplicationBenchmarkTarget,
    readonly endpoint: string,
    readonly token: string,
    private readonly onDispose: () => void
  ) {}

  async releaseReplication(waitForSnapshot: boolean = false): Promise<CombinedReleaseObservation> {
    return await releaseCombinedReplication({
      release: () => this.controller.request('release_replication', { waitForSnapshot }, this.setup.iterationId)
    });
  }

  async observeCheckpoint(): Promise<CombinedBenchmarkObservation> {
    return await waitForCombinedTargetEvidence({
      target: this.target,
      collectEvidence: () => this.controller.request('collect_evidence', {}, this.setup.iterationId),
      comparePosition: (checkpoint, target) => this.source.comparePosition(checkpoint, target)
    });
  }

  comparePosition(checkpoint: string, target: SnapshotBenchmarkTarget): ReplicationPositionComparison {
    return this.source.comparePosition(checkpoint, target);
  }

  async dispose(): Promise<void> {
    if (this.sourceCleanupComplete) return;
    this.cleanupStarted = true;
    const errors: unknown[] = [];

    if (!this.childCleanupComplete) {
      try {
        await this.controller.request('cleanup_iteration', {}, this.setup.iterationId);
        this.childCleanupComplete = true;
      } catch (error) {
        errors.push(error);
      }
    }

    if (this.childCleanupComplete) {
      try {
        await this.cleanupSource();
      } catch (error) {
        errors.push(error);
      }
    }

    if (errors.length > 0) throw new AggregateError(errors, 'Controlled combined iteration cleanup failed');
  }

  /** Called by the run resource only after the child is known to have stopped. */
  async disposeAfterChildStopped(): Promise<void> {
    this.childCleanupComplete = true;
    if (this.sourceCleanupComplete) return;
    try {
      await this.cleanupSource();
    } catch (error) {
      throw new AggregateError([error], 'Controlled combined source cleanup failed after child shutdown');
    }
  }

  get cleanupAttempted(): boolean {
    return this.cleanupStarted;
  }

  private async cleanupSource(): Promise<void> {
    if (this.sourceCleanupComplete) return;
    await this.source.cleanup();
    this.sourceCleanupComplete = true;
    if (!this.disposeNotified) {
      this.disposeNotified = true;
      this.onDispose();
    }
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
