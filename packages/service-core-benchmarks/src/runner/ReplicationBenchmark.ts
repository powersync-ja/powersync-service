import { generateBaselineReplicationManifest } from '../generators/generate-baseline-replication-manifest.js';
import { monotonicMilliseconds } from '../replication/replication-child-protocol.js';
import { BenchmarkCorrectnessCheck, BenchmarkCorrectnessResult } from '../types/BenchmarkIteration.js';
import { BenchmarkRunOptions } from '../types/BenchmarkRunOptions.js';
import {
  ReplicationBenchmarkImplementation,
  ReplicationBenchmarkIterationContext,
  ReplicationBenchmarkIterationResource,
  ReplicationBenchmarkObservation,
  ReplicationBenchmarkRunContext,
  ReplicationBenchmarkRunResource,
  ReplicationBenchmarkScenario,
  ReplicationBenchmarkTarget
} from '../types/ReplicationBenchmark.js';
import { Benchmark } from './Benchmark.js';

interface IterationState extends ReplicationBenchmarkIterationContext<ReplicationBenchmarkIterationResource> {
  keepalives: number;
}

export class ReplicationBenchmark extends Benchmark<
  ReplicationBenchmarkScenario,
  ReplicationBenchmarkRunContext<ReplicationBenchmarkRunResource>,
  IterationState,
  ReplicationBenchmarkObservation
> {
  constructor(
    scenario: ReplicationBenchmarkScenario,
    private readonly implementation: ReplicationBenchmarkImplementation,
    runOptions: BenchmarkRunOptions
  ) {
    super(scenario, runOptions);
  }

  protected async setupRun(
    signal: AbortSignal
  ): Promise<ReplicationBenchmarkRunContext<ReplicationBenchmarkRunResource>> {
    if (this.scenario.phase === 'catch-up') {
      throw new Error('Replication catch-up is not supported by the current benchmark implementation');
    }
    if (this.implementation.sourceId !== this.scenario.producer) {
      throw new Error(`Source implementation ${this.implementation.sourceId} does not match ${this.scenario.producer}`);
    }
    if (
      this.implementation.storageId !== this.scenario.storage.implementation ||
      this.implementation.storageVersion !== this.scenario.storage.version
    ) {
      throw new Error('Replication storage implementation does not match the resolved scenario');
    }
    return { resource: await this.implementation.open(signal, this.runOptions.runId) };
  }

  protected async setupIteration(
    run: ReplicationBenchmarkRunContext<ReplicationBenchmarkRunResource>,
    runtime: IterationState['runtime']
  ): Promise<IterationState> {
    runtime.signal.throwIfAborted();
    const manifest = generateBaselineReplicationManifest(this.scenario);
    const resource = await run.resource.createIteration({
      iterationId: `${this.runOptions.runId}-${runtime.kind}-${runtime.iteration}`,
      scenario: this.scenario,
      manifest
    });
    const context: IterationState = { runtime, resource, manifest, keepalives: 0 };
    if (this.scenario.phase === 'streaming') {
      await resource.releaseReplication(true);
    }
    return context;
  }

  protected async executeIteration(context: IterationState): Promise<ReplicationBenchmarkObservation> {
    const { runtime, resource, manifest } = context;
    runtime.signal.throwIfAborted();
    let target: ReplicationBenchmarkTarget;
    let startAtNs: string;
    let startEvent: string;
    let releasedAtNs: string | undefined;

    if (this.scenario.phase === 'snapshot') {
      const release = await resource.releaseReplication(false);
      target = release.target ?? manifest.target;
      startAtNs = release.releasedAtNs;
      startEvent = 'replication_released';
      releasedAtNs = release.releasedAtNs;
    } else {
      let firstTarget: ReplicationBenchmarkTarget | undefined;
      let lastTarget: ReplicationBenchmarkTarget | undefined;
      for (const transaction of manifest.transactions) {
        const committed = await resource.commitTransaction(transaction.id);
        firstTarget ??= committed;
        lastTarget = committed;
      }
      if (firstTarget?.committedAtNs == null || lastTarget == null) {
        throw new Error('Streaming workload did not produce a committed target');
      }
      target = lastTarget;
      startAtNs = firstTarget.committedAtNs;
      startEvent = 'source_committed';
    }

    const observation = await resource.observeCheckpoint({ target, releasedAtNs });
    runtime.metrics.recordBoundary(
      this.scenario.phase === 'snapshot' ? 'replication_snapshot' : 'replication_streaming',
      startEvent,
      'checkpoint_visible',
      monotonicMilliseconds(startAtNs),
      monotonicMilliseconds(observation.checkpointVisibleAtNs)
    );
    const sourceRows =
      this.scenario.phase === 'snapshot'
        ? manifest.snapshotRows.length
        : manifest.transactions.reduce((total, transaction) => total + transaction.mutations.length, 0);
    runtime.metrics.setCounter('source_rows', sourceRows);
    runtime.metrics.setCounter(
      'source_transactions',
      this.scenario.phase === 'snapshot' ? 0 : manifest.transactions.length
    );
    runtime.metrics.setCounter('source_logical_bytes', manifest.sourceLogicalBytes);
    runtime.metrics.setCounter('payload_bytes', manifest.payloadBytes);
    runtime.metrics.setCounter('writer_save_calls', manifest.expectedPutCount);
    runtime.metrics.setCounter('bucket_operations', observation.operations.length);
    runtime.metrics.setCounter('parameter_operations', 0);
    runtime.metrics.setCounter('distinct_buckets', observation.bucketCount);
    runtime.metrics.setCounter(
      'visible_checkpoints',
      this.scenario.phase === 'snapshot' ? 1 : manifest.transactions.length + 1
    );
    runtime.metrics.setCounter('target_markers', this.scenario.phase === 'snapshot' ? 1 : manifest.transactions.length);
    runtime.metrics.setCounter('keepalives', observation.keepalives + context.keepalives);
    runtime.metrics.setCounter('retries', observation.retries);
    runtime.metrics.setCounter('restarts', observation.restarts);
    return observation;
  }

  protected async verifyIteration(
    observation: ReplicationBenchmarkObservation,
    context: IterationState
  ): Promise<BenchmarkCorrectnessResult> {
    const comparison = context.resource.comparePosition(observation.checkpoint, observation.target);
    const puts = observation.operations.filter((operation) => operation.op === 'PUT');
    const marker = observation.operations.find((operation) => operation.object_id === observation.target.markerId);
    const checks: BenchmarkCorrectnessCheck[] = [
      check('snapshot_complete', observation.snapshotDone, { actual: observation.snapshotDone }),
      check('checkpoint_position', comparison.reached, comparison),
      check('bucket_count', observation.bucketCount === this.scenario.expected_bucket_count, {
        expected: this.scenario.expected_bucket_count,
        actual: observation.bucketCount
      }),
      check('operation_count', observation.operations.length === this.scenario.expected_bucket_operation_count, {
        expected: this.scenario.expected_bucket_operation_count,
        actual: observation.operations.length
      }),
      check('put_operation_count', puts.length === this.scenario.expected_bucket_operation_count, {
        expected: this.scenario.expected_bucket_operation_count,
        actual: puts.length
      }),
      check('target_marker_visible', markerContainsTarget(marker), {
        marker_id: observation.target.markerId,
        actual: marker ?? null
      })
    ];
    return { passed: checks.every((candidate) => candidate.passed), checks };
  }

  protected async cleanupIteration(context: IterationState): Promise<void> {
    await context.resource.dispose();
  }

  protected async collectRunMetadata(
    run: ReplicationBenchmarkRunContext<ReplicationBenchmarkRunResource>
  ): Promise<object> {
    return {
      ...run.resource.environment,
      producer: this.scenario.producer,
      phase: this.scenario.phase,
      storage_version: this.scenario.storage.version
    };
  }

  protected async cleanupRun(run: ReplicationBenchmarkRunContext<ReplicationBenchmarkRunResource>): Promise<void> {
    await run.resource.dispose();
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

function check(name: string, passed: boolean, details: object): BenchmarkCorrectnessCheck {
  return { name, passed, details };
}
