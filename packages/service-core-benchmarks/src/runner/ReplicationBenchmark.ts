import { execFileSync } from 'node:child_process';
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
  backlogTarget?: ReplicationBenchmarkTarget;
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
    const manifest = (this.scenario.createManifest ?? generateBaselineReplicationManifest)(this.scenario);
    const resource = await run.resource.createIteration({
      iterationId: `${this.runOptions.runId}-${runtime.kind}-${runtime.iteration}`,
      scenario: this.scenario,
      manifest
    });
    const context: IterationState = { runtime, resource, manifest, keepalives: 0 };
    if (this.scenario.phase !== 'snapshot') {
      await resource.releaseReplication(true);
    }
    if (this.scenario.phase === 'catch-up') {
      await resource.pauseReplication();
      for (const transaction of manifest.transactions) {
        context.backlogTarget = await resource.commitTransaction(transaction);
      }
    }
    await resource.beginMeasurement();
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
      startAtNs = process.hrtime.bigint().toString();
      const release = await resource.releaseReplication(false);
      target = release.target ?? manifest.target;

      startEvent = 'replication_released';
      releasedAtNs = release.releasedAtNs;
    } else if (this.scenario.phase === 'catch-up') {
      target = context.backlogTarget!;
      startAtNs = process.hrtime.bigint().toString();
      startEvent = 'replication_resumed';
      await resource.resumeReplication();
    } else {
      startAtNs = process.hrtime.bigint().toString();
      let firstTarget: ReplicationBenchmarkTarget | undefined;
      let lastTarget: ReplicationBenchmarkTarget | undefined;
      for (const transaction of manifest.transactions) {
        const committed = await resource.commitTransaction(transaction);
        firstTarget ??= committed;
        lastTarget = committed;
      }
      if (firstTarget?.committedAtNs == null || lastTarget == null) {
        throw new Error('Streaming workload did not produce a committed target');
      }
      target = lastTarget;

      startEvent = 'producer_started';
    }

    const observation = await resource.observeCheckpoint({ target, releasedAtNs });
    runtime.metrics.recordBoundary(
      `replication_${this.scenario.phase}`,
      startEvent,
      'checkpoint_visible',
      monotonicMilliseconds(startAtNs),
      monotonicMilliseconds(observation.checkpointVisibleAtNs)
    );
    const sourceRows =
      this.scenario.phase === 'snapshot'
        ? manifest.snapshotRows.length
        : this.scenario.workload.streaming_mutation_count;
    runtime.metrics.setCounter('source_rows', sourceRows);
    runtime.metrics.setCounter(
      'source_transactions',
      this.scenario.phase === 'snapshot' ? 0 : manifest.transactions.length
    );
    runtime.metrics.setCounter('source_logical_bytes', manifest.sourceLogicalBytes);
    runtime.metrics.setCounter('payload_bytes', manifest.payloadBytes);
    const seconds =
      (monotonicMilliseconds(observation.checkpointVisibleAtNs) - monotonicMilliseconds(startAtNs)) / 1000;
    runtime.metrics.setCounter('rows_per_second', sourceRows / seconds);
    runtime.metrics.setCounter('logical_mib_per_second', manifest.sourceLogicalBytes / 1024 ** 2 / seconds);
    return observation;
  }

  protected async verifyIteration(
    observation: ReplicationBenchmarkObservation,
    context: IterationState
  ): Promise<BenchmarkCorrectnessResult> {
    // Full data verification is outside both timing and resource monitoring.
    const evidence = await context.resource.collectEvidence(observation.target.markerId);
    const comparison = context.resource.comparePosition(observation.checkpoint, observation.target);
    const checks: BenchmarkCorrectnessCheck[] = [
      check('snapshot_complete', observation.snapshotDone, { actual: observation.snapshotDone }),
      check('checkpoint_position', comparison.reached, comparison),
      check('bucket_count', evidence.bucketCount === this.scenario.expected_bucket_count, {
        expected: this.scenario.expected_bucket_count,
        actual: evidence.bucketCount
      }),
      check('operation_count', evidence.operationCount === this.scenario.expected_bucket_operation_count, {
        expected: this.scenario.expected_bucket_operation_count,
        actual: evidence.operationCount
      }),
      check('put_operation_count', evidence.putCount === context.manifest.expectedPutCount, {
        expected: context.manifest.expectedPutCount,
        actual: evidence.putCount
      }),
      check('target_marker_visible', evidence.markerVisible, { marker_id: observation.target.markerId })
    ];
    const metrics = context.runtime.metrics;
    metrics.setCounter('bucket_operations', evidence.operationCount);
    metrics.setCounter('put_payload_bytes_mean', evidence.putPayloadBytes / Math.max(1, evidence.putCount));
    metrics.setCounter('put_payload_bytes_min', evidence.minPutPayloadBytes);
    metrics.setCounter('put_payload_bytes_max', evidence.maxPutPayloadBytes);
    if (evidence.s3) {
      metrics.setCounter('s3_uploads', evidence.s3.uploads);
      metrics.setCounter('s3_uploaded_bytes', evidence.s3.bytes);
      checks.push(check('s3_uploads_observed', !evidence.s3.required || evidence.s3.uploads > 0, evidence.s3));
    }
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
      node_version: process.version,
      git_revision: gitRevision(),
      producer: this.scenario.producer,
      phase: this.scenario.phase,
      storage_version: this.scenario.storage.version
    };
  }

  protected async cleanupRun(run: ReplicationBenchmarkRunContext<ReplicationBenchmarkRunResource>): Promise<void> {
    await run.resource.dispose();
  }
}

function check(name: string, passed: boolean, details: object): BenchmarkCorrectnessCheck {
  return { name, passed, details };
}

function gitRevision(): string | null {
  try {
    return execFileSync('git', ['rev-parse', 'HEAD'], { encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'] }).trim();
  } catch {
    return null;
  }
}
