import { createDataManifest } from '../generators/benchmark-data.js';
import { monotonicMilliseconds } from '../replication/replication-child-protocol.js';
import { BenchmarkCorrectnessCheck, BenchmarkCorrectnessResult } from '../types/BenchmarkIteration.js';
import { BenchmarkRunOptions } from '../types/BenchmarkRunOptions.js';
import {
  ReplicationBenchmarkImplementation,
  ReplicationBenchmarkIterationContext,
  ReplicationBenchmarkObservation,
  ReplicationBenchmarkRunContext,
  ReplicationBenchmarkScenario,
  ReplicationBenchmarkTarget
} from '../types/ReplicationBenchmark.js';
import { verifyBenchmarkEvidence } from '../utils/benchmark-sync.js';
import { replicationWorkCounters } from '../utils/replication-counters.js';
import { Benchmark } from './Benchmark.js';

interface IterationState extends ReplicationBenchmarkIterationContext {
  keepalives: number;
}

export class ReplicationBenchmark extends Benchmark<
  ReplicationBenchmarkScenario,
  ReplicationBenchmarkRunContext,
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

  protected async setupRun(signal: AbortSignal): Promise<ReplicationBenchmarkRunContext> {
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
    run: ReplicationBenchmarkRunContext,
    runtime: IterationState['runtime']
  ): Promise<IterationState> {
    runtime.signal.throwIfAborted();
    const manifest = createDataManifest(
      this.scenario.data,
      this.scenario.phase === 'snapshot' ? 'snapshot' : 'streaming'
    );
    const resource = await run.resource.createIteration({
      iterationId: `${this.runOptions.runId}-${runtime.kind}-${runtime.iteration}`,
      scenario: this.scenario,
      manifest
    });
    const context: IterationState = { runtime, resource, manifest, keepalives: 0 };
    if (this.scenario.phase === 'streaming') {
      try {
        const release = await resource.releaseReplication(true);
        const initial = await resource.observeCheckpoint({ target: release.target! });
        if (
          !verifyBenchmarkEvidence(this.scenario.data, this.scenario.sync, initial.evidence, ['snapshot']).every(
            (check) => check.passed
          )
        )
          throw new Error('Initial replication snapshot verification failed');
      } catch (error) {
        await resource.dispose();
        throw error;
      }
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

    const workCounters = replicationWorkCounters(this.scenario.phase, manifest, observation.evidence);
    Object.entries(workCounters).forEach((counter) => {
      const [name, value] = counter;
      runtime.metrics.setCounter(name, value);
    });

    runtime.metrics.setCounter(
      'source_transactions',
      this.scenario.phase === 'snapshot' ? 0 : manifest.transactions.length
    );
    runtime.metrics.setCounter('source_logical_bytes', manifest.sourceLogicalBytes);
    runtime.metrics.setCounter('payload_bytes', manifest.payloadBytes);
    runtime.metrics.setCounter(
      'bucket_data_bytes',
      observation.evidence.phaseDataBytes[this.scenario.phase === 'snapshot' ? 'snapshot' : 'streaming'] ?? 0
    );
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
    const checks: BenchmarkCorrectnessCheck[] = [
      check('snapshot_complete', observation.snapshotDone, { actual: observation.snapshotDone }),
      check('checkpoint_position', comparison.reached, comparison),
      check('bucket_count', observation.bucketCount === this.scenario.expected_bucket_count, {
        expected: this.scenario.expected_bucket_count,
        actual: observation.bucketCount
      }),
      ...verifyBenchmarkEvidence(
        this.scenario.data,
        this.scenario.sync,
        observation.evidence,
        this.scenario.phase === 'snapshot' ? ['snapshot'] : ['snapshot', 'streaming']
      )
    ];
    return { passed: checks.every((candidate) => candidate.passed), checks };
  }

  protected async cleanupIteration(context: IterationState): Promise<void> {
    await context.resource.dispose();
  }

  protected async collectRunMetadata(run: ReplicationBenchmarkRunContext): Promise<object> {
    return {
      ...run.resource.environment,
      producer: this.scenario.producer,
      phase: this.scenario.phase,
      storage_version: this.scenario.storage.version
    };
  }

  protected async cleanupRun(run: ReplicationBenchmarkRunContext): Promise<void> {
    await run.resource.dispose();
  }
}

function check(name: string, passed: boolean, details: object): BenchmarkCorrectnessCheck {
  return { name, passed, details };
}
