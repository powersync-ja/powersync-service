import { randomUUID } from 'node:crypto';
import { createDataManifest } from '../generators/benchmark-data.js';
import { monotonicMilliseconds } from '../replication/replication-child-protocol.js';
import { BenchmarkCorrectnessCheck, BenchmarkCorrectnessResult } from '../types/BenchmarkIteration.js';
import { BenchmarkRunOptions } from '../types/BenchmarkRunOptions.js';
import {
  CombinedBenchmarkImplementation,
  CombinedBenchmarkIterationContext,
  CombinedBenchmarkObservation,
  CombinedBenchmarkRunContext,
  CombinedBenchmarkScenario
} from '../types/CombinedBenchmark.js';
import { check } from '../utils/api-utils.js';
import { benchmarkClientStreams, benchmarkMarkerIds, verifyBenchmarkEvidence } from '../utils/benchmark-sync.js';
import {
  createNdjsonStreamReader,
  drainNdjsonResponse,
  drainNdjsonUntilCheckpoint,
  NdjsonDrainObservation,
  NdjsonStreamReader
} from '../utils/ndjson-drain.js';
import { Benchmark } from './Benchmark.js';

interface CombinedExecutionObservation {
  readonly storage: CombinedBenchmarkObservation;
  readonly client: NdjsonDrainObservation;
}

interface CombinedIterationState extends CombinedBenchmarkIterationContext {
  readonly streamReader?: NdjsonStreamReader;
  readonly initialCheckpoint?: string;
}

export class CombinedBenchmark extends Benchmark<
  CombinedBenchmarkScenario,
  CombinedBenchmarkRunContext,
  CombinedIterationState,
  CombinedExecutionObservation
> {
  constructor(
    scenario: CombinedBenchmarkScenario,
    private readonly implementation: CombinedBenchmarkImplementation,
    runOptions: BenchmarkRunOptions
  ) {
    super(scenario, runOptions);
  }

  protected async setupRun(signal: AbortSignal): Promise<CombinedBenchmarkRunContext> {
    this.validateImplementation();
    this.validateClientConfiguration();
    return { resource: await this.implementation.open(signal, this.runOptions.runId) };
  }

  protected async setupIteration(
    run: CombinedBenchmarkRunContext,
    runtime: CombinedBenchmarkIterationContext['runtime']
  ): Promise<CombinedIterationState> {
    runtime.signal.throwIfAborted();
    const manifest = createDataManifest(
      this.scenario.data,
      this.scenario.mode === 'streaming' ? 'streaming' : 'snapshot'
    );
    const resource = await run.resource.createIteration({
      iterationId: `${this.runOptions.runId}-${runtime.kind}-${runtime.iteration}`,
      scenario: this.scenario,
      manifest
    });
    if (this.scenario.mode !== 'streaming') return { runtime, resource, manifest };

    let streamReader: NdjsonStreamReader | undefined;
    try {
      await resource.releaseReplication(false);
      const initialStorage = await resource.observeCheckpoint();
      const response = await this.openSyncStream(resource.endpoint, resource.token, runtime.signal);
      streamReader = createNdjsonStreamReader(response, benchmarkMarkerIds(this.scenario.data));
      const initial = await streamReader.nextCheckpoint();
      if (
        initial.status !== 200 ||
        !verifyBenchmarkEvidence(this.scenario.data, this.scenario.sync, initial.evidence, ['snapshot']).every(
          (check) => check.passed
        ) ||
        initial.completedCheckpoint !== initialStorage.storageCheckpoint
      ) {
        throw new Error('Combined streaming benchmark initial client sync did not complete');
      }

      return { runtime, resource, manifest, streamReader, initialCheckpoint: initial.completedCheckpoint };
    } catch (error) {
      await streamReader?.close();
      await resource.dispose();
      throw error;
    }
  }

  protected async executeIteration(context: CombinedIterationState): Promise<CombinedExecutionObservation> {
    const { runtime, resource } = context;
    runtime.signal.throwIfAborted();
    if (this.scenario.mode === 'streaming') return await this.executeStreamingIteration(context);

    runtime.metrics.startBoundary('end_to_end_snapshot', 'replication_release_start');
    try {
      const release = await resource.releaseReplication(false);
      const storageObservation = await resource.observeCheckpoint();
      runtime.metrics.recordBoundary(
        'replication_snapshot',
        'replication_released',
        'checkpoint_visible',
        monotonicMilliseconds(release.childReleasedAtNs),
        monotonicMilliseconds(storageObservation.childCheckpointVisibleAtNs)
      );

      runtime.metrics.startBoundary('http_read', 'request_start');
      let clientObservation: NdjsonDrainObservation;
      try {
        const response = await this.openSyncStream(resource.endpoint, resource.token, runtime.signal);
        clientObservation = await drainNdjsonResponse(response, benchmarkMarkerIds(this.scenario.data));
      } finally {
        runtime.metrics.endBoundary('http_read', 'stream_complete');
      }

      this.recordCounters(context, storageObservation, clientObservation);
      return { storage: storageObservation, client: clientObservation };
    } finally {
      runtime.metrics.endBoundary('end_to_end_snapshot', 'client_checkpoint_complete');
    }
  }

  private async executeStreamingIteration(context: CombinedIterationState): Promise<CombinedExecutionObservation> {
    const { runtime, resource, manifest, streamReader } = context;
    if (streamReader == null) throw new Error('Combined streaming client was not initialized');
    runtime.metrics.startBoundary('end_to_end_streaming', 'source_commit_start');
    try {
      let firstCommittedAtNs: string | undefined;
      let target = manifest.target;
      for (const transaction of manifest.transactions) {
        const committed = await resource.commitTransaction(transaction.id);
        firstCommittedAtNs ??= committed.committedAtNs;
        target = committed;
      }
      if (firstCommittedAtNs == null) throw new Error('Streaming workload produced no committed target');
      runtime.metrics.startBoundary('http_read', 'source_committed');
      let storageObservation: CombinedBenchmarkObservation;
      let clientObservation: NdjsonDrainObservation;
      try {
        storageObservation = await resource.observeCheckpoint(target);
        clientObservation = await drainNdjsonUntilCheckpoint(streamReader, storageObservation.storageCheckpoint);
      } finally {
        runtime.metrics.endBoundary('http_read', 'client_checkpoint_complete');
      }
      runtime.metrics.recordBoundary(
        'replication_streaming',
        'source_committed',
        'checkpoint_visible',
        monotonicMilliseconds(firstCommittedAtNs),
        monotonicMilliseconds(storageObservation.checkpointVisibleAtNs)
      );
      this.recordCounters(context, storageObservation, clientObservation);
      return { storage: storageObservation, client: clientObservation };
    } finally {
      runtime.metrics.endBoundary('end_to_end_streaming', 'client_checkpoint_complete');
    }
  }

  protected async verifyIteration(
    observation: CombinedExecutionObservation,
    context: CombinedIterationState
  ): Promise<BenchmarkCorrectnessResult> {
    const { storage, client } = observation;
    const comparison = context.resource.comparePosition(storage.checkpoint, storage.target);
    const checks: BenchmarkCorrectnessCheck[] = [
      check('snapshot_complete', storage.snapshotDone, { actual: storage.snapshotDone }),
      check('checkpoint_position', comparison.comparable && comparison.reached, comparison),
      check('storage_bucket_count', storage.bucketCount === this.scenario.expected_bucket_count, {
        expected: this.scenario.expected_bucket_count,
        actual: storage.bucketCount
      }),
      ...verifyBenchmarkEvidence(
        this.scenario.data,
        this.scenario.sync,
        storage.evidence,
        this.scenario.mode === 'streaming' ? ['snapshot', 'streaming'] : ['snapshot']
      ).map((check) => ({ ...check, name: `storage_${check.name}` })),
      check('http_status', client.status === 200, { actual: client.status }),
      check('ndjson_content_type', client.headers['content-type']?.includes('application/x-ndjson') === true, {
        actual: client.headers['content-type']
      }),
      check('response_uncompressed', client.headers['content-encoding'] == null, {
        actual: client.headers['content-encoding'] ?? null
      }),
      check('client_bucket_count', client.bucketNames.length === this.scenario.expected_bucket_count, {
        expected: this.scenario.expected_bucket_count,
        actual: client.bucketNames.length
      }),
      check(
        'checkpoint_before_data',
        client.checkpointLineIndex != null &&
          client.firstDataLineIndex != null &&
          client.checkpointLineIndex < client.firstDataLineIndex,
        {
          checkpoint_line: client.checkpointLineIndex,
          first_data_line: client.firstDataLineIndex
        }
      ),
      check(
        'checkpoint_before_completion',
        client.checkpointLineIndex != null &&
          client.completionLineIndex != null &&
          client.checkpointLineIndex < client.completionLineIndex,
        {
          checkpoint_line: client.checkpointLineIndex,
          completion_line: client.completionLineIndex
        }
      ),
      ...verifyBenchmarkEvidence(this.scenario.data, this.scenario.sync, client.evidence, [
        this.scenario.mode === 'streaming' ? 'streaming' : 'snapshot'
      ]).map((check) => ({ ...check, name: `client_${check.name}` })),
      check('response_has_bytes', client.wireBytes > 0, { actual: client.wireBytes }),
      check(
        'checkpoint_complete_matches_protocol_checkpoint',
        client.completedCheckpoint != null && client.completedCheckpoint === client.checkpointLastOpId,
        {
          checkpoint: client.checkpointLastOpId,
          checkpoint_complete: client.completedCheckpoint
        }
      ),
      check(
        'checkpoint_complete_matches_storage_checkpoint',
        client.completedCheckpoint != null && client.completedCheckpoint === storage.storageCheckpoint,
        {
          storage_checkpoint: storage.storageCheckpoint,
          checkpoint_complete: client.completedCheckpoint
        }
      )
    ];
    if (this.scenario.mode === 'streaming') {
      checks.push(
        check('checkpoint_advanced', client.completedCheckpoint !== context.initialCheckpoint, {
          initial: context.initialCheckpoint,
          actual: client.completedCheckpoint
        }),
        check('post_initial_rows_only', client.evidence.byPhase.streaming === client.evidence.operationCount, {
          operation_count: client.evidence.operationCount
        })
      );
    }
    return { passed: checks.every((candidate) => candidate.passed), checks };
  }

  protected async cleanupIteration(context: CombinedIterationState): Promise<void> {
    await context.streamReader?.close();
    await context.resource.dispose();
  }

  protected async collectRunMetadata(run: CombinedBenchmarkRunContext): Promise<object> {
    return {
      ...run.resource.environment,
      producer: this.scenario.producer,
      storage: this.scenario.storage.implementation,
      storage_version: this.scenario.storage.version,
      mode: this.scenario.mode,
      transport: this.scenario.transport.encoding,
      clients: this.scenario.clients.count
    };
  }

  protected async cleanupRun(run: CombinedBenchmarkRunContext): Promise<void> {
    await run.resource.dispose();
  }

  private validateImplementation(): void {
    if (this.implementation.sourceId !== this.scenario.producer) {
      throw new Error(
        `Source implementation ${this.implementation.sourceId} does not match scenario ${this.scenario.producer}`
      );
    }
    if (
      this.implementation.storageId !== this.scenario.storage.implementation ||
      this.implementation.storageVersion !== this.scenario.storage.version
    ) {
      throw new Error('Combined storage implementation does not match the resolved scenario');
    }
  }

  private validateClientConfiguration(): void {
    if (this.scenario.mode !== 'initial' && this.scenario.mode !== 'streaming')
      throw new Error('Unsupported combined benchmark mode');
    if (this.scenario.transport.encoding !== 'ndjson') {
      throw new Error('Combined benchmark only supports NDJSON transport');
    }
    if (this.scenario.transport.compression !== 'none') {
      throw new Error('Combined benchmark only supports uncompressed transport');
    }
    if (this.scenario.clients.count !== 1) throw new Error('Combined benchmark only supports one client');
  }

  private recordCounters(
    context: CombinedIterationState,
    storage: CombinedBenchmarkObservation,
    client: NdjsonDrainObservation
  ): void {
    const { metrics } = context.runtime;
    const clientPutCount = client.evidence.putCount;
    const streaming = this.scenario.mode === 'streaming';
    metrics.setCounter(
      'source_rows',
      streaming ? context.manifest.streamingRowCount : context.manifest.snapshotRowCount
    );
    metrics.setCounter('source_transactions', streaming ? context.manifest.transactions.length : 0);
    metrics.setCounter('source_logical_bytes', context.manifest.sourceLogicalBytes);
    metrics.setCounter('payload_bytes', context.manifest.payloadBytes);
    metrics.setCounter('bucket_data_bytes', storage.evidence.phaseDataBytes[streaming ? 'streaming' : 'snapshot'] ?? 0);
    metrics.setCounter(
      'writer_save_calls',
      streaming ? context.manifest.streamingRowCount : context.manifest.expectedPutCount
    );
    metrics.setCounter('bucket_operations', storage.evidence.byPhase[streaming ? 'streaming' : 'snapshot'] ?? 0);
    metrics.setCounter('storage_put_operations', storage.evidence.byPhase[streaming ? 'streaming' : 'snapshot'] ?? 0);
    metrics.setCounter('parameter_operations', 0);
    metrics.setCounter('distinct_buckets', storage.bucketCount);
    metrics.setCounter('visible_checkpoints', streaming ? context.manifest.transactions.length : 1);
    metrics.setCounter('target_markers', streaming ? context.manifest.transactions.length : 1);
    metrics.setCounter('client_operations', client.evidence.operationCount);
    metrics.setCounter('client_put_operations', clientPutCount);
    metrics.setCounter('client_count', this.scenario.clients.count);
    metrics.setCounter('protocol_plaintext_bytes', client.wireBytes);
    metrics.setCounter('response_wire_bytes', client.wireBytes);
    metrics.setCounter('response_lines', client.lineCount);
  }

  private async openSyncStream(endpoint: string, token: string, signal: AbortSignal): Promise<Response> {
    return await fetch(`${endpoint}/sync/stream`, {
      method: 'POST',
      headers: {
        authorization: `Bearer ${token}`,
        accept: 'application/x-ndjson',
        'accept-encoding': 'identity',
        'content-type': 'application/json'
      },
      body: JSON.stringify({
        raw_data: true,
        client_id: randomUUID(),
        buckets: [],
        parameters: this.scenario.sync_parameters,
        streams: { include_defaults: true, subscriptions: benchmarkClientStreams(this.scenario.sync) }
      }),
      signal
    });
  }
}
