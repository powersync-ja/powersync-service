import { randomUUID } from 'node:crypto';
import { generateBaselineSnapshotManifest } from '../generators/generate-baseline-snapshot-manifest.js';
import { monotonicMilliseconds } from '../replication/replication-child-protocol.js';
import { BenchmarkCorrectnessCheck, BenchmarkCorrectnessResult } from '../types/BenchmarkIteration.js';
import { BenchmarkRunOptions } from '../types/BenchmarkRunOptions.js';
import {
  CombinedBenchmarkImplementation,
  CombinedBenchmarkIterationContext,
  CombinedBenchmarkIterationResource,
  CombinedBenchmarkObservation,
  CombinedBenchmarkRunContext,
  CombinedBenchmarkRunResource,
  CombinedBenchmarkScenario
} from '../types/CombinedBenchmark.js';
import { check } from '../utils/api-utils.js';
import { drainNdjsonResponse, NdjsonDataOperation, NdjsonDrainObservation } from '../utils/ndjson-drain.js';
import { Benchmark } from './Benchmark.js';

interface CombinedExecutionObservation {
  readonly storage: CombinedBenchmarkObservation;
  readonly client: NdjsonDrainObservation;
}

type RunContext = CombinedBenchmarkRunContext<CombinedBenchmarkRunResource>;
type IterationContext = CombinedBenchmarkIterationContext<CombinedBenchmarkIterationResource>;

export class CombinedBenchmark extends Benchmark<
  CombinedBenchmarkScenario,
  RunContext,
  IterationContext,
  CombinedExecutionObservation
> {
  constructor(
    scenario: CombinedBenchmarkScenario,
    private readonly implementation: CombinedBenchmarkImplementation,
    runOptions: BenchmarkRunOptions
  ) {
    super(scenario, runOptions);
  }

  protected async setupRun(signal: AbortSignal): Promise<RunContext> {
    this.validateImplementation();
    this.validateClientConfiguration();
    return { resource: await this.implementation.open(signal, this.runOptions.runId) };
  }

  protected async setupIteration(run: RunContext, runtime: IterationContext['runtime']): Promise<IterationContext> {
    runtime.signal.throwIfAborted();
    const manifest = generateBaselineSnapshotManifest(this.scenario.workload);
    const resource = await run.resource.createIteration({
      iterationId: `${this.runOptions.runId}-${runtime.kind}-${runtime.iteration}`,
      scenario: this.scenario,
      manifest
    });
    return { runtime, resource, manifest };
  }

  protected async executeIteration(context: IterationContext): Promise<CombinedExecutionObservation> {
    const { runtime, resource, manifest } = context;
    runtime.signal.throwIfAborted();
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
        const response = await fetch(`${resource.endpoint}/sync/stream`, {
          method: 'POST',
          headers: {
            authorization: `Bearer ${resource.token}`,
            accept: 'application/x-ndjson',
            'accept-encoding': 'identity',
            'content-type': 'application/json'
          },
          body: JSON.stringify({
            raw_data: true,
            client_id: randomUUID(),
            buckets: [],
            parameters: this.scenario.sync_parameters
          }),
          signal: runtime.signal
        });
        clientObservation = await drainNdjsonResponse(response);
      } finally {
        runtime.metrics.endBoundary('http_read', 'stream_complete');
      }

      this.recordCounters(context, storageObservation, clientObservation);
      return { storage: storageObservation, client: clientObservation };
    } finally {
      runtime.metrics.endBoundary('end_to_end_snapshot', 'client_checkpoint_complete');
    }
  }

  protected async verifyIteration(
    observation: CombinedExecutionObservation,
    context: IterationContext
  ): Promise<BenchmarkCorrectnessResult> {
    const { storage, client } = observation;
    const comparison = context.resource.comparePosition(storage.checkpoint, storage.target);
    const storagePuts = storage.operations.filter((operation) => operation.op === 'PUT');
    const clientPuts = client.operations.filter((operation) => operation.op === 'PUT');
    const storageMarker = storage.operations.find((operation) => operation.object_id === storage.target.markerId);
    const clientMarker = client.operations.find((operation) => operation.object_id === storage.target.markerId);
    const expectedOperations = this.scenario.expected_bucket_operation_count;
    const checks: BenchmarkCorrectnessCheck[] = [
      check('snapshot_complete', storage.snapshotDone, { actual: storage.snapshotDone }),
      check('checkpoint_position', comparison.comparable && comparison.reached, comparison),
      check('storage_bucket_count', storage.bucketCount === this.scenario.expected_bucket_count, {
        expected: this.scenario.expected_bucket_count,
        actual: storage.bucketCount
      }),
      check('storage_operation_count', storage.operations.length === expectedOperations, {
        expected: expectedOperations,
        actual: storage.operations.length
      }),
      check('storage_put_operation_count', storagePuts.length === expectedOperations, {
        expected: expectedOperations,
        actual: storagePuts.length
      }),
      check('storage_target_marker_visible', markerContainsTarget(storageMarker), {
        marker_id: storage.target.markerId,
        actual: storageMarker ?? null
      }),
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
      check('client_operation_count', client.operations.length === expectedOperations, {
        expected: expectedOperations,
        actual: client.operations.length
      }),
      check('client_put_operation_count', clientPuts.length === expectedOperations, {
        expected: expectedOperations,
        actual: clientPuts.length
      }),
      check('client_target_marker_visible', markerContainsTarget(clientMarker), {
        marker_id: storage.target.markerId,
        actual: clientMarker ?? null
      }),
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
    return { passed: checks.every((candidate) => candidate.passed), checks };
  }

  protected async cleanupIteration(context: IterationContext): Promise<void> {
    await context.resource.dispose();
  }

  protected async collectRunMetadata(run: RunContext): Promise<object> {
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

  protected async cleanupRun(run: RunContext): Promise<void> {
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
    if (this.scenario.mode !== 'initial') throw new Error('Combined benchmark only supports initial mode');
    if (this.scenario.transport.encoding !== 'ndjson') {
      throw new Error('Combined benchmark only supports NDJSON transport');
    }
    if (this.scenario.transport.compression !== 'none') {
      throw new Error('Combined benchmark only supports uncompressed transport');
    }
    if (this.scenario.clients.count !== 1) throw new Error('Combined benchmark only supports one client');
  }

  private recordCounters(
    context: IterationContext,
    storage: CombinedBenchmarkObservation,
    client: NdjsonDrainObservation
  ): void {
    const { metrics } = context.runtime;
    const storagePutCount = storage.operations.filter((operation) => operation.op === 'PUT').length;
    const clientPutCount = client.operations.filter((operation) => operation.op === 'PUT').length;
    metrics.setCounter('source_rows', context.manifest.snapshotRows.length);
    metrics.setCounter('source_transactions', 0);
    metrics.setCounter('source_logical_bytes', context.manifest.sourceLogicalBytes);
    metrics.setCounter('payload_bytes', context.manifest.payloadBytes);
    metrics.setCounter('writer_save_calls', context.manifest.expectedPutCount);
    metrics.setCounter('bucket_operations', storage.operations.length);
    metrics.setCounter('storage_put_operations', storagePutCount);
    metrics.setCounter('parameter_operations', 0);
    metrics.setCounter('distinct_buckets', storage.bucketCount);
    metrics.setCounter('visible_checkpoints', 1);
    metrics.setCounter('target_markers', 1);
    metrics.setCounter('client_operations', client.operations.length);
    metrics.setCounter('client_put_operations', clientPutCount);
    metrics.setCounter('client_count', this.scenario.clients.count);
    metrics.setCounter('protocol_plaintext_bytes', client.wireBytes);
    metrics.setCounter('response_wire_bytes', client.wireBytes);
    metrics.setCounter('response_lines', client.lines.length);
  }
}

function markerContainsTarget(
  operation: { readonly op: string; readonly data?: unknown } | NdjsonDataOperation | undefined
): boolean {
  if (operation?.op !== 'PUT' || typeof operation.data !== 'string') return false;
  try {
    const data: unknown = JSON.parse(operation.data);
    return isRecord(data) && data.is_target === 1;
  } catch {
    return false;
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value != null && typeof value === 'object' && !Array.isArray(value);
}
