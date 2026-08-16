import { BenchmarkError } from './BenchmarkError.js';
import { BenchmarkIterationDescriptor } from './BenchmarkIteration.js';
import { BenchmarkScenario } from './BenchmarkScenario.js';

export type BenchmarkResourceComponent =
  | 'service'
  | 'source_database'
  | 'storage_database'
  | 'drain_client'
  | 'load_generator';

export interface ResourceMonitorResult {
  component: BenchmarkResourceComponent;
  status: 'available' | 'unavailable' | 'failed';
  reason: string | null;
  metrics: object;
  errors: BenchmarkError[];
}

export interface ResourceMonitorContext {
  readonly scenario: BenchmarkScenario;
  readonly iteration: BenchmarkIterationDescriptor;
  readonly signal: AbortSignal;
}

export interface ResourceMonitor {
  readonly component: BenchmarkResourceComponent;
  start(context: ResourceMonitorContext): Promise<void>;
  stop(context: ResourceMonitorContext): Promise<ResourceMonitorResult>;
}

export interface IntervalScheduler {
  set(callback: () => void, intervalMs: number): object;
  clear(handle: object): void;
}

export interface NodeProcessSample {
  readonly cpu_user_microseconds: number;
  readonly cpu_system_microseconds: number;
  readonly rss_bytes: number;
}

export interface NodeProcessSampler {
  sample(): NodeProcessSample;
}

export interface NodeProcessResourceMonitorOptions {
  readonly sampleIntervalMs?: number;
  readonly sampler?: NodeProcessSampler;
  readonly scheduler?: IntervalScheduler;
}
