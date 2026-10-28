import { BenchmarkTimingBoundary } from './BenchmarkIteration.js';

export interface BenchmarkClock {
  now(): number;
}

export interface BenchmarkMetricSnapshot {
  readonly boundaries: Record<string, BenchmarkTimingBoundary>;
  readonly counters: Record<string, number>;
}

export interface BenchmarkActiveBoundary {
  readonly name: string;
  readonly startEvent: string;
  readonly startedAtMs: number;
}
