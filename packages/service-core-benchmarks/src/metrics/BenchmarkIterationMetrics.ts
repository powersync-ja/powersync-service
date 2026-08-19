import { BenchmarkTimingBoundary } from '../types/BenchmarkIteration.js';
import { BenchmarkActiveBoundary, BenchmarkClock, BenchmarkMetricSnapshot } from '../types/BenchmarkMetrics.js';

export class BenchmarkIterationMetrics {
  private readonly activeBoundaries: BenchmarkActiveBoundary[] = [];
  private readonly completedBoundaries = new Map<string, BenchmarkTimingBoundary>();
  private readonly counters = new Map<string, number>();

  constructor(private readonly clock: BenchmarkClock) {}

  startBoundary(name: string, startEvent: string): void {
    if (this.completedBoundaries.has(name) || this.activeBoundaries.some((boundary) => boundary.name === name)) {
      throw new Error(`Boundary ${name} has already started`);
    }

    this.activeBoundaries.push({ name, startEvent, startedAtMs: this.readClock() });
  }

  endBoundary(name: string, endEvent: string): void {
    const active = this.activeBoundaries.at(-1);
    if (active == null) {
      throw new Error(`Boundary ${name} has not started`);
    }
    if (active.name !== name) {
      throw new Error(`Boundary ${active.name} must end before boundary ${name}`);
    }

    const endedAtMs = this.readClock();
    if (endedAtMs < active.startedAtMs) {
      throw new Error(`Boundary ${name} ended before it started`);
    }

    this.activeBoundaries.pop();
    this.completedBoundaries.set(name, {
      start_event: active.startEvent,
      end_event: endEvent,
      started_at_ms: active.startedAtMs,
      ended_at_ms: endedAtMs,
      duration_ms: endedAtMs - active.startedAtMs
    });
  }

  recordBoundary(name: string, startEvent: string, endEvent: string, startedAtMs: number, endedAtMs: number): void {
    if (this.completedBoundaries.has(name) || this.activeBoundaries.some((boundary) => boundary.name === name)) {
      throw new Error(`Boundary ${name} has already started`);
    }
    if (!Number.isFinite(startedAtMs) || !Number.isFinite(endedAtMs)) {
      throw new Error('Benchmark boundary timestamps must be finite');
    }
    if (endedAtMs < startedAtMs) {
      throw new Error(`Boundary ${name} ended before it started`);
    }

    this.completedBoundaries.set(name, {
      start_event: startEvent,
      end_event: endEvent,
      started_at_ms: startedAtMs,
      ended_at_ms: endedAtMs,
      duration_ms: endedAtMs - startedAtMs
    });
  }

  setCounter(name: string, value: number): void {
    validateCounter(value);
    this.counters.set(name, value);
  }

  incrementCounter(name: string, delta: number = 1): void {
    validateCounter(delta);
    const value = (this.counters.get(name) ?? 0) + delta;
    validateCounter(value);
    this.counters.set(name, value);
  }

  finalize(): BenchmarkMetricSnapshot {
    if (this.activeBoundaries.length > 0) {
      throw new Error(
        `Cannot finalize metrics with unfinished boundaries: ${this.activeBoundaries
          .map((boundary) => boundary.name)
          .join(', ')}`
      );
    }

    return {
      boundaries: Object.fromEntries(this.completedBoundaries),
      counters: Object.fromEntries(this.counters)
    };
  }

  private readClock(): number {
    const now = this.clock.now();
    if (!Number.isFinite(now)) {
      throw new Error('Benchmark clock must return a finite value');
    }
    return now;
  }
}

function validateCounter(value: number): void {
  if (!Number.isFinite(value) || value < 0) {
    throw new Error('Counter values must be finite non-negative numbers');
  }
}
