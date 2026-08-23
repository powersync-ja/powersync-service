import { BenchmarkScenario } from './BenchmarkScenario.js';
import { StorageBenchmarkImplementationId, StorageBenchmarkWorkload } from './StorageBenchmark.js';

export interface ApiClientConfiguration {
  readonly mode: 'initial';
  readonly transport: {
    readonly encoding: 'ndjson';
    readonly compression: 'none';
  };
  readonly clients: {
    readonly count: 1;
  };
}

export interface ApiBenchmarkScenario extends BenchmarkScenario<StorageBenchmarkWorkload>, ApiClientConfiguration {
  readonly layer: 'api';
  readonly storage: {
    readonly implementation: StorageBenchmarkImplementationId;
    readonly version: number;
  };
}
