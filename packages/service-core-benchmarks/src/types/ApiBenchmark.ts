import { BenchmarkScenario } from './BenchmarkScenario.js';
import { StorageBenchmarkImplementationId, StorageBenchmarkWorkload } from './StorageBenchmark.js';

export interface ApiBenchmarkScenario extends BenchmarkScenario<StorageBenchmarkWorkload> {
  readonly layer: 'api';
  readonly storage: {
    readonly implementation: StorageBenchmarkImplementationId;
    readonly version: number;
  };
  readonly mode: 'initial';
  readonly transport: {
    readonly encoding: 'ndjson';
    readonly compression: 'none';
  };
  readonly clients: {
    readonly count: 1;
  };
}
