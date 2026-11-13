import { BenchmarkScenario } from './BenchmarkScenario.js';
import { StorageBenchmarkImplementation, StorageBenchmarkImplementationId } from './StorageBenchmark.js';

export interface ApiClientConfiguration {
  readonly mode: 'initial' | 'streaming';
  readonly transport: {
    readonly encoding: 'ndjson';
    readonly compression: 'none';
  };
  readonly clients: {
    readonly count: 1;
  };
}

export interface ApiBenchmarkScenario extends BenchmarkScenario, ApiClientConfiguration {
  readonly layer: 'api';
  readonly storage: {
    readonly implementation: StorageBenchmarkImplementationId;
    readonly version: number;
  };
}

export interface ApiBenchmarkCase {
  readonly scenario: ApiBenchmarkScenario;
  readonly implementation: StorageBenchmarkImplementation;
}
