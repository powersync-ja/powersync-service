import { fixtureEnvironment } from '../env.js';
import type { BenchmarkResourceComponent } from '../types/BenchmarkResource.js';
import type { BenchmarkLayer } from '../types/BenchmarkScenario.js';
import type { ReplicationBenchmarkProducerId } from '../types/ReplicationBenchmark.js';
import type { StorageBenchmarkImplementationId } from '../types/StorageBenchmark.js';
import { DockerDatabaseResourceMonitor } from './DockerDatabaseResourceMonitor.js';

interface DatabaseScenario {
  layer: BenchmarkLayer;
  producer?: ReplicationBenchmarkProducerId;
  storage: { implementation: StorageBenchmarkImplementationId };
}

interface DatabaseMonitorTarget {
  component: Extract<BenchmarkResourceComponent, 'source_database' | 'storage_database'>;
  url: string;
}

export function databaseMonitorTargets(
  scenario: DatabaseScenario,
  environment: Readonly<Record<string, string | undefined>> = fixtureEnvironment
): DatabaseMonitorTarget[] {
  const replication = scenario.layer === 'replication' || scenario.layer === 'combined';
  const targets: DatabaseMonitorTarget[] = [];
  if (replication && scenario.producer != null) {
    const variable =
      scenario.producer === 'source:postgres' ? 'BENCHMARK_POSTGRES_SOURCE_URL' : 'BENCHMARK_MONGODB_SOURCE_URL';
    targets.push({ component: 'source_database', url: environment[variable] ?? '' });
  }
  const postgres = scenario.storage.implementation === 'storage:postgres';
  const variable = postgres ? 'BENCHMARK_POSTGRES_STORAGE_URL' : 'BENCHMARK_MONGODB_STORAGE_URL';
  targets.push({ component: 'storage_database', url: environment[variable]! });
  return targets;
}

export function createDatabaseResourceMonitors(scenario: DatabaseScenario): DockerDatabaseResourceMonitor[] {
  return databaseMonitorTargets(scenario).map(
    ({ component, url }) => new DockerDatabaseResourceMonitor(component, url)
  );
}
