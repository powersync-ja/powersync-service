import type {
  BenchmarkResourceComponent,
  IntervalScheduler,
  ResourceMonitor,
  ResourceMonitorContext,
  ResourceMonitorResult
} from '../types/BenchmarkResource.js';
import { toBenchmarkError } from '../utils/benchmark-errors.js';
import { defaultDockerSampler, DockerContainer, DockerDatabaseSampler, DockerResourceSample } from '../utils/docker.js';
import { systemIntervalScheduler } from '../utils/interval-scheduler.js';

export function findDatabaseContainer(containers: readonly DockerContainer[], url: string): string | null {
  let parsed: URL;
  try {
    parsed = new URL(url);
  } catch {
    return null;
  }
  if (!['localhost', '127.0.0.1', '[::1]'].includes(parsed.hostname)) return null;
  const defaultPort = parsed.protocol.startsWith('mongodb') ? 27017 : 5432;
  const port = Number(parsed.port || defaultPort);
  const matches = containers.filter((container) =>
    container.Ports.some((published) => published.Type === 'tcp' && published.PublicPort === port)
  );
  return matches.length === 1 ? matches[0].Id : null;
}

interface Options {
  sampler?: DockerDatabaseSampler;
  scheduler?: IntervalScheduler;
  sampleIntervalMs?: number;
}

export class DockerDatabaseResourceMonitor implements ResourceMonitor {
  private readonly sampler: DockerDatabaseSampler;
  private readonly scheduler: IntervalScheduler;
  private readonly sampleIntervalMs: number;
  private samples: DockerResourceSample[] = [];
  private interval: object | null = null;
  private pending: Promise<void> | null = null;
  private sampleErrors: ReturnType<typeof toBenchmarkError>[] = [];
  private unavailableReason: string | null = null;
  private running = false;
  private containerId: string | null = null;

  constructor(
    readonly component: Extract<BenchmarkResourceComponent, 'source_database' | 'storage_database'>,
    private readonly databaseUrl: string,
    options: Options = {}
  ) {
    this.sampler = options.sampler ?? defaultDockerSampler;
    this.scheduler = options.scheduler ?? systemIntervalScheduler;
    this.sampleIntervalMs = options.sampleIntervalMs ?? 250;
    if (!Number.isFinite(this.sampleIntervalMs) || this.sampleIntervalMs <= 0) {
      throw new Error('sampleIntervalMs must be a positive finite number');
    }
  }

  async start(_context: ResourceMonitorContext): Promise<void> {
    if (this.running) throw new Error('Docker database resource monitor is already running');
    this.running = true;
    this.samples = [];
    this.sampleErrors = [];
    this.unavailableReason = null;
    this.containerId = null;
    try {
      this.containerId = findDatabaseContainer(await this.sampler.listContainers(), this.databaseUrl);
      if (this.containerId == null) {
        this.unavailableReason = 'No unique local Docker container matches the database URL';
        return;
      }
      this.samples.push(await this.sampler.sample(this.containerId));
      this.interval = this.scheduler.set(() => this.captureSample(), this.sampleIntervalMs);
    } catch (error) {
      this.unavailableReason = `Docker database resource monitoring unavailable: ${error instanceof Error ? error.message : String(error)}`;
      this.containerId = null;
    }
  }

  async stop(_context: ResourceMonitorContext): Promise<ResourceMonitorResult> {
    if (!this.running) throw new Error('Docker database resource monitor is not running');
    this.running = false;
    if (this.interval != null) this.scheduler.clear(this.interval);
    this.interval = null;
    if (this.pending != null) await this.pending;
    if (this.unavailableReason != null || this.containerId == null) {
      return {
        component: this.component,
        status: 'unavailable',
        reason: this.unavailableReason,
        metrics: {},
        errors: []
      };
    }
    try {
      this.samples.push(await this.sampler.sample(this.containerId));
    } catch (error) {
      this.sampleErrors.push(toBenchmarkError('stop_monitor', error));
    }
    const baseline = this.samples[0];
    const final = this.samples.at(-1)!;
    const memory = this.samples.map((sample) => sample.memoryBytes);
    const cpuTotalMs = (final.cpuTotalNs - baseline.cpuTotalNs) / 1_000_000;
    if (!Number.isFinite(cpuTotalMs) || cpuTotalMs < 0) {
      this.sampleErrors.push(toBenchmarkError('stop_monitor', new Error('Invalid Docker CPU counter delta')));
    }
    return {
      component: this.component,
      status: this.sampleErrors.length ? 'failed' : 'available',
      reason: null,
      metrics: {
        sample_interval_ms: this.sampleIntervalMs,
        sample_count: this.samples.length,
        cpu_total_ms: cpuTotalMs,
        memory_baseline_bytes: memory[0],
        memory_average_bytes: memory.reduce((sum, value) => sum + value, 0) / memory.length,
        memory_peak_bytes: Math.max(...memory),
        memory_delta_bytes: memory.at(-1)! - memory[0]
      },
      errors: [...this.sampleErrors]
    };
  }

  private captureSample(): void {
    if (this.pending != null || this.containerId == null) return;
    this.pending = this.sampler
      .sample(this.containerId)
      .then((sample) => {
        this.samples.push(sample);
      })
      .catch((error) => {
        this.sampleErrors.push(toBenchmarkError('sample_monitor', error));
      })
      .finally(() => {
        this.pending = null;
      });
  }
}
