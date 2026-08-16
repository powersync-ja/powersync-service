import { BenchmarkError } from '../types/BenchmarkError.js';
import {
  IntervalScheduler,
  NodeProcessResourceMonitorOptions,
  NodeProcessSample,
  NodeProcessSampler,
  ResourceMonitor,
  ResourceMonitorContext,
  ResourceMonitorResult
} from '../types/BenchmarkResource.js';
import { toBenchmarkError } from '../utils/benchmark-errors.js';
import { systemIntervalScheduler } from '../utils/interval-scheduler.js';

export class NodeProcessResourceMonitor implements ResourceMonitor {
  readonly component = 'load_generator';

  private readonly sampleIntervalMs: number;
  private readonly sampler: NodeProcessSampler;
  private readonly scheduler: IntervalScheduler;
  private samples: NodeProcessSample[] = [];
  private sampleErrors: BenchmarkError[] = [];
  private interval: object | null = null;

  constructor(options: NodeProcessResourceMonitorOptions = {}) {
    this.sampleIntervalMs = options.sampleIntervalMs ?? 100;
    this.sampler = options.sampler ?? systemNodeProcessSampler;
    this.scheduler = options.scheduler ?? systemIntervalScheduler;

    if (!Number.isFinite(this.sampleIntervalMs) || this.sampleIntervalMs <= 0) {
      throw new Error('sampleIntervalMs must be a positive finite number');
    }
  }

  async start(_context: ResourceMonitorContext): Promise<void> {
    if (this.interval != null) {
      throw new Error('Node process resource monitor is already running');
    }

    this.samples = [this.sampler.sample()];
    this.sampleErrors = [];
    this.interval = this.scheduler.set(() => this.captureIntervalSample(), this.sampleIntervalMs);
  }

  async stop(_context: ResourceMonitorContext): Promise<ResourceMonitorResult> {
    if (this.interval == null) {
      throw new Error('Node process resource monitor is not running');
    }

    this.scheduler.clear(this.interval);
    this.interval = null;
    try {
      this.samples.push(this.sampler.sample());
    } catch (error) {
      this.sampleErrors.push(toBenchmarkError('stop_monitor', error));
    }

    const metrics = this.calculateMetrics();
    return {
      component: this.component,
      status: this.sampleErrors.length === 0 ? 'available' : 'failed',
      reason: null,
      metrics,
      errors: [...this.sampleErrors]
    };
  }

  private captureIntervalSample(): void {
    try {
      this.samples.push(this.sampler.sample());
    } catch (error) {
      this.sampleErrors.push(toBenchmarkError('sample_monitor', error));
    }
  }

  private calculateMetrics(): object {
    const baseline = this.samples[0];
    const final = this.samples.at(-1)!;
    const rssTotal = this.samples.reduce((total, sample) => total + sample.rss_bytes, 0);
    const cpuUserMs = (final.cpu_user_microseconds - baseline.cpu_user_microseconds) / 1_000;
    const cpuSystemMs = (final.cpu_system_microseconds - baseline.cpu_system_microseconds) / 1_000;

    return {
      pid: process.pid,
      sample_interval_ms: this.sampleIntervalMs,
      sample_count: this.samples.length,
      cpu_user_ms: cpuUserMs,
      cpu_system_ms: cpuSystemMs,
      cpu_total_ms: cpuUserMs + cpuSystemMs,
      rss_baseline_bytes: baseline.rss_bytes,
      rss_average_bytes: rssTotal / this.samples.length,
      rss_peak_bytes: Math.max(...this.samples.map((sample) => sample.rss_bytes)),
      rss_delta_bytes: final.rss_bytes - baseline.rss_bytes
    };
  }
}

const systemNodeProcessSampler: NodeProcessSampler = {
  sample() {
    const cpu = process.cpuUsage();
    return {
      cpu_user_microseconds: cpu.user,
      cpu_system_microseconds: cpu.system,
      rss_bytes: process.memoryUsage.rss()
    };
  }
};
