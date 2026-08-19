import {
  ReplicationChildCommandPayloads,
  ReplicationChildResourceSamplePayload,
  ReplicationChildResponsePayloads
} from '../replication/replication-child-protocol.js';
import { ResourceMonitor, ResourceMonitorContext, ResourceMonitorResult } from '../types/BenchmarkResource.js';

export interface ReplicationChildRequestClient {
  request<Kind extends 'monitor_start' | 'monitor_stop'>(
    kind: Kind,
    payload: ReplicationChildCommandPayloads[Kind],
    iterationId?: string
  ): Promise<ReplicationChildResponsePayloads[Kind]>;
}

export class ReplicationChildResourceMonitor implements ResourceMonitor {
  readonly component = 'service' as const;
  private baseline?: ReplicationChildResourceSamplePayload;

  constructor(private readonly getClient: () => ReplicationChildRequestClient) {}

  async start(_context: ResourceMonitorContext): Promise<void> {
    if (this.baseline != null) throw new Error('Replication child resource monitor is already running');
    this.baseline = await this.getClient().request('monitor_start', {});
  }

  async stop(_context: ResourceMonitorContext): Promise<ResourceMonitorResult> {
    const baseline = this.baseline;
    if (baseline == null) throw new Error('Replication child resource monitor is not running');
    const final = await this.getClient().request('monitor_stop', {});
    this.baseline = undefined;
    const cpuUserMs = (final.cpu.user - baseline.cpu.user) / 1_000;
    const cpuSystemMs = (final.cpu.system - baseline.cpu.system) / 1_000;
    return {
      component: this.component,
      status: 'available',
      reason: null,
      metrics: {
        pid: final.pid,
        sample_count: 2,
        cpu_user_ms: cpuUserMs,
        cpu_system_ms: cpuSystemMs,
        cpu_total_ms: cpuUserMs + cpuSystemMs,
        rss_baseline_bytes: baseline.memory.rss,
        rss_peak_bytes: Math.max(baseline.memory.rss, final.memory.rss),
        rss_delta_bytes: final.memory.rss - baseline.memory.rss
      },
      errors: []
    };
  }
}
