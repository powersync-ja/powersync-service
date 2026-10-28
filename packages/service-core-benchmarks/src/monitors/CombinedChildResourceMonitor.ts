import { CombinedChildController } from '../combined/CombinedChildController.js';
import { CombinedChildResourceSamplePayload } from '../combined/combined-child-protocol.js';
import { ResourceMonitor, ResourceMonitorContext, ResourceMonitorResult } from '../types/BenchmarkResource.js';

export class CombinedChildResourceMonitor implements ResourceMonitor {
  readonly component = 'service' as const;
  private baseline?: CombinedChildResourceSamplePayload;

  constructor(private readonly getController: () => CombinedChildController) {}

  async start(_context: ResourceMonitorContext): Promise<void> {
    if (this.baseline != null) throw new Error('Combined child resource monitor is already running');
    this.baseline = await this.getController().request('monitor_start', {});
  }

  async stop(_context: ResourceMonitorContext): Promise<ResourceMonitorResult> {
    const baseline = this.baseline;
    if (baseline == null) throw new Error('Combined child resource monitor is not running');
    const final = await this.getController().request('monitor_stop', {});
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
