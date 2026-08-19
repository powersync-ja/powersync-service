import {
  BenchmarkResourceComponent,
  ResourceMonitor,
  ResourceMonitorContext,
  ResourceMonitorResult
} from '../types/BenchmarkResource.js';

export class UnavailableResourceMonitor implements ResourceMonitor {
  constructor(
    readonly component: BenchmarkResourceComponent,
    private readonly reason: string
  ) {
    if (reason.trim().length === 0) {
      throw new Error('Unavailable resource monitors require a reason');
    }
  }

  async start(_context: ResourceMonitorContext): Promise<void> {}

  async stop(_context: ResourceMonitorContext): Promise<ResourceMonitorResult> {
    return {
      component: this.component,
      status: 'unavailable',
      reason: this.reason,
      metrics: {},
      errors: []
    };
  }
}
