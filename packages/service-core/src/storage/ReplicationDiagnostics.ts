/** Optional process-local measurements for the change-batch benchmark. No tracing allocation when disabled. */
export class ReplicationDiagnostics {
  static active: ReplicationDiagnostics | undefined;
  private readonly measurements = new Map<string, { count: number; total_ms: number; max_ms: number }>();
  constructor(readonly cpuProfile = false) {}

  record(name: string, duration: number) {
    const value = this.measurements.get(name) ?? { count: 0, total_ms: 0, max_ms: 0 };
    value.count++;
    value.total_ms += duration;
    value.max_ms = Math.max(value.max_ms, duration);
    this.measurements.set(name, value);
  }

  span(name: string) {
    const started = performance.now();
    let ended = false;
    const end = () => {
      if (ended) return;
      ended = true;
      this.record(name, performance.now() - started);
    };
    return { end, [Symbol.dispose]: end };
  }

  snapshot() {
    return Object.fromEntries(this.measurements);
  }
}
