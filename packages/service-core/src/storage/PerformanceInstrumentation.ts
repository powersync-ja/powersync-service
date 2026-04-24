export interface PerformanceInstrumentation {
  getBreakDown(): Record<string, number>;
}

export class PerformanceTimer<K extends string> {
  stats: Record<K, number>;

  constructor(keys: K[]) {
    let stats: Record<K, number> = {} as any;
    for (let k of keys) {
      stats[k] = 0;
    }
    this.stats = stats;
  }

  add(k: K, v: number) {
    this.stats[k] += v;
  }

  mark(): PerformanceInstrumentation {
    const start = { ...this.stats };

    return {
      getBreakDown: () => {
        return Object.fromEntries(
          Object.entries(this.stats).map(([k, v]) => {
            return [k, (v as number) - start[k as K]];
          })
        );
      }
    };
  }
}
