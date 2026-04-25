import * as fs from 'node:fs';
export interface PerformanceInstrumentation {
  getBreakDown(digits?: number): Record<string, number>;
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
      getBreakDown: (fractionDigits?: number) => {
        return Object.fromEntries(
          Object.entries(this.stats).map(([k, v]) => {
            let duration = (v as number) - start[k as K];
            if (fractionDigits != null) {
              duration = parseFloat(duration.toFixed(fractionDigits));
            }
            return [k, duration];
          })
        );
      }
    };
  }
}

const traceEvents: any[] = [
  {
    ph: 'M',
    cat: '__metadata',
    name: 'process_name',
    pid: process.pid,
    tid: 1000,
    args: { name: 'powersync' }
  },
  {
    ph: 'M',
    cat: '__metadata',
    name: 'thread_name',
    pid: process.pid,
    tid: 1000,
    args: { name: 'PowerSync instrumentation' }
  }
];

export interface Span extends Disposable {
  name: string;
  startAt: number;
  endAt: number;
  selfDuration: number;
  nestedSince: number | undefined;
  subtrackFromSelf: number;
  nestedDurations: Record<string, number>;

  end(): Record<string, number>;
}

function now() {
  return Number(process.hrtime.bigint() / 1000n);
}
export class PerformanceTrace<K extends string> {
  stack: (Span | null)[] = [];

  constructor(keys: K[]) {}

  mark(): PerformanceInstrumentation {
    return {
      getBreakDown(digits) {
        return {};
      }
    };
  }

  span(name: K): Span {
    const stack = this.stack;
    const index = this.stack.length;
    const parent = this.stack[this.stack.length - 1];
    const startAt = now();
    if (parent != null) {
      parent.nestedSince ??= startAt;
    }
    const s: Span = {
      name,
      startAt: now(),
      selfDuration: 0,
      endAt: 0,
      nestedSince: undefined,
      subtrackFromSelf: 0,
      nestedDurations: {},
      end() {
        if (this.endAt != 0) {
          return this.nestedDurations;
        }
        const endAt = now();
        this.endAt = endAt;
        const endTime = this.nestedSince ?? endAt;
        this.selfDuration = endTime - startAt - this.subtrackFromSelf;
        traceEvents.push({
          name,
          cat: 'powersync',
          ph: 'X',
          ts: this.startAt,
          dur: endAt - startAt,
          pid: process.pid,
          tid: 1000,
          args: {
            selfTime: this.selfDuration
          }
        });
        stack[index] = null;
        while (stack.length > 0 && stack[stack.length - 1] == null) {
          stack.pop();
        }
        const newTop = stack[stack.length - 1];
        if (newTop != null && stack.length <= index) {
          newTop.subtrackFromSelf += endAt - newTop.nestedSince!;
          for (let key in this.nestedDurations) {
            newTop.nestedDurations[key] = (newTop.nestedDurations[key] ?? 0) + this.nestedDurations[key];
          }
          newTop.nestedDurations[this.name] = (newTop.nestedDurations[this.name] ?? 0) + this.selfDuration;
          newTop.nestedSince = undefined;
        }
        return this.nestedDurations;
      },
      [Symbol.dispose]() {
        this.end();
      }
    };
    this.stack.push(s);

    return s;
  }
}

process.on('exit', () => {
  fs.writeFileSync('trace.json', JSON.stringify({ traceEvents: traceEvents }));
});
