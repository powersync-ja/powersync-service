import * as fs from 'node:fs';

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
  stack: Span[] = [];

  constructor(categories: K[]) {}

  span(category: K, subcat?: string): Span {
    const stack = this.stack;
    const index = this.stack.length;
    const parent = this.stack[this.stack.length - 1];
    const startAt = now();
    if (parent != null) {
      parent.nestedSince ??= startAt;
    }
    let name: string = category;
    if (subcat) {
      name += ':' + subcat;
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
        while (stack.length - 1 > index) {
          stack[stack.length - 1].end();
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
        stack.pop();
        if (parent != null) {
          parent.subtrackFromSelf += endAt - parent.nestedSince!;
          for (let key in this.nestedDurations) {
            parent.nestedDurations[key] = (parent.nestedDurations[key] ?? 0) + this.nestedDurations[key];
          }
          parent.nestedDurations[category] = (parent.nestedDurations[category] ?? 0) + this.selfDuration;
          parent.nestedSince = undefined;
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
