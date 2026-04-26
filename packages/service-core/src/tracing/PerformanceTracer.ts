import { emitTraceEvents } from './TraceWriter.js';

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

let nextThreadId = 1;

/**
 * Lightweight tracing helper, with two main goals:
 * 1. Generate aggregate timing info with low overhead.
 * 2. Optional support for generating trace files during development.
 *
 * This is only intended for a single "thread" - concurrent operations on the same instance have undefined behavior.
 * To trace concurrent operations, use separate instances of PerformanceTracer.
 *
 * Spans cannot be overlapping: If a parent span is ended, all nested spans are automatically ended.
 */
export class PerformanceTracer<K extends string> {
  stack: Span[] = [];
  threadId: number;

  constructor(traceName: string) {
    this.threadId = nextThreadId;
    nextThreadId += 1;
    emitTraceEvents({
      ph: 'M',
      cat: '__metadata',
      name: 'thread_name',
      pid: process.pid,
      tid: this.threadId,
      args: { name: `PowerSync ${traceName}` }
    });
  }

  span(category: K, subcat?: string): Span {
    const stack = this.stack;
    const index = this.stack.length;
    const parent = this.stack[this.stack.length - 1];
    const threadId = this.threadId;
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
        emitTraceEvents({
          name,
          cat: 'powersync',
          ph: 'X',
          ts: this.startAt,
          dur: endAt - startAt,
          pid: process.pid,
          tid: threadId
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
