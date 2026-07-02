import { traceWriter } from './TraceWriter.js';

export interface Span extends Disposable {
  name: string;
  /**
   * Start time in microseconds since an arbitrary epoch.
   */
  startAt: number;
  /**
   * End time in microseconds since an arbitrary epoch.
   *
   * 0 if the span hasn't ended yet.
   */
  endAt: number;

  /**
   * Time spent not in nested spans, in microseconds.
   */
  selfDuration: number;

  nestedSince: number | undefined;
  subtrackFromSelf: number;

  /**
   * Durations spent in nested spans, in microseconds.
   */
  nestedDurations: Record<string, number>;

  /**
   * Total duration of this span in milliseconds, rounded up. Only valid after the span has ended.
   */
  durationMillis: number;

  /**
   * End the span - same as [Symbol.dispose]().
   *
   * Safe to call multiple times. Any nested spans will automatically end as well.
   *
   * Returns an aggregate record of category -> "selfDuration", in microseconds.
   */
  end(): Record<string, number>;

  /**
   * Call a nested function, then end the span, returning the function's result.
   */
  with<T>(cb: () => Promise<T>): Promise<T>;
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
    traceWriter?.write({
      ph: 'M',
      cat: '__metadata',
      name: 'thread_name',
      pid: process.pid,
      tid: this.threadId,
      args: { name: `PowerSync ${traceName}` }
    });
  }

  /**
   * Recommended usage:
   *
   *   using _ = tracer.span('cat', 'details');
   *
   * The above automatically ends the span when it goes out of scope. Alternatively, call
   * .end() on the span to end it earlier.
   *
   * @param category one of the defined categories
   * @param subcat optional subcategory. Not used for calculating "self" durations in the aggregate API.
   */
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
        traceWriter?.write({
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

      get durationMillis() {
        return Math.ceil((this.endAt - this.startAt) / 1000);
      },

      [Symbol.dispose]() {
        this.end();
      },

      async with<T>(cb: () => Promise<T>): Promise<T> {
        try {
          return await cb();
        } finally {
          this.end();
        }
      }
    };
    this.stack.push(s);

    return s;
  }
}
