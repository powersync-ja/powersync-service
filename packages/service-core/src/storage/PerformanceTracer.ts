import { Mutex } from 'async-mutex';
import * as fs from 'node:fs/promises';

class TraceWriter {
  handle: fs.FileHandle | null = null;
  length = 0;
  queue: any[] = [];
  private mutex = new Mutex();

  constructor(public readonly path: string) {
    this.open().catch((e) => {
      console.error(`Failed to open trace file at ${path}`, e);
    });
  }

  async open() {
    await this.mutex.runExclusive(async () => {
      this.handle = await fs.open(this.path, 'w+');
      this.handle.truncate(0);
      await this.handle.write('[]');
      this.length = 2;
    });
  }

  write(...traceEvents: any[]) {
    this.writeAsync(...traceEvents).catch((e) => {
      console.error(`Failed to write trace file`, e);
    });
  }

  async writeAsync(...traceEvents: any[]) {
    this.queue.push(...traceEvents);
    await this.mutex.runExclusive(async () => {
      if (this.queue.length > 0) {
        const buffer = Buffer.from(JSON.stringify(this.queue));
        await this.handle?.write(buffer, 1, buffer.length - 1, this.length - 1);
        this.queue = [];
        this.length += buffer.length - 2;
      }
    });
  }
}

const traceFile = process.env.POWERSYNC_TRACE_FILE;
const writer = traceFile ? new TraceWriter(traceFile) : null;

if (writer) {
  writer.write({
    ph: 'M',
    cat: '__metadata',
    name: 'process_name',
    pid: process.pid,
    tid: 1000,
    args: { name: 'powersync' }
  });
}

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

export class PerformanceTracer<K extends string> {
  stack: Span[] = [];
  threadId: number;

  constructor(traceName: string) {
    this.threadId = nextThreadId;
    nextThreadId += 1;
    writer?.write({
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
        writer?.write({
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
