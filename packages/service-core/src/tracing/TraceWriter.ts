import { Mutex } from 'async-mutex';
import * as fs from 'node:fs/promises';

/**
 * Write traces in the Chrome JSON Trace Format.
 *
 * View at https://ui.perfetto.dev/
 */
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
        // Write queued events.
        // After each write, we end the file as a valid JSON array.
        // On the next write, we overwrite the last character to extend the array.
        const buffer = Buffer.from(JSON.stringify(this.queue));
        await this.handle?.write(buffer, 1, buffer.length - 1, this.length - 1);
        this.queue = [];
        this.length += buffer.length - 2;
      }
    });
  }
}

const traceFile = process.env.POWERSYNC_TRACE_FILE;
/**
 * traceWriter, only present if POWERSYNC_TRACE_FILE env var is configured.
 */
export const traceWriter = traceFile ? new TraceWriter(traceFile) : null;

if (traceWriter) {
  traceWriter.write({
    ph: 'M',
    cat: '__metadata',
    name: 'process_name',
    pid: process.pid,
    tid: 1000,
    args: { name: 'powersync' }
  });
}
