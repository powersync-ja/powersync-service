import { ChildProcess, fork } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { NdjsonDrainObservation } from '../utils/ndjson-drain.js';
import { ApiClientCommand, ApiClientConfiguration, ApiClientResponse } from './api-client-protocol.js';

/** Owns one iteration's HTTP client. */
export class ForkedApiClient {
  private readonly child: ChildProcess;
  private readonly pending = new Map<
    number,
    { resolve: (response: ApiClientResponse) => void; reject: (error: Error) => void }
  >();
  private readonly exited: Promise<void>;
  private readonly onAbort: () => void;
  private sequence = 0;
  private failure?: Error;
  private closing = false;
  private hasExited = false;

  constructor(private readonly signal: AbortSignal) {
    signal.throwIfAborted();
    const sourceMode = fileURLToPath(import.meta.url).endsWith('.ts');
    this.child = fork(new URL(sourceMode ? './api-client-child.ts' : './api-client-child.js', import.meta.url), [], {
      cwd: fileURLToPath(new URL('../../', import.meta.url)),
      execArgv: sourceMode ? ['--loader', import.meta.resolve('ts-node/esm')] : [],
      serialization: 'advanced',
      stdio: ['ignore', 'ignore', 'pipe', 'ipc']
    });

    let stderr = '';
    this.child.stderr?.setEncoding('utf8');
    this.child.stderr?.on('data', (chunk: string) => (stderr = (stderr + chunk).slice(-8_192)));
    this.child.on('message', (response: ApiClientResponse) => {
      const pending = this.pending.get(response.id);
      if (pending == null) return;
      this.pending.delete(response.id);
      if ('error' in response) {
        const error = new Error(`API client failed: ${response.error.message}`);
        if (response.error.stack != null) error.stack += `\nClient stack:\n${response.error.stack}`;
        this.fail(error);
        pending.reject(error);
      } else {
        pending.resolve(response);
      }
    });
    this.child.on('error', (error) => this.fail(error));
    this.exited = new Promise((resolve) => {
      this.child.once('exit', (code, exitSignal) => {
        this.hasExited = true;
        this.fail(new Error(`API client exited (code ${code}, signal ${exitSignal})${stderr ? `\n${stderr}` : ''}`));
        resolve();
      });
    });
    this.onAbort = () => {
      this.fail(new Error('API client benchmark was aborted', { cause: signal.reason }));
      this.child.kill('SIGTERM');
    };
    signal.addEventListener('abort', this.onAbort, { once: true });
    if (signal.aborted) this.onAbort();
  }

  get pid(): number | undefined {
    return this.child.pid;
  }

  async initialize(configuration: ApiClientConfiguration): Promise<void> {
    await this.request({ id: ++this.sequence, kind: 'initialize', configuration });
  }

  async open(): Promise<NdjsonDrainObservation> {
    return await this.observe({ id: ++this.sequence, kind: 'open' });
  }

  async drain(checkpoint: string): Promise<NdjsonDrainObservation> {
    return await this.observe({ id: ++this.sequence, kind: 'drain', checkpoint });
  }

  async dispose(): Promise<void> {
    this.closing = true;
    try {
      if (this.hasExited) return;
      if (this.failure == null) {
        const graceful = this.request({ id: ++this.sequence, kind: 'close' }).then(
          () => this.exited.then(() => true),
          () => false
        );
        if (await beforeTimeout(graceful, 1_000)) return;
      }
      this.child.kill('SIGTERM');
      if (
        await beforeTimeout(
          this.exited.then(() => true),
          5_000
        )
      )
        return;
      this.child.kill('SIGKILL');
      if (
        await beforeTimeout(
          this.exited.then(() => true),
          5_000
        )
      ) {
        throw new Error('API client required force termination');
      }
      throw new Error('API client did not exit after SIGKILL');
    } finally {
      this.signal.removeEventListener('abort', this.onAbort);
    }
  }

  private async observe(command: ApiClientCommand): Promise<NdjsonDrainObservation> {
    const response = await this.request(command);
    if ('error' in response || response.observation == null)
      throw new Error('API client did not return an observation');
    return response.observation;
  }

  private request(command: ApiClientCommand): Promise<ApiClientResponse> {
    if (this.failure != null) return Promise.reject(this.failure);
    if (this.closing && command.kind !== 'close') return Promise.reject(new Error('API client is closing'));
    return new Promise((resolve, reject) => {
      this.pending.set(command.id, { resolve, reject });
      this.child.send(command, (error) => {
        if (error != null) this.fail(error);
      });
    });
  }

  private fail(error: Error): void {
    this.failure ??= error;
    for (const pending of this.pending.values()) pending.reject(this.failure);
    this.pending.clear();
  }
}

async function beforeTimeout(completion: Promise<boolean>, milliseconds: number): Promise<boolean> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      completion,
      new Promise<boolean>((resolve) => {
        timer = setTimeout(() => resolve(false), milliseconds);
        timer.unref();
      })
    ]);
  } finally {
    clearTimeout(timer);
  }
}
