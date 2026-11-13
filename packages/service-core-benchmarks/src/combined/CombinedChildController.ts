import {
  COMBINED_CHILD_PROTOCOL_VERSION,
  CombinedChildCommandEnvelope,
  CombinedChildCommandKind,
  CombinedChildCommandPayloads,
  CombinedChildEvent,
  CombinedChildResponsePayloads
} from './combined-child-protocol.js';

export interface CombinedChildTransport {
  send<Kind extends CombinedChildCommandKind>(command: CombinedChildCommandEnvelope<Kind>): void;
  onMessage(listener: (event: CombinedChildEvent) => void): () => void;
  onExit(listener: (code: number | null, signal: NodeJS.Signals | null) => void): () => void;
  onError(listener: (error: Error) => void): () => void;
  kill(signal: NodeJS.Signals): void;
}

export interface CombinedChildControllerOptions {
  readonly runId: string;
  readonly delay?: (milliseconds: number) => Promise<void>;
}

type StableChildState = 'new' | 'initialized' | 'iteration-ready' | 'running';
type ChildState =
  | StableChildState
  | 'initializing'
  | 'setting-up'
  | 'releasing'
  | 'collecting'
  | 'monitoring'
  | 'cleaning-up'
  | 'shutting-down'
  | 'failed'
  | 'stopped';

interface PendingRequest {
  readonly command: { readonly kind: CombinedChildCommandKind; readonly iterationId?: string };
  readonly responseState?: StableChildState;
  readonly resolve: (payload: unknown) => void;
  readonly reject: (error: Error) => void;
}

const ALLOWED_COMMANDS: Record<StableChildState, readonly CombinedChildCommandKind[]> = {
  new: ['initialize', 'abort'],
  initialized: ['setup_iteration', 'shutdown', 'abort'],
  'iteration-ready': ['release_replication', 'monitor_start', 'monitor_stop', 'cleanup_iteration', 'abort'],
  running: ['collect_evidence', 'monitor_start', 'monitor_stop', 'cleanup_iteration', 'abort']
};

const PENDING_STATES: Partial<Record<CombinedChildCommandKind, ChildState>> = {
  initialize: 'initializing',
  setup_iteration: 'setting-up',
  release_replication: 'releasing',
  collect_evidence: 'collecting',
  monitor_start: 'monitoring',
  monitor_stop: 'monitoring',
  cleanup_iteration: 'cleaning-up',
  shutdown: 'shutting-down',
  abort: 'shutting-down'
};

export class CombinedChildController {
  private readonly pending = new Map<string, PendingRequest>();
  private readonly delay: (milliseconds: number) => Promise<void>;
  private readonly exitPromise: Promise<void>;
  private resolveExit!: () => void;
  private state: ChildState = 'new';
  private requestSequence = 0;
  private exited = false;
  private monitorRunning = false;

  constructor(
    private readonly transport: CombinedChildTransport,
    private readonly options: CombinedChildControllerOptions
  ) {
    this.delay = options.delay ?? unrefDelay;
    this.exitPromise = new Promise((resolve) => (this.resolveExit = resolve));
    transport.onMessage((event) => this.handleMessage(event));
    transport.onExit((code, signal) => this.handleExit(code, signal));
    transport.onError((error) => this.handleTransportError(error));
  }

  get hasExited(): boolean {
    return this.exited;
  }

  request<Kind extends CombinedChildCommandKind>(
    kind: Kind,
    payload: CombinedChildCommandPayloads[Kind],
    iterationId?: string
  ): Promise<CombinedChildResponsePayloads[Kind]> {
    const responseState = this.assertAllowed(kind);
    const requestId = `request-${++this.requestSequence}`;
    const command: CombinedChildCommandEnvelope<Kind> = {
      protocolVersion: COMBINED_CHILD_PROTOCOL_VERSION,
      direction: 'command',
      kind,
      runId: this.options.runId,
      iterationId,
      requestId,
      payload
    };
    this.state = PENDING_STATES[kind] ?? this.state;
    const response = new Promise<CombinedChildResponsePayloads[Kind]>((resolve, reject) => {
      this.pending.set(requestId, {
        command,
        responseState,
        resolve: resolve as (payload: unknown) => void,
        reject
      });
    });
    try {
      this.transport.send(command);
    } catch (error) {
      this.fail(error instanceof Error ? error : new Error(String(error)));
    }
    return response;
  }

  async shutdown(): Promise<void> {
    if (this.state === 'stopped' || this.exited) return;

    if (this.state === 'shutting-down' || (this.state !== 'failed' && !(this.state in ALLOWED_COMMANDS))) {
      await this.terminateAfterFailure();
      return;
    }

    const command = this.state === 'initialized' ? 'shutdown' : 'abort';
    const acknowledgement = this.request(command, {}).then(
      () => true,
      () => false
    );
    const gracefulExit = Promise.all([acknowledgement, this.exitPromise]).then(([acknowledged]) => acknowledged);
    if (await this.beforeTimeout(gracefulExit, 10_000)) return;

    this.transport.kill('SIGTERM');
    if (await this.waitForExit(5_000)) return;

    this.transport.kill('SIGKILL');
    if (await this.waitForExit(5_000)) throw new Error('Combined child required force termination');
    throw new Error('Combined child did not exit after SIGKILL');
  }

  private async terminateAfterFailure(): Promise<void> {
    if (this.exited) return;
    this.transport.kill('SIGTERM');
    if (await this.waitForExit(5_000)) return;
    this.transport.kill('SIGKILL');
    if (await this.waitForExit(5_000)) throw new Error('Combined child required force termination');
    throw new Error('Combined child did not exit after SIGKILL');
  }

  private async waitForExit(milliseconds: number): Promise<boolean> {
    return await this.beforeTimeout(
      this.exitPromise.then(() => true),
      milliseconds
    );
  }

  private async beforeTimeout(completion: Promise<boolean>, milliseconds: number): Promise<boolean> {
    return await Promise.race([completion, this.delay(milliseconds).then(() => false)]);
  }

  private handleMessage(event: CombinedChildEvent): void {
    if (event.direction !== 'event' || event.protocolVersion !== COMBINED_CHILD_PROTOCOL_VERSION) {
      this.fail(new Error(`Unsupported combined child protocol version ${event.protocolVersion}`));
      return;
    }
    if (event.runId !== this.options.runId) {
      this.fail(new Error(`Received combined child message for stale run ${event.runId}`));
      return;
    }
    if (event.kind === 'fatal') {
      const error = new Error(`Combined child failed: ${event.error.message}`);
      if (event.error.stack != null) error.stack = `${error.stack}\nChild stack:\n${event.error.stack}`;
      this.fail(error);
      return;
    }
    const pending = this.pending.get(event.requestId);
    if (pending == null) {
      this.fail(new Error(`Received response for unknown combined child request ${event.requestId}`));
      return;
    }
    if (pending.command.kind !== event.command || pending.command.iterationId !== event.iterationId) {
      this.fail(new Error(`Received out-of-order response for ${event.requestId}`));
      return;
    }
    this.pending.delete(event.requestId);
    this.state = pending.responseState ?? this.state;
    if (event.command === 'monitor_start') this.monitorRunning = true;
    if (event.command === 'monitor_stop') this.monitorRunning = false;
    pending.resolve(event.payload);
  }

  private handleExit(code: number | null, signal: NodeJS.Signals | null): void {
    if (this.exited) return;
    this.exited = true;
    this.resolveExit();
    if (this.state === 'shutting-down' || this.state === 'failed') {
      this.state = 'stopped';
      return;
    }
    this.state = 'failed';
    this.fail(
      new Error(
        signal == null
          ? `Combined child exited unexpectedly with code ${code}`
          : `Combined child exited unexpectedly with signal ${signal}`
      )
    );
  }

  private handleTransportError(error: Error): void {
    this.fail(new Error(`Combined child transport failed: ${error.message}`));
  }

  private assertAllowed(kind: CombinedChildCommandKind): StableChildState | undefined {
    if (this.state === 'failed' && kind === 'abort') return undefined;
    if (!(this.state in ALLOWED_COMMANDS) || !ALLOWED_COMMANDS[this.state as StableChildState].includes(kind)) {
      throw new Error(`Command ${kind} is invalid while combined child is ${this.state}`);
    }
    if (kind === 'monitor_start' && this.monitorRunning) {
      throw new Error('Command monitor_start is invalid while combined child monitor is running');
    }
    if (kind === 'monitor_stop' && !this.monitorRunning) {
      throw new Error('Command monitor_stop is invalid while combined child monitor is stopped');
    }
    if (kind === 'cleanup_iteration' && this.monitorRunning) {
      throw new Error('Command cleanup_iteration is invalid while combined child monitor is running');
    }

    const stableState = this.state as StableChildState;
    if (kind === 'initialize') return 'initialized';
    if (kind === 'setup_iteration') return 'iteration-ready';
    if (kind === 'release_replication') return 'running';
    if (kind === 'cleanup_iteration') return 'initialized';
    if (kind === 'collect_evidence' || kind === 'monitor_start' || kind === 'monitor_stop') return stableState;
    return undefined;
  }

  private fail(error: Error): void {
    if (!this.exited) this.state = 'failed';
    for (const request of this.pending.values()) request.reject(error);
    this.pending.clear();
  }
}

async function unrefDelay(milliseconds: number): Promise<void> {
  await new Promise<void>((resolve) => {
    const timer = setTimeout(resolve, milliseconds);
    timer.unref();
  });
}
