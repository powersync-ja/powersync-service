import {
  REPLICATION_CHILD_PROTOCOL_VERSION,
  ReplicationChildCommandEnvelope,
  ReplicationChildCommandKind,
  ReplicationChildCommandPayloads,
  ReplicationChildEvent,
  ReplicationChildResponsePayloads
} from './replication-child-protocol.js';

export interface ReplicationChildTransport {
  send<Kind extends ReplicationChildCommandKind>(command: ReplicationChildCommandEnvelope<Kind>): void;
  onMessage(listener: (event: ReplicationChildEvent) => void): () => void;
  onExit(listener: (code: number | null, signal: NodeJS.Signals | null) => void): () => void;
  kill(signal: NodeJS.Signals): void;
}

export interface ReplicationChildControllerOptions {
  readonly runId: string;
  readonly delay?: (milliseconds: number) => Promise<void>;
}

type ChildState = 'new' | 'initialized' | 'iteration-ready' | 'running' | 'shutting-down' | 'failed' | 'stopped';

interface PendingRequest {
  readonly command: { readonly kind: ReplicationChildCommandKind; readonly iterationId?: string };
  readonly resolve: (payload: unknown) => void;
  readonly reject: (error: Error) => void;
}

const ALLOWED_COMMANDS: Record<ChildState, readonly ReplicationChildCommandKind[]> = {
  new: ['initialize', 'abort'],
  initialized: ['setup_iteration', 'shutdown', 'abort'],
  'iteration-ready': ['release_replication', 'monitor_start', 'cleanup_iteration', 'abort'],
  running: ['collect_evidence', 'monitor_start', 'monitor_stop', 'cleanup_iteration', 'abort'],
  'shutting-down': [],
  failed: [],
  stopped: []
};

export class ReplicationChildController {
  private readonly pending = new Map<string, PendingRequest>();
  private readonly delay: (milliseconds: number) => Promise<void>;
  private readonly exitPromise: Promise<void>;
  private resolveExit!: () => void;
  private state: ChildState = 'new';
  private requestSequence = 0;
  private exited = false;

  constructor(
    private readonly transport: ReplicationChildTransport,
    private readonly options: ReplicationChildControllerOptions
  ) {
    this.delay = options.delay ?? ((milliseconds) => new Promise((resolve) => setTimeout(resolve, milliseconds)));
    this.exitPromise = new Promise((resolve) => (this.resolveExit = resolve));
    transport.onMessage((event) => this.handleMessage(event));
    transport.onExit((code, signal) => this.handleExit(code, signal));
  }

  request<Kind extends ReplicationChildCommandKind>(
    kind: Kind,
    payload: ReplicationChildCommandPayloads[Kind],
    iterationId?: string
  ): Promise<ReplicationChildResponsePayloads[Kind]> {
    this.assertAllowed(kind);
    const requestId = `request-${++this.requestSequence}`;
    const command: ReplicationChildCommandEnvelope<Kind> = {
      protocolVersion: REPLICATION_CHILD_PROTOCOL_VERSION,
      direction: 'command',
      kind,
      runId: this.options.runId,
      iterationId,
      requestId,
      payload
    };
    this.transitionOnSend(kind);
    const response = new Promise<ReplicationChildResponsePayloads[Kind]>((resolve, reject) => {
      this.pending.set(requestId, {
        command,
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
    if (this.state === 'stopped') return;
    const acknowledgement = this.request('shutdown', {}).then(
      () => true,
      () => false
    );
    const gracefulExit = Promise.all([acknowledgement, this.exitPromise]).then(([acknowledged]) => acknowledged);
    if (await this.beforeTimeout(gracefulExit, 10_000)) return;

    this.transport.kill('SIGTERM');
    if (
      await this.beforeTimeout(
        this.exitPromise.then(() => true),
        5_000
      )
    )
      return;

    this.transport.kill('SIGKILL');
    await this.exitPromise;
    throw new Error('Replication child required force termination');
  }

  private async beforeTimeout(completion: Promise<boolean>, milliseconds: number): Promise<boolean> {
    return await Promise.race([completion, this.delay(milliseconds).then(() => false)]);
  }

  private handleMessage(event: ReplicationChildEvent): void {
    if (event.protocolVersion !== REPLICATION_CHILD_PROTOCOL_VERSION) {
      this.fail(new Error(`Unsupported replication child protocol version ${event.protocolVersion}`));
      return;
    }
    if (event.runId !== this.options.runId) {
      this.fail(new Error(`Received child message for stale run ${event.runId}`));
      return;
    }
    if (event.kind === 'fatal') {
      const error = new Error(`Replication child failed: ${event.error.message}`);
      if (event.error.stack != null) error.stack = `${error.stack}\nChild stack:\n${event.error.stack}`;
      this.fail(error);
      return;
    }
    const pending = this.pending.get(event.requestId);
    if (pending == null) {
      this.fail(new Error(`Received response for unknown request ${event.requestId}`));
      return;
    }
    if (pending.command.kind !== event.command || pending.command.iterationId !== event.iterationId) {
      this.fail(new Error(`Received out-of-order response for ${event.requestId}`));
      return;
    }
    this.pending.delete(event.requestId);
    this.transitionOnResponse(event.command);
    pending.resolve(event.payload);
  }

  private handleExit(code: number | null, signal: NodeJS.Signals | null): void {
    this.exited = true;
    this.resolveExit();
    if (this.state === 'shutting-down') {
      this.state = 'stopped';
    } else {
      this.fail(
        new Error(
          signal == null
            ? `Replication child exited unexpectedly with code ${code}`
            : `Replication child exited unexpectedly with signal ${signal}`
        )
      );
    }
  }

  private assertAllowed(kind: ReplicationChildCommandKind): void {
    if (!ALLOWED_COMMANDS[this.state].includes(kind)) {
      throw new Error(`Command ${kind} is invalid while child is ${this.state}`);
    }
  }

  private transitionOnSend(kind: ReplicationChildCommandKind): void {
    if (kind === 'shutdown' || kind === 'abort') this.state = 'shutting-down';
  }

  private transitionOnResponse(kind: ReplicationChildCommandKind): void {
    if (kind === 'initialize') this.state = 'initialized';
    if (kind === 'setup_iteration') this.state = 'iteration-ready';
    if (kind === 'release_replication') this.state = 'running';
    if (kind === 'cleanup_iteration') this.state = 'initialized';
  }

  private fail(error: Error): void {
    if (!this.exited) this.state = 'failed';
    for (const request of this.pending.values()) request.reject(error);
    this.pending.clear();
  }
}
