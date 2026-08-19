import { ChildProcess, fork } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { ReplicationChildTransport } from './ReplicationChildController.js';
import { ReplicationChildCommand, ReplicationChildEvent } from './replication-child-protocol.js';

export interface ForkedReplicationChildTransportOptions {
  readonly stdout?: (chunk: string) => void;
  readonly stderr?: (chunk: string) => void;
}

export class ForkedReplicationChildTransport implements ReplicationChildTransport {
  private readonly child: ChildProcess;

  constructor(options: ForkedReplicationChildTransportOptions = {}) {
    this.child = fork(fileURLToPath(new URL('./replication-child.ts', import.meta.url)), [], {
      execArgv: ['--loader', 'ts-node/esm'],
      serialization: 'advanced',
      stdio: ['ignore', 'pipe', 'pipe', 'ipc']
    });
    this.child.stdout?.setEncoding('utf8');
    this.child.stderr?.setEncoding('utf8');
    this.child.stdout?.on('data', (chunk: string) => options.stdout?.(chunk));
    this.child.stderr?.on('data', (chunk: string) => options.stderr?.(chunk));
  }

  get pid(): number | undefined {
    return this.child.pid;
  }

  send(command: ReplicationChildCommand): void {
    if (!this.child.connected) throw new Error('Replication child IPC channel is closed');
    this.child.send(command);
  }

  onMessage(listener: (event: ReplicationChildEvent) => void): () => void {
    const handler = (message: unknown) => listener(message as ReplicationChildEvent);
    this.child.on('message', handler);
    return () => this.child.off('message', handler);
  }

  onExit(listener: (code: number | null, signal: NodeJS.Signals | null) => void): () => void {
    this.child.on('exit', listener);
    return () => this.child.off('exit', listener);
  }

  kill(signal: NodeJS.Signals): void {
    this.child.kill(signal);
  }
}
