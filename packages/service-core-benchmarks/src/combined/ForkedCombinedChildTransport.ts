import { ChildProcess, fork } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { CombinedChildTransport } from './CombinedChildController.js';
import {
  CombinedChildCommandEnvelope,
  CombinedChildCommandKind,
  CombinedChildEvent
} from './combined-child-protocol.js';

export interface ForkedCombinedChildTransportOptions {
  readonly stdout?: (chunk: string) => void;
  readonly stderr?: (chunk: string) => void;
}

export interface CombinedChildProcessOptions {
  readonly entrypoint: URL;
  readonly execArgv: readonly string[];
}

export function resolveCombinedChildProcessOptions(
  transportModuleUrl: string = import.meta.url
): CombinedChildProcessOptions {
  const sourceMode = fileURLToPath(transportModuleUrl).endsWith('.ts');
  return {
    entrypoint: new URL(sourceMode ? './combined-child.ts' : './combined-child.js', transportModuleUrl),
    execArgv: sourceMode ? ['--loader', 'ts-node/esm'] : []
  };
}

export class ForkedCombinedChildTransport implements CombinedChildTransport {
  private readonly child: ChildProcess;
  private readonly errorListeners = new Set<(error: Error) => void>();

  constructor(options: ForkedCombinedChildTransportOptions = {}) {
    const processOptions = resolveCombinedChildProcessOptions();
    this.child = fork(fileURLToPath(processOptions.entrypoint), [], {
      execArgv: [...processOptions.execArgv],
      serialization: 'advanced',
      stdio: ['ignore', 'pipe', 'pipe', 'ipc']
    });
    this.child.stdout?.setEncoding('utf8');
    this.child.stderr?.setEncoding('utf8');
    this.child.stdout?.on('data', (chunk: string) => options.stdout?.(chunk));
    this.child.stderr?.on('data', (chunk: string) => options.stderr?.(chunk));
    this.child.on('error', (error) => this.reportError(error));
  }

  get pid(): number | undefined {
    return this.child.pid;
  }

  send<Kind extends CombinedChildCommandKind>(command: CombinedChildCommandEnvelope<Kind>): void {
    if (!this.child.connected) throw new Error('Combined child IPC channel is closed');
    this.child.send(command, (error) => {
      if (error != null) this.reportError(error);
    });
  }

  onMessage(listener: (event: CombinedChildEvent) => void): () => void {
    const handler = (message: unknown) => listener(message as CombinedChildEvent);
    this.child.on('message', handler);
    return () => this.child.off('message', handler);
  }

  onExit(listener: (code: number | null, signal: NodeJS.Signals | null) => void): () => void {
    this.child.on('exit', listener);
    return () => this.child.off('exit', listener);
  }

  onError(listener: (error: Error) => void): () => void {
    this.errorListeners.add(listener);
    return () => this.errorListeners.delete(listener);
  }

  kill(signal: NodeJS.Signals): void {
    this.child.kill(signal);
  }

  private reportError(error: Error): void {
    for (const listener of this.errorListeners) listener(error);
  }
}
