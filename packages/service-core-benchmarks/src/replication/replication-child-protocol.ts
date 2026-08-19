export const REPLICATION_CHILD_PROTOCOL_VERSION = 1 as const;

export type ReplicationChildCommandKind =
  | 'initialize'
  | 'setup_iteration'
  | 'release_replication'
  | 'collect_evidence'
  | 'cleanup_iteration'
  | 'monitor_start'
  | 'monitor_stop'
  | 'shutdown'
  | 'abort';

export interface ReplicationChildCommand {
  readonly protocolVersion: typeof REPLICATION_CHILD_PROTOCOL_VERSION;
  readonly direction: 'command';
  readonly kind: ReplicationChildCommandKind;
  readonly runId: string;
  readonly iterationId?: string;
  readonly requestId: string;
  readonly payload: unknown;
}

export interface ReplicationChildResponseEvent {
  readonly protocolVersion: typeof REPLICATION_CHILD_PROTOCOL_VERSION;
  readonly direction: 'event';
  readonly kind: 'response';
  readonly runId: string;
  readonly iterationId?: string;
  readonly requestId: string;
  readonly command: ReplicationChildCommandKind;
  readonly payload: unknown;
}

export interface ReplicationChildFatalEvent {
  readonly protocolVersion: typeof REPLICATION_CHILD_PROTOCOL_VERSION;
  readonly direction: 'event';
  readonly kind: 'fatal';
  readonly runId: string;
  readonly iterationId?: string;
  readonly requestId?: string;
  readonly error: { readonly name: string; readonly message: string; readonly stack?: string };
}

export type ReplicationChildEvent = ReplicationChildResponseEvent | ReplicationChildFatalEvent;

export function monotonicMilliseconds(serializedNanoseconds: string): number {
  const nanoseconds = BigInt(serializedNanoseconds);
  return Number(nanoseconds) / 1_000_000;
}

export function serializeError(error: unknown): ReplicationChildFatalEvent['error'] {
  if (error instanceof Error) {
    return { name: error.name, message: error.message, stack: error.stack };
  }
  return { name: 'Error', message: String(error) };
}
