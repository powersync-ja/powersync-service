import type { ReplicationBenchmarkObservation, ReplicationReleaseObservation } from '../types/ReplicationBenchmark.js';
import type { ReplicationChildClassDescriptor } from './ReplicationChildClassLoader.js';

export const REPLICATION_CHILD_PROTOCOL_VERSION = 3 as const;

export interface ReplicationChildInitializePayload {
  readonly storage: ReplicationChildClassDescriptor;
}

export interface ReplicationChildSetupIterationPayload {
  readonly syncRules: string;
  readonly storageVersion: number;
  readonly source: ReplicationChildClassDescriptor;
  readonly syncParameters: Record<string, unknown>;
}

export interface ReplicationChildCommandPayloads {
  readonly initialize: ReplicationChildInitializePayload;
  readonly setup_iteration: ReplicationChildSetupIterationPayload;
  readonly release_replication: { readonly waitForSnapshot?: boolean };
  readonly collect_evidence: Record<string, never>;
  readonly cleanup_iteration: Record<string, never>;
  readonly monitor_start: Record<string, never>;
  readonly monitor_stop: Record<string, never>;
  readonly shutdown: Record<string, never>;
  readonly abort: Record<string, never>;
}

export interface ReplicationChildEvidencePayload {
  readonly checkpoint: string | null;
  readonly operations: ReplicationBenchmarkObservation['operations'];
  readonly snapshotDone: boolean;
  readonly bucketCount: number;
}

export interface ReplicationChildResourceSamplePayload {
  readonly pid: number;
  readonly cpu: { readonly user: number; readonly system: number };
  readonly memory: { readonly rss: number };
  readonly sampledAtNs: string;
}

export interface ReplicationChildResponsePayloads {
  readonly initialize: { readonly environment: object; readonly pid: number };
  readonly setup_iteration: { readonly replicationStreamName: string };
  readonly release_replication: ReplicationReleaseObservation;
  readonly collect_evidence: ReplicationChildEvidencePayload;
  readonly cleanup_iteration: Record<string, never>;
  readonly monitor_start: ReplicationChildResourceSamplePayload;
  readonly monitor_stop: ReplicationChildResourceSamplePayload;
  readonly shutdown: Record<string, never>;
  readonly abort: Record<string, never>;
}

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

export interface ReplicationChildCommandEnvelope<Kind extends ReplicationChildCommandKind> {
  readonly protocolVersion: typeof REPLICATION_CHILD_PROTOCOL_VERSION;
  readonly direction: 'command';
  readonly kind: Kind;
  readonly runId: string;
  readonly iterationId?: string;
  readonly requestId: string;
  readonly payload: ReplicationChildCommandPayloads[Kind];
}

export type ReplicationChildCommand = {
  [Kind in ReplicationChildCommandKind]: ReplicationChildCommandEnvelope<Kind>;
}[ReplicationChildCommandKind];

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
