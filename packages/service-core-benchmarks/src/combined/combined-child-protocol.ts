import type * as jose from 'jose';
import type { ReplicationChildClassDescriptor } from '../replication/ReplicationChildClassLoader.js';
import type { ReplicationReleaseObservation } from '../types/ReplicationBenchmark.js';

export const COMBINED_CHILD_PROTOCOL_VERSION = 2 as const;

export interface CombinedChildInitializePayload {
  readonly storage: ReplicationChildClassDescriptor;
}

export interface CombinedChildSetupIterationPayload {
  readonly syncRules: string;
  readonly storageVersion: number;
  readonly source: ReplicationChildClassDescriptor;
  readonly port: number;
  readonly jwk: jose.JWK;
  readonly syncParameters: Record<string, unknown>;
}

export interface CombinedChildCommandPayloads {
  readonly initialize: CombinedChildInitializePayload;
  readonly setup_iteration: CombinedChildSetupIterationPayload;
  readonly release_replication: { readonly waitForSnapshot?: boolean };
  readonly collect_evidence: Record<string, never>;
  readonly monitor_start: Record<string, never>;
  readonly monitor_stop: Record<string, never>;
  readonly cleanup_iteration: Record<string, never>;
  readonly shutdown: Record<string, never>;
  readonly abort: Record<string, never>;
}

interface CombinedChildEvidenceBase {
  /** Exact bucket-storage checkpoint id exposed to the sync protocol. */
  readonly storageCheckpoint: string;
  readonly operations: readonly { readonly op: string; readonly object_id?: string; readonly data?: string | null }[];
  readonly snapshotDone: boolean;
  readonly bucketCount: number;
}

export type CombinedChildEvidencePayload =
  | (CombinedChildEvidenceBase & {
      readonly checkpoint: null;
      readonly checkpointVisibleAtNs?: never;
    })
  | (CombinedChildEvidenceBase & {
      /** Source position associated with the storage checkpoint. */
      readonly checkpoint: string;
      /** Child-clock diagnostic only. Durations spanning the parent and child must not use this value. */
      readonly checkpointVisibleAtNs: string;
    });

export interface CombinedChildResourceSamplePayload {
  readonly pid: number;
  readonly cpu: { readonly user: number; readonly system: number };
  readonly memory: { readonly rss: number };
  readonly sampledAtNs: string;
}

export interface CombinedChildResponsePayloads {
  readonly initialize: { readonly environment: object; readonly pid: number };
  readonly setup_iteration: { readonly replicationStreamName: string; readonly endpoint: string };
  readonly release_replication: ReplicationReleaseObservation;
  readonly collect_evidence: CombinedChildEvidencePayload;
  readonly monitor_start: CombinedChildResourceSamplePayload;
  readonly monitor_stop: CombinedChildResourceSamplePayload;
  readonly cleanup_iteration: Record<string, never>;
  readonly shutdown: Record<string, never>;
  readonly abort: Record<string, never>;
}

export type CombinedChildCommandKind = keyof CombinedChildCommandPayloads;

export interface CombinedChildCommandEnvelope<Kind extends CombinedChildCommandKind> {
  readonly protocolVersion: typeof COMBINED_CHILD_PROTOCOL_VERSION;
  readonly direction: 'command';
  readonly kind: Kind;
  readonly runId: string;
  readonly requestId: string;
  readonly iterationId?: string;
  readonly payload: CombinedChildCommandPayloads[Kind];
}

export type CombinedChildCommand = {
  [Kind in CombinedChildCommandKind]: CombinedChildCommandEnvelope<Kind>;
}[CombinedChildCommandKind];

export interface CombinedChildResponseEvent {
  readonly protocolVersion: typeof COMBINED_CHILD_PROTOCOL_VERSION;
  readonly direction: 'event';
  readonly kind: 'response';
  readonly runId: string;
  readonly requestId: string;
  readonly iterationId?: string;
  readonly command: CombinedChildCommandKind;
  readonly payload: unknown;
}

export interface CombinedChildFatalEvent {
  readonly protocolVersion: typeof COMBINED_CHILD_PROTOCOL_VERSION;
  readonly direction: 'event';
  readonly kind: 'fatal';
  readonly runId: string;
  readonly requestId?: string;
  readonly iterationId?: string;
  readonly error: { readonly name: string; readonly message: string; readonly stack?: string };
}

export type CombinedChildEvent = CombinedChildResponseEvent | CombinedChildFatalEvent;

export function serializeCombinedChildError(error: unknown): CombinedChildFatalEvent['error'] {
  if (error instanceof Error) {
    return { name: error.name, message: error.message, stack: error.stack };
  }
  return { name: 'Error', message: String(error) };
}
