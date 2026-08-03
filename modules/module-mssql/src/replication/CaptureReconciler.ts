import { ErrorCode, ReplicationAssertionError, ServiceError } from '@powersync/lib-services-framework';
import { JsonValue, SourceTable, storage } from '@powersync/service-core';
import * as t from 'ts-codec';
import { CaptureInstance } from '../common/CaptureInstance.js';

/**
 * Thrown when the capture instance a replicated table was pinned to has been dropped. Fatal for the
 * replication job: a replacement is never adopted in place because it may capture a different
 * schema, and the dropped instance cannot be restored, so a new sync deploy is required.
 *
 * Deliberately not a `DatabaseQueryError` (the poller treats those as recoverable) and not a
 * `CDCDataExpiredError` (which triggers a full stream restart).
 */
export class CaptureInstanceMissingError extends ServiceError {
  constructor(message: string) {
    super(ErrorCode.PSYNC_S1601, message);
  }
}

/**
 * Thrown when a sync configuration table cannot be replicated yet - it does not exist, or CDC has
 * not been enabled for it. Fatal for the replication job, since it must not advance past a table it
 * cannot replicate, but recoverable without a new sync deploy: the stream never committed anything
 * without the table, so it starts normally once the table is ready.
 */
export class SourceTableNotReadyError extends ServiceError {
  constructor(message: string) {
    super(ErrorCode.PSYNC_S1602, message);
  }
}

/**
 * Thrown when a replicated table is dropped or renamed. Fatal for the replication job: the table can
 * no longer be polled, and changes committed before it went away may not have been read yet, so
 * continuing would commit past them and skip those rows permanently.
 */
export class SourceTableUnavailableError extends ServiceError {
  constructor(message: string) {
    super(ErrorCode.PSYNC_S1603, message);
  }
}

/**
 * Opaque source metadata persisted for MSSQL capture-instance-pinned source tables.
 *
 * We store the CDC change-table object id rather than only the capture-instance name, because
 * capture-instance names can be reused.
 */
export const MSSQLSourceMetadata = t.object({
  captureTableObjectId: t.number
});
export type MSSQLSourceMetadata = t.Decoded<typeof MSSQLSourceMetadata>;

/**
 * Parse persisted opaque source metadata into {@link MSSQLSourceMetadata}, or null for
 * legacy metadata-free records.
 */
export function readCaptureMetadata(value: JsonValue | undefined): MSSQLSourceMetadata | null {
  if (value == null) {
    return null;
  }
  return MSSQLSourceMetadata.decode(value as t.Encoded<typeof MSSQLSourceMetadata>);
}

/**
 * Build the source-owned reconciler for an MSSQL physical table.
 *
 * `availableInstances` are the capture instances currently available for the source table, ordered
 * newest-first (as returned by {@link getCaptureInstances}). They must be loaded before entering
 * storage reconciliation so the reconciler itself performs no source-database I/O.
 *
 * Rules (applied to identity-compatible candidates):
 * - No compatible candidates: new binding - pin to the newest available capture instance.
 * - All compatible lack metadata: legacy binding - pin them in place to the newest available
 *   capture instance, matching the instance the legacy streaming path would select.
 * - Compatible share one persisted capture identity that is still available: pinned binding - keep
 *   them compatible and persist the same identity. A newer instance is never adopted while the bound
 *   one is still usable.
 * - Compatible share one persisted capture identity that has been dropped: fail the replication job.
 *   A replacement instance may capture a different schema, so it is never adopted in place - a new
 *   sync deploy is required.
 * - Mixed metadata-free + pinned, or multiple pinned identities: invalid persisted state - fail.
 */
export function createCaptureReconciler(availableInstances: CaptureInstance[]) {
  return (({ source, candidates }) => {
    if (availableInstances.length === 0) {
      throw new SourceTableNotReadyError(
        `No CDC capture instance is available for source table ${source.schema}.${source.name}. ` +
          `Enable CDC for this table to start replicating it.`
      );
    }

    // Partition on the generic identity fields (schema/name, object id, replica-id columns).
    // Capture-instance pinning rules are layered on top of the candidates this accepts.
    const compatible: SourceTable[] = [];
    const incompatibleTables: SourceTable[] = [];
    for (const candidate of candidates) {
      if (storage.sourceIdentityCompatible(source, candidate)) {
        compatible.push(candidate);
      } else {
        incompatibleTables.push(candidate);
      }
    }

    if (compatible.length === 0) {
      // New physical-table binding. Pin to the newest valid capture instance.
      const newest = availableInstances[0];
      return {
        compatibleTables: [],
        incompatibleTables,
        newTableValues: { sourceMetadata: captureMetadata(newest.objectId) }
      };
    }

    const pinnedObjectIds = new Set<number>();
    let metadataFreeCount = 0;
    for (const candidate of compatible) {
      const metadata = readCaptureMetadata(candidate.sourceMetadata);
      if (metadata == null) {
        metadataFreeCount += 1;
      } else {
        pinnedObjectIds.add(metadata.captureTableObjectId);
      }
    }

    // Invalid persisted state: never guess which snapshot state belongs to which capture schema.
    if (pinnedObjectIds.size > 0 && metadataFreeCount > 0) {
      throw new ReplicationAssertionError(
        `Source table ${source.schema}.${source.name} has a mixture of legacy (metadata-free) and ` +
          `capture-instance-pinned records. This is invalid persisted state.`
      );
    }
    // V3 may persist multiple SourceTable records for one physical table, but every record in the
    // same replication stream must use the same capture source.
    if (pinnedObjectIds.size > 1) {
      throw new ReplicationAssertionError(
        `Source table ${source.schema}.${source.name} has multiple persisted capture identities ` +
          `(${[...pinnedObjectIds].join(', ')}). This is invalid persisted state.`
      );
    }

    if (pinnedObjectIds.size === 0) {
      // Backfill legacy records with the same newest instance the old streaming implementation
      // would have selected.
      const newest = availableInstances[0];
      const sourceMetadata = captureMetadata(newest.objectId);
      return {
        compatibleTables: compatible.map((candidate) => candidate.withSourceMetadata(sourceMetadata)),
        incompatibleTables,
        newTableValues: { sourceMetadata }
      };
    }

    // Pinned binding. Resolve the persisted capture identity against what is available.
    const [captureTableObjectId] = [...pinnedObjectIds];
    const available = availableInstances.some((instance) => instance.objectId === captureTableObjectId);
    if (!available) {
      // The pinned capture instance has been dropped. Any replacement may capture a different
      // schema, so adopting one in place would silently change what is replicated, and the existing
      // records' snapshot state belongs to a capture schema that no longer exists. Stop the
      // replication job; adopting a new capture instance requires a new sync deploy.
      throw new CaptureInstanceMissingError(
        `The CDC capture instance (object id ${captureTableObjectId}) for source table ` +
          `${source.schema}.${source.name} is no longer available. Deploy the sync configuration as a ` +
          `new replication stream to replicate this table against a new capture instance.`
      );
    }
    return {
      compatibleTables: compatible,
      incompatibleTables,
      newTableValues: { sourceMetadata: captureMetadata(captureTableObjectId) }
    };
  }) satisfies storage.SourceTableCandidateReconciler;
}

function captureMetadata(captureTableObjectId: number): MSSQLSourceMetadata {
  return { captureTableObjectId };
}
