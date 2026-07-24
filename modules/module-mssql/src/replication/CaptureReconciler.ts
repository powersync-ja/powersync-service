import { ReplicationAssertionError } from '@powersync/lib-services-framework';
import { JsonValue, SourceTable, SourceTableId, storage } from '@powersync/service-core';
import { CaptureInstance } from '../common/CaptureInstance.js';

/**
 * Opaque source metadata persisted for MSSQL capture-instance-pinned source tables.
 *
 * We store the CDC change-table object id rather than only the capture-instance name, because
 * capture-instance names can be reused.
 */
export type MSSQLSourceMetadata = {
  captureTableObjectId: number;
};

/**
 * Parse persisted opaque source metadata into {@link MSSQLSourceMetadata}, or null for
 * legacy metadata-free records.
 */
export function readCaptureMetadata(value: JsonValue | undefined): MSSQLSourceMetadata | null {
  if (value == null || typeof value !== 'object' || Array.isArray(value)) {
    return null;
  }
  const captureTableObjectId = (value as Record<string, unknown>).captureTableObjectId;
  if (typeof captureTableObjectId !== 'number') {
    return null;
  }
  return { captureTableObjectId };
}

/**
 * Compare the generic identity fields MSSQL owns: schema/name, source object id and replica-id
 * columns. Capture-instance pinning rules are layered on top of the candidates this accepts.
 */
function identityCompatible(source: storage.SourceEntityDescriptor, candidate: SourceTable): boolean {
  return storage.sourceIdentityCompatible(source, candidate);
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
 * - All compatible lack metadata: legacy binding - keep them compatible, persist no metadata.
 * - Compatible share one persisted capture identity: pinned binding - keep them compatible and
 *   persist the same identity; fail if that capture instance is no longer available.
 * - Mixed metadata-free + pinned, or multiple pinned identities: invalid persisted state - fail.
 */
export function createCaptureReconciler(availableInstances: CaptureInstance[]) {
  return (({ source, candidates }) => {
    const compatible = candidates.filter((candidate) => identityCompatible(source, candidate));

    if (compatible.length === 0) {
      // New physical-table binding. Pin to the newest valid capture instance, if any.
      const newest = availableInstances[0];
      if (newest == null) {
        // No capture instance is available yet; resolve as metadata-free so the table can still be
        // tracked. It will be pinned once CDC is enabled and a capture instance exists.
        return { sourceCompatibleCandidateIds: new Set<SourceTableId>(), sourceMetadata: undefined };
      }
      return {
        sourceCompatibleCandidateIds: new Set<SourceTableId>(),
        sourceMetadata: { captureTableObjectId: newest.objectId } satisfies MSSQLSourceMetadata
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

    const sourceCompatibleCandidateIds = new Set<SourceTableId>(compatible.map((candidate) => candidate.id));

    if (pinnedObjectIds.size === 0) {
      // Legacy binding. Preserve metadata-free behavior for the whole resolution.
      return { sourceCompatibleCandidateIds, sourceMetadata: undefined };
    }

    // Pinned binding. Resolve the persisted capture identity against what is available.
    const [captureTableObjectId] = [...pinnedObjectIds];
    const available = availableInstances.some((instance) => instance.objectId === captureTableObjectId);
    if (!available) {
      throw new ReplicationAssertionError(
        `The pinned capture instance (object id ${captureTableObjectId}) for source table ` +
          `${source.schema}.${source.name} is no longer available. Re-enable CDC or redeploy the ` +
          `sync configuration to adopt a new capture instance; PowerSync will not silently switch instances.`
      );
    }
    return {
      sourceCompatibleCandidateIds,
      sourceMetadata: { captureTableObjectId } satisfies MSSQLSourceMetadata
    };
  }) satisfies storage.SourceTableCandidateReconciler;
}
