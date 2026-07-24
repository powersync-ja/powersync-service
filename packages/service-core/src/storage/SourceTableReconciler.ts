import { JsonValue, SourceEntityDescriptor } from './SourceEntity.js';
import { SourceTable, SourceTableId } from './SourceTable.js';

/**
 * Result of classifying overlapping persisted candidates against a discovered source entity.
 *
 * Returned by a {@link SourceTableCandidateReconciler}. The reconciler owns source-specific
 * compatibility and metadata selection; storage owns membership reconciliation and persistence.
 */
export interface SourceTableCandidateResolution {
  /**
   * Existing candidates representing the same source generation, that storage may reuse.
   *
   * Returning ids rather than hydrated tables keeps the reconciler's output to classification
   * only; storage retains ownership of the candidate instances and persisted documents.
   * Reconciler implementations should return the ids directly from `candidates`, preserving the
   * storage-specific id representation supplied by the storage implementation.
   *
   * Candidates not included here are treated as incompatible (renames, relation-id changes,
   * replica-identity changes, superseded capture identities, ...) and returned for the connector
   * to drop.
   */
  sourceCompatibleCandidateIds: ReadonlySet<SourceTableId>;

  /**
   * Opaque metadata to persist on any record created by this resolution.
   *
   * All records created in one resolution receive this same value, so v3 never mixes
   * metadata-free and pinned records for the same physical-table binding.
   *
   * Undefined preserves legacy metadata-free behavior.
   */
  sourceMetadata?: JsonValue;
}

/**
 * Source-provided callback that classifies overlapping persisted candidates.
 *
 * Contract:
 * - Deterministic and free of storage mutations.
 * - May be asynchronous, but is awaited while source-table resolution is in progress and may be
 *   running inside a storage transaction. Avoid slow or unbounded external I/O where possible.
 * - `candidates` are all persisted source tables overlapping the discovered entity by
 *   `(schema + name) OR object/relation id`.
 *
 * @throws if the persisted candidate state is invalid for this source (e.g. conflicting metadata).
 */
export interface SourceTableCandidateReconcilerInput {
  /**
   * Source entity currently being resolved.
   */
  source: SourceEntityDescriptor;

  /**
   * Persisted source tables overlapping the discovered entity by
   * `(schema + name) OR object/relation id`.
   */
  candidates: ReadonlyArray<Readonly<SourceTable>>;
}

export type SourceTableCandidateReconciler = (
  input: SourceTableCandidateReconcilerInput
) => SourceTableCandidateResolution | Promise<SourceTableCandidateResolution>;

/**
 * Compare replica-id column lists for exact structural equality.
 *
 * Shared helper for source reconcilers so source-specific policy is not pushed back into storage.
 */
export function sameReplicaIdColumns(left: SourceTable['replicaIdColumns'], right: SourceEntityDescriptor): boolean {
  const target = right.replicaIdColumns;
  return (
    left.length == target.length &&
    left.every(
      (column, index) =>
        column.name == target[index].name && column.type == target[index].type && column.typeId == target[index].typeId
    )
  );
}

/**
 * Generic identity comparison shared by connectors: schema/name, object id, and replica-id columns.
 *
 * This reproduces the pre-existing storage identity match. Connectors layer source-specific
 * policy (such as MSSQL capture pinning) on top of the candidates this accepts.
 */
export function sourceIdentityCompatible(source: SourceEntityDescriptor, candidate: SourceTable): boolean {
  return (
    candidate.schema == source.schema &&
    candidate.name == source.name &&
    (source.objectId == null || candidate.objectId == source.objectId) &&
    sameReplicaIdColumns(candidate.replicaIdColumns, source)
  );
}

/**
 * Default reconciler preserving legacy metadata-free behavior.
 *
 * Used by storage when a connector does not supply a `reconcileSourceTables` callback. Every
 * candidate matching the generic identity fields is compatible, and no metadata is persisted.
 */
export const defaultSourceTableReconciler = (({ source, candidates }) => {
  const sourceCompatibleCandidateIds = new Set<SourceTableId>();
  for (const candidate of candidates) {
    if (sourceIdentityCompatible(source, candidate)) {
      sourceCompatibleCandidateIds.add(candidate.id);
    }
  }
  return { sourceCompatibleCandidateIds, sourceMetadata: undefined };
}) satisfies SourceTableCandidateReconciler;
