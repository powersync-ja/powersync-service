import { ServiceAssertionError } from '@powersync/lib-services-framework';
import { isDeepStrictEqual } from 'node:util';
import { JsonValue, SourceEntityDescriptor } from './SourceEntity.js';
import { SourceTable, SourceTableId, sourceTableIdEquals } from './SourceTable.js';

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
   * Return the original hydrated table when it is unchanged, or a modified hydrated copy when
   * source-owned values should be updated. Storage persists only allowlisted differences without
   * replacing the record, preserving its snapshot state and definition memberships.
   */
  compatibleTables: ReadonlyArray<SourceTable>;

  /**
   * Existing candidates that cannot be reused (renames, relation-id changes, replica-identity
   * changes, superseded source generations, ...). Storage returns these to the connector to drop.
   *
   * Every input candidate must be listed exactly once in either `compatibleTables` or
   * `incompatibleTables`.
   */
  incompatibleTables: ReadonlyArray<SourceTable>;

  /**
   * Values to persist on any source-table record created by this resolution.
   *
   * This is separate from compatible-candidate updates because incremental storage may both reuse
   * existing records and create a new record for uncovered sync-config memberships. Storage, not
   * the reconciler, decides whether a new record is required.
   */
  newTableValues: SourceTableCreateValues;
}

export interface SourceTableCreateValues {
  /**
   * Opaque metadata to persist on records created by this resolution.
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
  candidates: ReadonlyArray<SourceTable>;
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
export const defaultSourceTableReconciler: SourceTableCandidateReconciler = ({ source, candidates }) => {
  const compatibleTables: SourceTable[] = [];
  const incompatibleTables: SourceTable[] = [];
  for (const candidate of candidates) {
    if (sourceIdentityCompatible(source, candidate)) {
      compatibleTables.push(candidate);
    } else {
      incompatibleTables.push(candidate);
    }
  }
  return { compatibleTables, incompatibleTables, newTableValues: {} };
};

/**
 * Validate that a source reconciler explicitly partitions every supplied candidate exactly once.
 */
export function validateSourceTableCandidateResolution(
  candidates: ReadonlyArray<SourceTable>,
  resolution: SourceTableCandidateResolution
): void {
  const classifiedTables = [...resolution.compatibleTables, ...resolution.incompatibleTables];

  for (const candidate of candidates) {
    const classifications = classifiedTables.filter((table) => sourceTableIdEquals(table.id, candidate.id));
    if (classifications.length !== 1) {
      throw new ServiceAssertionError(
        `Source table candidate ${candidate.id.toString()} must be classified exactly once, got ${classifications.length}`
      );
    }
  }

  for (const table of classifiedTables) {
    if (!candidates.some((candidate) => sourceTableIdEquals(candidate.id, table.id))) {
      throw new ServiceAssertionError(`Source table reconciliation returned unknown candidate ${table.id.toString()}`);
    }
  }
}

/**
 * An allowlisted difference between a persisted candidate and the copy the reconciler returned.
 */
export interface SourceTableMetadataUpdate {
  id: SourceTableId;
  sourceMetadata: JsonValue | undefined;
}

/**
 * Diff the reconciler's compatible tables against the candidates they came from, and return the
 * allowlisted changes storage should persist.
 *
 * Storage owns this diff so every backend applies the same rules; each backend only supplies the
 * write. Comparison is by value, so a reconciler that rebuilds structurally identical metadata on
 * each resolution does not cause a write every time.
 *
 * Call {@link validateSourceTableCandidateResolution} first - this assumes every compatible table
 * corresponds to a supplied candidate.
 */
export function diffSourceTableUpdates(
  candidates: ReadonlyArray<SourceTable>,
  resolution: SourceTableCandidateResolution
): SourceTableMetadataUpdate[] {
  const updates: SourceTableMetadataUpdate[] = [];
  for (const resolvedTable of resolution.compatibleTables) {
    const candidate = candidates.find((table) => sourceTableIdEquals(table.id, resolvedTable.id));
    if (candidate == null) {
      throw new ServiceAssertionError(
        `Source table reconciliation returned unknown candidate ${resolvedTable.id.toString()}`
      );
    }
    if (isDeepStrictEqual(candidate.sourceMetadata, resolvedTable.sourceMetadata)) {
      continue;
    }
    updates.push({ id: resolvedTable.id, sourceMetadata: resolvedTable.sourceMetadata });
  }
  return updates;
}
