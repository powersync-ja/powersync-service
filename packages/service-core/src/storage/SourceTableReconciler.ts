import { ServiceAssertionError } from '@powersync/lib-services-framework';
import { isDeepStrictEqual } from 'node:util';
import { JsonValue, SourceEntityDescriptor } from './SourceEntity.js';
import { SourceTable, SourceTableId, sourceTableIdEquals } from './SourceTable.js';

/**
 * A source connector's classification of overlapping persisted tables.
 */
export interface SourceTableCandidateResolution {
  /**
   * Records storage can reuse. Copies may include updated source metadata.
   */
  compatibleTables: ReadonlyArray<SourceTable>;

  /**
   * Records that cannot be reused. Every candidate must appear in exactly one result list.
   */
  incompatibleTables: ReadonlyArray<SourceTable>;

  /**
   * Values for records storage creates during this resolution.
   */
  newTableValues: SourceTableCreateValues;
}

export interface SourceTableCreateValues {
  /**
   * Source metadata for new records.
   */
  sourceMetadata?: JsonValue;
}

/**
 * Input to a source-owned reconciliation callback. The callback may run inside a storage
 * transaction, so it must not mutate storage or perform slow external work.
 */
export interface SourceTableCandidateReconcilerInput {
  /**
   * Source entity being resolved.
   */
  source: SourceEntityDescriptor;

  /**
   * Persisted tables overlapping by name or object id.
   */
  candidates: ReadonlyArray<SourceTable>;
}

export type SourceTableCandidateReconciler = (
  input: SourceTableCandidateReconcilerInput
) => SourceTableCandidateResolution | Promise<SourceTableCandidateResolution>;

/**
 * Compare replica-id columns in order.
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
 * Compare the shared source-table identity fields.
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
 * Default identity-based reconciliation for connectors without source-specific metadata.
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
 * Check that every candidate was classified exactly once.
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
 * A source-metadata update to persist.
 */
export interface SourceTableMetadataUpdate {
  id: SourceTableId;
  sourceMetadata: JsonValue | undefined;
}

/**
 * Return source-metadata changes from compatible candidates, comparing metadata by value.
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
