import { ErrorCode, ReplicationAssertionError, ServiceError } from '@powersync/lib-services-framework';
import { JsonValue, SourceTableCandidate, storage } from '@powersync/service-core';
import * as t from 'ts-codec';
import type { MSSQLTableReconciliationContext } from './MSSQLTableReconciliationContext.js';
import { MSSQLTableReconciliationState } from './MSSQLTableReconciliationContext.js';

/**
 * The pinned capture instance was dropped and cannot be replaced in this stream.
 */
export class CaptureInstanceMissingError extends ServiceError {
  constructor(message: string) {
    super(ErrorCode.PSYNC_S1601, message);
  }
}

/**
 * A configured table does not exist or does not have CDC enabled yet.
 */
export class SourceTableNotReadyError extends ServiceError {
  constructor(message: string) {
    super(ErrorCode.PSYNC_S1602, message);
  }
}

/**
 * A replicated table is unavailable or no longer matches its persisted source binding.
 */
export class SourceTableUnavailableError extends ServiceError {
  constructor(message: string) {
    super(ErrorCode.PSYNC_S1603, message);
  }
}

/**
 * Persisted capture-table identity. Names are not enough because SQL Server can reuse them.
 */
export const MSSQLSourceMetadata = t.object({
  captureTableObjectId: t.number
});
export type MSSQLSourceMetadata = t.Decoded<typeof MSSQLSourceMetadata>;

/**
 * Parse persisted capture metadata, returning null for legacy records.
 */
export function readCaptureMetadata(value: JsonValue): MSSQLSourceMetadata | null {
  if (value == null) {
    return null;
  }
  return MSSQLSourceMetadata.decode(value as t.Encoded<typeof MSSQLSourceMetadata>);
}

/**
 * Reconcile a discovered source-table state against persisted bindings. Ready tables reconcile
 * against capture instances ordered newest first. New and legacy bindings use the newest instance;
 * pinned bindings keep their instance or fail if it is gone.
 */
export function createCaptureReconciler(context: MSSQLTableReconciliationContext) {
  return (({ source, candidates }) => {
    if (context.state === MSSQLTableReconciliationState.TABLE_MISSING) {
      if (candidates.length > 0) {
        // The table is missing, but we already recorded it. Display the corresponding error.
        throw sourceTableUnavailableError(source);
      }
      throw new SourceTableNotReadyError(
        `Source table ${formatQualifiedTableName(source.schema, source.name)} from the sync configuration does not ` +
          `exist. Create the table and enable CDC for it.`
      );
    }

    // Apply capture pinning after the shared identity check.
    const compatible: SourceTableCandidate[] = [];
    const incompatibleTables: SourceTableCandidate[] = [];
    for (const candidate of candidates) {
      if (storage.sourceIdentityCompatible(source, candidate)) {
        compatible.push(candidate);
      } else {
        incompatibleTables.push(candidate);
      }
    }

    if (compatible.length === 0) {
      if (candidates.length > 0) {
        // An overlapping persisted binding with a different source identity is not a new table.
        // This can happen when a table is dropped and recreated while replication is stopped.
        // Reusing the old snapshot or replacing it during a normal job restart would silently
        // adopt a different physical table without a sync config deployment boundary.
        throw sourceTableUnavailableError(source);
      }

      if (context.state === MSSQLTableReconciliationState.CDC_DISABLED) {
        throw new SourceTableNotReadyError(
          `CDC is not enabled for source table ${formatQualifiedTableName(source.schema, source.name)}, which ` +
            `matches the sync configuration. Enable CDC for this table to start replicating it.`
        );
      }

      // Pin a new binding to the newest instance.
      const newest = context.captureInstances[0];
      return {
        compatibleTables: [],
        incompatibleTables,
        newTableValues: { sourceMetadata: captureMetadata(newest.objectId) }
      };
    }

    if (context.state === MSSQLTableReconciliationState.CDC_DISABLED) {
      throw new CaptureInstanceMissingError(
        `CDC is no longer enabled for source table ${formatQualifiedTableName(source.schema, source.name)}. ` +
          `Re-enable CDC, then redeploy the sync config to adopt the replacement capture instance. Its ` +
          `already-replicated data is retained until the redeployed sync config becomes active.`
      );
    }

    const availableInstances = context.captureInstances;

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

    // Never guess which capture schema existing snapshots belong to.
    if (pinnedObjectIds.size > 0 && metadataFreeCount > 0) {
      throw new ReplicationAssertionError(
        `Source table ${source.schema}.${source.name} has a mixture of legacy (metadata-free) and ` +
          `capture-instance-pinned records. This is invalid persisted state.`
      );
    }
    // All records for a physical table must use the same capture instance.
    if (pinnedObjectIds.size > 1) {
      throw new ReplicationAssertionError(
        `Source table ${source.schema}.${source.name} has multiple persisted capture identities ` +
          `(${[...pinnedObjectIds].join(', ')}). This is invalid persisted state.`
      );
    }

    if (pinnedObjectIds.size === 0) {
      // Backfill legacy records with the instance the old code would have selected.
      const newest = availableInstances[0];
      const sourceMetadata = captureMetadata(newest.objectId);
      return {
        compatibleTables: compatible.map((candidate) => candidate.withSourceMetadata(sourceMetadata)),
        incompatibleTables,
        newTableValues: { sourceMetadata }
      };
    }

    const [captureTableObjectId] = [...pinnedObjectIds];
    const available = availableInstances.some((instance) => instance.objectId === captureTableObjectId);
    if (!available) {
      // A replacement may capture a different schema, so do not adopt it in place.
      throw new CaptureInstanceMissingError(
        `The CDC capture instance (object id ${captureTableObjectId}) for source table ` +
          `${source.schema}.${source.name} is no longer available. Deploy a new sync config to ` +
          `replicate this table against a new capture instance.`
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

function sourceTableUnavailableError(source: { schema: string; name: string }): SourceTableUnavailableError {
  return new SourceTableUnavailableError(
    `Table ${formatQualifiedTableName(source.schema, source.name)} no longer matches the source table binding ` +
      `selected by this replication process. It may have been dropped and recreated, renamed, or had its ` +
      `replication identity changed. Redeploy the sync config to adopt the replacement. Its already-replicated ` +
      `data is retained until the redeployed sync config becomes active.`
  );
}

function formatQualifiedTableName(schema: string, table: string): string {
  const escapeIdentifier = (identifier: string) => `[${identifier.replace(/]/g, ']]')}]`;
  return `${escapeIdentifier(schema)}.${escapeIdentifier(table)}`;
}
