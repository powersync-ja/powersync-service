import { ReplicationAssertionError } from '@powersync/lib-services-framework';
import { SaveOperationTag, storage } from '@powersync/service-core';
import { ProjectedChangeStreamDocument } from './RawChangeStream.js';
import { SourceRowConverter } from './SourceRowConverter.js';

/** Convert a change for a resolved, synced collection and apply it through the storage writer. */
export async function writeMongoChange(
  batch: storage.BucketStorageBatch,
  table: storage.SourceTable,
  change: ProjectedChangeStreamDocument,
  converter: SourceRowConverter
): Promise<storage.FlushedResult | null> {
  if (change.operationType == 'insert') {
    const { row: baseRecord, replicaId: _replicaId } = converter.rawToSqliteRow(change.fullDocument);
    return await batch.save({
      tag: SaveOperationTag.INSERT,
      sourceTable: table,
      before: undefined,
      beforeReplicaId: undefined,
      after: baseRecord,
      // Same as _replicaId
      // We specifically need to use the source _id, not the converted one in baseRecord,
      // to preserve _id uniqueness properties.
      afterReplicaId: change.documentKey._id
    });
  } else if (change.operationType == 'update' || change.operationType == 'replace') {
    if (change.fullDocument == null) {
      // Treat as delete
      return await batch.save({
        tag: SaveOperationTag.DELETE,
        sourceTable: table,
        before: undefined,
        beforeReplicaId: change.documentKey._id
      });
    }
    const { row: after, replicaId: _replicaId } = converter.rawToSqliteRow(change.fullDocument!);
    return await batch.save({
      tag: SaveOperationTag.UPDATE,
      sourceTable: table,
      before: undefined,
      beforeReplicaId: undefined,
      after: after,
      afterReplicaId: change.documentKey._id // Same as _replicaId
    });
  } else if (change.operationType == 'delete') {
    return await batch.save({
      tag: SaveOperationTag.DELETE,
      sourceTable: table,
      before: undefined,
      beforeReplicaId: change.documentKey._id
    });
  } else {
    throw new ReplicationAssertionError(`Unsupported operation: ${change.operationType}`);
  }
}
