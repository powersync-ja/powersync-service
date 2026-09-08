import { ReplicationAssertionError } from '@powersync/lib-services-framework';
import { SaveOperationTag, storage } from '@powersync/service-core';
import { ProjectedChangeStreamDocument } from './RawChangeStream.js';
import { SourceRowConverter } from './SourceRowConverter.js';

export const MONGO_PREPARATION_WORKER = new URL(
  './replication/MongoRowPreparation.worker.js',
  import.meta.resolve('@powersync/service-module-mongodb')
);

/** Convert a change for a resolved, synced collection and apply it through the storage writer. */
export async function writeMongoChange(
  batch: storage.BucketStorageBatch,
  table: storage.SourceTable,
  change: ProjectedChangeStreamDocument,
  converter: SourceRowConverter
): Promise<storage.FlushedResult | null> {
  if (
    batch.saveRaw &&
    (change.operationType === 'insert' || change.operationType === 'update' || change.operationType === 'replace') &&
    change.fullDocument != null
  ) {
    return batch.saveRaw({
      tag: change.operationType === 'insert' ? SaveOperationTag.INSERT : SaveOperationTag.UPDATE,
      sourceTable: table,
      raw: change.fullDocument,
      worker: MONGO_PREPARATION_WORKER,
      convert: () => ({ row: converter.rawToSqliteRow(change.fullDocument!).row, replicaId: change.documentKey._id })
    });
  }
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
