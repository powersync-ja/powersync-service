import { mongo } from '@powersync/lib-service-mongodb';
import { InternalOpId } from '@powersync/service-core';
import { MongoParameterCompactor } from '../MongoParameterCompactor.js';
import { ReplicationStreamDocumentV3 } from './models.js';
import type { VersionedPowerSyncMongoV3 } from './VersionedPowerSyncMongoV3.js';

/**
 * Incrementally compacts V3 parameter indexes using the stream operation sequence as a work
 * cursor. The cursor is advanced only after every parameter index has completed the same range.
 */
export class MongoParameterCompactorV3 extends MongoParameterCompactor {
  declare protected readonly db: VersionedPowerSyncMongoV3;

  protected async getCollections(): Promise<mongo.Collection<mongo.Document>[]> {
    const collections = await this.db.listParameterIndexCollections(this.replicationStreamId);
    return collections.map(({ collection }) => collection as unknown as mongo.Collection<mongo.Document>);
  }

  protected async readCompactedBefore(): Promise<InternalOpId> {
    const stream = (await this.db.sync_rules.findOne(
      { _id: this.replicationStreamId },
      { projection: { parameter_compaction: 1 } }
    )) as ReplicationStreamDocumentV3 | null;
    return stream?.parameter_compaction?.compacted_before == null
      ? 0n
      : BigInt(stream.parameter_compaction.compacted_before);
  }

  protected async persistCompactedBefore(compactedBefore: InternalOpId): Promise<void> {
    await this.db.sync_rules.updateOne(
      { _id: this.replicationStreamId },
      {
        $max: { 'parameter_compaction.compacted_before': compactedBefore }
      }
    );
  }

  protected shouldCompactDocument(_doc: { _id: bigint; key: mongo.Document }): boolean {
    return true;
  }

  /** Uses the `{ lookup: 1, key: 1, _id: -1 }` `lookup_op_id` index. */
  protected leadingHistoryDeleteFilter(lookup: unknown, keys: mongo.Document[], before: InternalOpId): mongo.Document {
    return {
      lookup,
      key: { $in: keys },
      _id: { $lt: before }
    };
  }
}
