import { mongo } from '@powersync/lib-service-mongodb';
import { bson, InternalOpId } from '@powersync/service-core';
import { MongoParameterCompactor } from '../MongoParameterCompactor.js';
import { SyncRuleDocumentV1 } from './models.js';
import { VersionedPowerSyncMongoV1 } from './VersionedPowerSyncMongoV1.js';

export class MongoParameterCompactorV1 extends MongoParameterCompactor {
  declare protected readonly db: VersionedPowerSyncMongoV1;

  protected async getCollections(): Promise<mongo.Collection<mongo.Document>[]> {
    return [this.db.parameterIndexV1 as unknown as mongo.Collection<mongo.Document>];
  }

  protected async readCompactedBefore(): Promise<InternalOpId> {
    const stream = (await this.db.sync_rules.findOne(
      { _id: this.replicationStreamId },
      { projection: { parameter_compaction: 1 } }
    )) as SyncRuleDocumentV1 | null;
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

  // The shared V1 collection is scanned using only its default `_id` index. Filter the stream in
  // code so compaction does not require an index on `key.g`.
  protected override shouldCompactDocument(doc: { key: mongo.Document }): boolean {
    return doc.key.g === this.replicationStreamId;
  }

  /**
   * Uses the legacy `{ 'key.g': 1, lookup: 1, _id: 1 }` index to narrow the stream, lookup and
   * operation-id range. `key` is not part of that index at all, so it is a residual predicate applied
   * to every document the range scan returns.
   *
   * That scan may therefore have to filter through many keys for the same lookup, but the cost is
   * amortized: a single scan covers up to 1000 keys, and identities seen again in a later batch skip
   * the scan entirely - they are deleted by `_id`.
   *
   * The V3 storage format uses an index more suitable for this.
   */
  protected leadingHistoryDeleteFilter(
    lookup: bson.Binary,
    keys: mongo.Document[],
    before: InternalOpId
  ): mongo.Document {
    return {
      'key.g': this.replicationStreamId,
      lookup,
      key: { $in: keys },
      _id: { $lt: before }
    };
  }
}
