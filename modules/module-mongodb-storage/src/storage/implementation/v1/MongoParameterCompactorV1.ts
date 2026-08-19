import { mongo } from '@powersync/lib-service-mongodb';
import { InternalOpId } from '@powersync/service-core';
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
   * operation-id range. `key` is a residual predicate because it follows the `_id` range.
   *
   * This is not super efficient - it my require filtering through many keys for the same lookup.
   * Note that in those cases, the reads for this lookup would also be slow - this is not fundamentally
   * worse.
   *
   * The V3 storage format uses an index more suitable for this.
   */
  protected deleteFilter(doc: mongo.Document): mongo.Document {
    return {
      'key.g': doc.key.g as number,
      lookup: doc.lookup,
      _id: { $lt: doc._id },
      key: doc.key
    };
  }

  /** Uses the default `_id` index for the exact operation-id match. */
  protected deleteTombstoneFilter(doc: mongo.Document): mongo.Document {
    return {
      'key.g': doc.key.g as number,
      lookup: doc.lookup,
      _id: doc._id,
      key: doc.key
    };
  }
}
