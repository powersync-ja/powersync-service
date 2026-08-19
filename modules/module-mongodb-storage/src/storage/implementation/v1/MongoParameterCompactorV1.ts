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
      { _id: this.group_id },
      { projection: { parameter_compaction: 1 } }
    )) as SyncRuleDocumentV1 | null;
    return stream?.parameter_compaction?.compacted_before == null
      ? 0n
      : BigInt(stream.parameter_compaction.compacted_before);
  }

  protected async persistCompactedBefore(compactedBefore: InternalOpId): Promise<void> {
    await this.db.sync_rules.updateOne({ _id: this.group_id }, {
      $max: { 'parameter_compaction.compacted_before': compactedBefore }
    } as any);
  }

  /**
   * The shared V1 collection is scanned using only its default `_id` index. Group filtering is
   * deliberately done in shouldCompactDocument(), avoiding an index requirement on `key.g`.
   */
  protected override compactionFilter(compactedBefore?: InternalOpId): mongo.Document {
    if (compactedBefore == null) {
      throw new Error('Missing V1 parameter compaction cursor');
    }
    return { _id: { $gte: compactedBefore, $lt: this.checkpoint } };
  }

  protected override shouldCompactDocument(doc: { _id: bigint; key: mongo.Document }): boolean {
    return doc._id < this.checkpoint && doc.key.g === this.group_id;
  }

  protected deleteFilter(doc: mongo.Document): mongo.Document {
    return {
      'key.g': doc.key.g as number,
      lookup: doc.lookup,
      _id: { $lt: doc._id },
      key: doc.key
    };
  }

  protected deleteTombstoneFilter(doc: mongo.Document): mongo.Document {
    return {
      'key.g': doc.key.g as number,
      lookup: doc.lookup,
      _id: doc._id,
      key: doc.key
    };
  }
}
