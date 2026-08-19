import { mongo } from '@powersync/lib-service-mongodb';
import { CompactOptions, InternalOpId } from '@powersync/service-core';
import { MongoParameterCompactor } from '../MongoParameterCompactor.js';
import { ReplicationStreamDocumentV3 } from './models.js';
import type { VersionedPowerSyncMongoV3 } from './VersionedPowerSyncMongoV3.js';

/**
 * Incrementally compacts V3 parameter indexes using the stream operation sequence as a work
 * cursor. The cursor is advanced only after every parameter index has completed the same range.
 */
export class MongoParameterCompactorV3 extends MongoParameterCompactor {
  declare protected readonly db: VersionedPowerSyncMongoV3;

  constructor(
    db: VersionedPowerSyncMongoV3,
    replicationStreamId: number,
    checkpoint: InternalOpId,
    options: CompactOptions,
    private readonly batchSize = 10_000
  ) {
    super(db, replicationStreamId, checkpoint, options, batchSize);
  }

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

  /**
   * Uses each parameter index collection's default `_id` index for the half-open op-id range.
   * The cursor update deliberately happens in the shared base class only after all collections
   * have completed.
   */
  protected override compactionFilter(compactedBefore: InternalOpId): mongo.Document {
    return {
      _id: {
        $gte: compactedBefore,
        $lt: this.checkpoint
      }
    };
  }

  protected shouldCompactDocument(_doc: { _id: bigint; key: mongo.Document }): boolean {
    return true;
  }

  /** Uses the `{ lookup: 1, key: 1, _id: -1 }` `lookup_op_id` index. */
  protected deleteFilter(doc: mongo.Document): mongo.Document {
    return {
      lookup: doc.lookup,
      key: doc.key,
      _id: { $lt: doc._id }
    };
  }

  /** Uses the default `_id` index for the exact operation-id match. */
  protected deleteTombstoneFilter(doc: mongo.Document): mongo.Document {
    return {
      _id: doc._id,
      lookup: doc.lookup,
      key: doc.key
    };
  }
}
