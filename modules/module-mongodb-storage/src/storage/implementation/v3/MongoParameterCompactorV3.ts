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
    group_id: number,
    checkpoint: InternalOpId,
    options: CompactOptions,
    getCollectionsCb: () => Promise<mongo.Collection<mongo.Document>[]>,
    private readonly batchSize = 10_000
  ) {
    super(db, group_id, checkpoint, options, getCollectionsCb);
  }

  protected async readCompactedBefore(): Promise<InternalOpId> {
    const stream = (await this.db.sync_rules.findOne(
      { _id: this.group_id },
      { projection: { parameter_compaction: 1 } }
    )) as ReplicationStreamDocumentV3 | null;
    return stream?.parameter_compaction?.compacted_before == null
      ? 0n
      : BigInt(stream.parameter_compaction.compacted_before);
  }

  protected async persistCompactedBefore(compactedBefore: InternalOpId): Promise<void> {
    await this.db.sync_rules.updateOne({ _id: this.group_id }, {
      $max: { 'parameter_compaction.compacted_before': compactedBefore }
    } as any);
  }

  /*
   * The cursor update deliberately happens in the shared base class only after all collections
   * have completed. This query is the MongoDB-side half-open range for each collection.
   */
  protected override compactionFilter(compactedBefore?: InternalOpId): mongo.Document {
    if (compactedBefore == null) {
      throw new Error('Missing V3 parameter compaction cursor');
    }
    return {
      _id: {
        $gte: compactedBefore,
        $lt: this.checkpoint
      }
    };
  }

  protected override get parameterCompactionBatchSize(): number {
    return this.batchSize;
  }
}
