import { mongo } from '@powersync/lib-service-mongodb';
import { logger } from '@powersync/lib-services-framework';
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

  override async compact() {
    const startedAt = Date.now();
    const stream = (await this.db.sync_rules.findOne(
      { _id: this.group_id },
      { projection: { parameter_compaction: 1 } }
    )) as ReplicationStreamDocumentV3 | null;
    const compactedBefore =
      stream?.parameter_compaction?.compacted_before == null
        ? 0n
        : BigInt(stream.parameter_compaction.compacted_before);

    logger.info(
      `Incrementally compacting parameters for sync config ${this.group_id} from ${compactedBefore} up to checkpoint ${this.checkpoint}`
    );

    const result = await this.compactCollections(compactedBefore);

    // This update is deliberately after all collections have completed. $max makes overlapping
    // compactors safe when a slower invocation finishes after a faster one.
    await this.db.sync_rules.updateOne({ _id: this.group_id }, {
      $max: { 'parameter_compaction.compacted_before': this.checkpoint }
    } as any);

    const durationSeconds = (Date.now() - startedAt) / 1000;
    logger.info(
      `Incremental parameter compaction completed for sync config ${this.group_id}: ` +
        `collections=${result.collections}, scanned=${result.scannedEntries}, distinct=${result.distinctIdentities}, ` +
        `deleted=${result.deletedEntries}, cursor=${compactedBefore}->${this.checkpoint}, duration=${durationSeconds.toFixed(1)}s`
    );
  }

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

  protected override compactionSort(): mongo.Document {
    return { _id: 1 };
  }

  protected override get parameterCompactionBatchSize(): number {
    return this.batchSize;
  }
}
