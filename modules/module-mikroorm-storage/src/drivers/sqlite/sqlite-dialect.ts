import type { SqlEntityManager } from '@mikro-orm/sql';
import {
  BucketData,
  BucketDataSchema,
  BucketParameters,
  BucketParametersSchema,
  CurrentData,
  CurrentDataSchema,
  Instance,
  InstanceSchema,
  SourceTable,
  SourceTableSchema,
  SyncRules,
  SyncRulesSchema,
  WriteCheckpoint,
  WriteCheckpointSchema
} from '../../entities/entities-index.js';
import { InProcessMikroOrmCheckpointWatcher, MikroOrmStorageDialect } from '../../storage/MikroOrmStorageDialect.js';
import { MIKRO_ORM_SQLITE_STORAGE_TYPE } from '../../types/types.js';

export const sqliteMikroOrmStorageDialect: MikroOrmStorageDialect = {
  type: MIKRO_ORM_SQLITE_STORAGE_TYPE,
  entityClasses: [
    BucketDataSchema,
    BucketParametersSchema,
    CurrentDataSchema,
    InstanceSchema,
    SourceTableSchema,
    SyncRulesSchema,
    WriteCheckpointSchema
  ],
  bucketDataEntity: BucketData,
  bucketParametersEntity: BucketParameters,
  currentDataEntity: CurrentData,
  instanceEntity: Instance,
  sourceTableEntity: SourceTable,
  syncRulesEntity: SyncRules,
  writeCheckpointEntity: WriteCheckpoint,
  async *streamBucketDataRows(options) {
    if (options.dataBuckets.length == 0) {
      return;
    }

    // SQLite reads can complete synchronously enough that tight polling loops
    // starve replication/checkpoint work in the same process. Yield once before
    // the query so single-process unified mode remains cooperative.
    await new Promise<void>((resolve) => setImmediate(resolve));

    const sqlEntityManager = options.em as SqlEntityManager;
    const sortedBuckets = [...options.dataBuckets].sort((a, b) => a.bucket.localeCompare(b.bucket));
    let remainingLimit = options.limit;

    for (const request of sortedBuckets) {
      if (remainingLimit <= 0) {
        break;
      }

      // Query each bucket as its own indexed range scan. In SQLite this was faster than joining a VALUES table for
      // many buckets because it avoids cross-bucket planning and temporary sorting work.
      const queryBuilder = sqlEntityManager
        .createQueryBuilder(BucketData, 'bucket_data')
        .select('*')
        .where({
          groupId: options.groupId,
          bucketName: request.bucket,
          opId: { $gt: request.start, $lte: options.checkpoint }
        })
        .orderBy({ opId: 'ASC' })
        .limit(remainingLimit);

      for await (const row of streamQueryBuilder(queryBuilder.stream())) {
        yield row;
        remainingLimit--;
      }
    }
  },
  createCheckpointWatcher: () => new InProcessMikroOrmCheckpointWatcher()
};

async function* streamQueryBuilder<T>(stream: AsyncIterableIterator<T>): AsyncIterable<T> {
  const iterator = stream[Symbol.asyncIterator]();
  try {
    while (true) {
      const result = await iterator.next();
      if (result.done) {
        return;
      }
      yield result.value;
    }
  } finally {
    await iterator.return?.();
  }
}
