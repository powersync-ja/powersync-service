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
import { MIKRO_ORM_MYSQL_STORAGE_TYPE } from '../../types/types.js';

export const mysqlMikroOrmStorageDialect: MikroOrmStorageDialect = {
  type: MIKRO_ORM_MYSQL_STORAGE_TYPE,
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

    const filters = options.dataBuckets.map((request) => ({
      bucketName: request.bucket,
      opId: { $gt: request.start, $lte: options.checkpoint }
    }));
    const sqlEntityManager = options.em as SqlEntityManager;
    const queryBuilder = sqlEntityManager
      .createQueryBuilder(BucketData, 'bucket_data')
      .select('*')
      .where({
        groupId: options.groupId,
        $or: filters
      })
      .orderBy({ bucketName: 'ASC', opId: 'ASC' })
      .limit(options.limit);

    yield* streamQueryBuilder(queryBuilder.stream());
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
