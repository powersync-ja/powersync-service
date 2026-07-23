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
import { streamBucketDataRowsFromSql } from '../sql/bucket-data-read.js';

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

    yield* streamBucketDataRowsFromSql(options, (queryOptions) => {
      const requestedRows = queryOptions.dataBuckets
        .map((_, index) => `${index == 0 ? 'SELECT' : 'UNION ALL SELECT'} ? AS bucket_name, ? AS start_op_id`)
        .join(' ');
      const params = queryOptions.dataBuckets.flatMap((request) => [request.bucket, request.start]);

      return {
        sql: `
          SELECT bucket_data.*
          FROM (${requestedRows}) AS requested
          JOIN bucket_data FORCE INDEX (bucket_data_bucket_op_index)
            ON bucket_data.group_id = ?
           AND bucket_data.bucket_name = requested.bucket_name
           AND bucket_data.op_id > requested.start_op_id
           AND bucket_data.op_id <= ?
          ORDER BY bucket_data.bucket_name ASC, bucket_data.op_id ASC
          LIMIT ?
        `,
        params: [...params, queryOptions.groupId, queryOptions.checkpoint, queryOptions.limit]
      };
    });
  },
  createCheckpointWatcher: () => new InProcessMikroOrmCheckpointWatcher()
};
