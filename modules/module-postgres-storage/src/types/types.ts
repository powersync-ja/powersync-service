import * as lib_postgres from '@powersync/lib-service-postgres';
import * as pg_wire from '@powersync/service-jpgwire';
import { configFile } from '@powersync/service-types';
import * as t from 'ts-codec';
export * as models from './models/models-index.js';

export const MAX_BATCH_RECORD_COUNT = 2000;

export const MAX_BATCH_ESTIMATED_SIZE = 5_000_000;

export const MAX_BATCH_CURRENT_DATA_SIZE = 50_000_000;

/**
 * Default for `batch_limits.max_statement_size`.
 *
 * The rows are passed to Postgres as a single JSON parameter, which is serialized to a string
 * before being sent. Node limits a string to roughly 512MB. The estimate used for chunking does
 * not account for the JSON escaping of the already-serialized row data, so the actual parameter
 * can be close to double the estimate. 64MB keeps a wide margin for that.
 */
export const MAX_FLUSH_STATEMENT_SIZE = 64_000_000;

export const BatchLimits = t.object({
  /**
   * Maximum size of operations we write in a single transaction.
   */
  max_estimated_size: t.number.optional(),
  /**
   * Limit number of documents to write in a single transaction.
   */
  max_record_count: t.number.optional(),
  /**
   * Maximum estimated size of the rows written by a single INSERT statement when flushing
   * bucket_data and bucket_parameters.
   *
   * A single source row can produce many operations when it is evaluated into many buckets,
   * which is not limited by `max_estimated_size`. Flushes larger than this limit are split
   * into multiple statements in the same transaction.
   */
  max_statement_size: t.number.optional()
});

export type BatchLimits = t.Encoded<typeof BatchLimits>;

export const OperationBatchLimits = BatchLimits.and(
  t.object({
    /**
     * Maximum size of size of current_data documents we lookup at a time.
     */
    max_current_data_batch_size: t.number.optional()
  })
);

export type OperationBatchLimits = t.Encoded<typeof OperationBatchLimits>;

export const PostgresStorageConfig = configFile.BaseStorageConfig.and(lib_postgres.BasePostgresConnectionConfig).and(
  t.object({
    /**
     * Allow batch operation limits to be configurable.
     * Postgres has less batch size restrictions compared to MongoDB.
     * Increasing limits can drastically improve replication performance, but
     * can come at the cost of higher memory usage or potential issues.
     */
    batch_limits: OperationBatchLimits.optional()
  })
);

export type PostgresStorageConfig = t.Encoded<typeof PostgresStorageConfig>;
export type PostgresStorageConfigDecoded = t.Decoded<typeof PostgresStorageConfig>;

export type RequiredOperationBatchLimits = Required<OperationBatchLimits>;

export type NormalizedPostgresStorageConfig = pg_wire.NormalizedConnectionConfig & {
  batch_limits: RequiredOperationBatchLimits;
  max_pool_size: number;
};

export const normalizePostgresStorageConfig = (
  baseConfig: PostgresStorageConfigDecoded
): NormalizedPostgresStorageConfig => {
  return {
    ...lib_postgres.normalizeConnectionConfig(baseConfig),
    batch_limits: {
      max_current_data_batch_size: baseConfig.batch_limits?.max_current_data_batch_size ?? MAX_BATCH_CURRENT_DATA_SIZE,
      max_estimated_size: baseConfig.batch_limits?.max_estimated_size ?? MAX_BATCH_ESTIMATED_SIZE,
      max_record_count: baseConfig.batch_limits?.max_record_count ?? MAX_BATCH_RECORD_COUNT,
      max_statement_size: baseConfig.batch_limits?.max_statement_size ?? MAX_FLUSH_STATEMENT_SIZE
    }
  };
};

export const isPostgresStorageConfig = (config: configFile.BaseStorageConfig): config is PostgresStorageConfig => {
  return config.type == lib_postgres.POSTGRES_CONNECTION_TYPE;
};
