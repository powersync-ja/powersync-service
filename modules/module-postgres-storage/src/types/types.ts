import * as pg_wire from '@powersync/service-jpgwire';
import * as pg_types from '@powersync/service-module-postgres/types';
import * as t from 'ts-codec';
export * as models from './models/models-index.js';

export const MAX_BATCH_RECORD_COUNT = 2000;

export const MAX_BATCH_ESTIMATED_SIZE = 5_000_000;

export const MAX_BATCH_CURRENT_DATA_SIZE = 50_000_000;

export const BatchLimits = t.object({
  /**
   * Maximum size of operations we write in a single transaction.
   */
  max_estimated_size: t.number.optional(),
  /**
   * Limit number of documents to write in a single transaction.
   */
  max_record_count: t.number.optional()
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

export const BaseStorageConfig = t.object({
  /**
   * Allow batch operation limits to be configurable.
   * Postgres has less batch size restrictions compared to MongoDB.
   * Increasing limits can drastically improve replication performance, but
   * can come at the cost of higher memory usage or potential issues.
   */
  batch_limits: OperationBatchLimits.optional()
});

export type BaseStorageConfig = t.Encoded<typeof BaseStorageConfig>;

export const PostgresStorageConfig = pg_types.PostgresConnectionConfig.and(BaseStorageConfig);
export type PostgresStorageConfig = t.Encoded<typeof PostgresStorageConfig>;
export type PostgresStorageConfigDecoded = t.Decoded<typeof PostgresStorageConfig>;

export type RequiredOperationBatchLimits = Required<OperationBatchLimits>;

export type NormalizedPostgresStorageConfig = pg_wire.NormalizedConnectionConfig & {
  batch_limits: RequiredOperationBatchLimits;
};

export const normalizePostgresStorageConfig = (
  baseConfig: PostgresStorageConfigDecoded
): NormalizedPostgresStorageConfig => {
  return {
    ...pg_types.normalizeConnectionConfig(baseConfig),
    batch_limits: {
      max_current_data_batch_size: baseConfig.batch_limits?.max_current_data_batch_size ?? MAX_BATCH_CURRENT_DATA_SIZE,
      max_estimated_size: baseConfig.batch_limits?.max_estimated_size ?? MAX_BATCH_ESTIMATED_SIZE,
      max_record_count: baseConfig.batch_limits?.max_record_count ?? MAX_BATCH_RECORD_COUNT
    }
  };
};
