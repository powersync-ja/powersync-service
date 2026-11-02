import { test_utils } from '@module/index.js';
import { mongo } from '@powersync/lib-service-mongodb';
import { TestStorageFactory } from '@powersync/service-core';
import { METRICS_HELPER } from '@powersync/service-core-tests';
import { TEST_CONNECTION_OPTIONS } from './util.js';

export const ChangeStreamTestContext = test_utils.ChangeStreamTestContext;

/**
 * Supply this suite's environment and metrics to the exported context.
 */
export function openChangeStreamTestContext(
  factory: TestStorageFactory,
  options?: Omit<test_utils.ChangeStreamTestContextOptions, 'factory' | 'connectionOptions' | 'metrics'> & {
    mongoOptions?: Partial<test_utils.ChangeStreamTestContextOptions['connectionOptions']>;
  }
) {
  return ChangeStreamTestContext.open({
    ...options,
    factory,
    connectionOptions: { ...TEST_CONNECTION_OPTIONS, ...options?.mongoOptions },
    metrics: METRICS_HELPER.metricsEngine
  });
}

export async function setSnapshotHistorySeconds(client: mongo.MongoClient, seconds: number) {
  const { minSnapshotHistoryWindowInSeconds: currentValue } = await client
    .db('admin')
    .command({ getParameter: 1, minSnapshotHistoryWindowInSeconds: 1 });

  await client.db('admin').command({ setParameter: 1, minSnapshotHistoryWindowInSeconds: seconds });

  return {
    async [Symbol.asyncDispose]() {
      await client.db('admin').command({ setParameter: 1, minSnapshotHistoryWindowInSeconds: currentValue });
    }
  };
}
