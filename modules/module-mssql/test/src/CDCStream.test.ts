import { describe, expect, test } from 'vitest';
import { METRICS_HELPER, putOp } from '@powersync/service-core-tests';
import { ReplicationMetric } from '@powersync/service-types';
import { v4 as uuid } from 'uuid';
import { describeWithStorage, enableCDCForTable, INITIALIZED_MONGO_STORAGE_FACTORY } from './util.js';
import { storage } from '@powersync/service-core';
import { CDCStreamTestContext } from './CDCStreamTestContext.js';

const BASIC_SYNC_RULES = `
bucket_definitions:
  global:
    data:
      - SELECT id, description FROM "test_data"
`;

// describe('CDCStream tests', () => {
//   describeWithStorage({ timeout: 20_000 }, defineCDCStreamTests);
// });

defineCDCStreamTests(INITIALIZED_MONGO_STORAGE_FACTORY);

function defineCDCStreamTests(factory: storage.TestStorageFactory) {
  test('Initial snapshot sync', async () => {
    await using context = await CDCStreamTestContext.open(factory);
    const { connectionManager } = context;
    await context.updateSyncRules(BASIC_SYNC_RULES);

    await connectionManager.query(`CREATE TABLE test_data (id UNIQUEIDENTIFIER PRIMARY KEY, description VARCHAR(MAX))`);
    await enableCDCForTable({ connectionManager, schema: 'dbo', table: 'test_data' });
    const testId = uuid();
    await connectionManager.query(`INSERT INTO test_data(id, description) VALUES('${testId}','test1')`);

    const startRowCount = (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.ROWS_REPLICATED)) ?? 0;

    await context.replicateSnapshot();

    const endRowCount = (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.ROWS_REPLICATED)) ?? 0;
    const data = await context.getBucketData('global[]');
    expect(data).toMatchObject([putOp('test_data', { id: testId, description: 'test1' })]);
    expect(endRowCount - startRowCount).toEqual(1);
  });

  // test('Replicate basic values', async () => {
  //   await using context = await CDCStreamTestContext.open(factory);
  //   const { connectionManager } = context;
  //   await context.updateSyncRules(`
  // bucket_definitions:
  //   global:
  //     data:
  //       - SELECT id, description, num FROM "test_data"`);
  //
  //   await connectionManager.query(
  //     `CREATE TABLE test_data (id UNIQUEIDENTIFIER PRIMARY KEY, description VARCHAR(MAX), num BIGINT)`
  //   );
  //
  //   await context.replicateSnapshot();
  //
  //   const startRowCount = (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.ROWS_REPLICATED)) ?? 0;
  //   const startTxCount = (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.TRANSACTIONS_REPLICATED)) ?? 0;
  //
  //   await context.startStreaming();
  //   const testId = uuid();
  //   await connectionManager.query(
  //     `INSERT INTO test_data(id, description, num) VALUES('${testId}', 'test1', 1152921504606846976)`
  //   );
  //   const data = await context.getBucketData('global[]');
  //
  //   expect(data).toMatchObject([putOp('test_data', { id: testId, description: 'test1', num: 1152921504606846976n })]);
  //   const endRowCount = (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.ROWS_REPLICATED)) ?? 0;
  //   const endTxCount = (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.TRANSACTIONS_REPLICATED)) ?? 0;
  //   expect(endRowCount - startRowCount).toEqual(1);
  //   expect(endTxCount - startTxCount).toEqual(1);
  // });
}
