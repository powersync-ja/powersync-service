import { mongo } from '@powersync/lib-service-mongodb';
import { describe, expect, test, vi } from 'vitest';
import { INITIALIZED_MONGO_STORAGE_FACTORY } from './util.js';

describe('storage metrics collection sizes', () => {
  test('uses sequential estimated dataSize commands for logical sizes', async () => {
    await using factory = await INITIALIZED_MONGO_STORAGE_FACTORY.factory();
    const before = await factory.getStorageMetrics();
    const document = { _id: new mongo.ObjectId(), value: 'payload' };
    const names = ['bucket_data_999_metrics', 'parameter_index_999_metrics', 'source_records_999_metrics'];
    for (const name of names) {
      await factory.db.db.collection(name).insertOne(document);
    }
    const expectedSize = mongo.BSON.calculateObjectSize(document);
    const command = factory.db.db.command.bind(factory.db.db);
    let inFlight = 0;
    let maxInFlight = 0;
    using commands = vi.spyOn(factory.db.db, 'command').mockImplementation(async (...args) => {
      inFlight++;
      maxInFlight = Math.max(maxInFlight, inFlight);
      try {
        return await command(...args);
      } finally {
        inFlight--;
      }
    });

    const after = await factory.getStorageMetrics();

    expect(after.operations_size_bytes - before.operations_size_bytes).toBe(expectedSize);
    expect(after.parameters_size_bytes - before.parameters_size_bytes).toBe(expectedSize);
    expect(after.replication_size_bytes - before.replication_size_bytes).toBe(expectedSize);
    expect(maxInFlight).toBe(1);
    for (const name of names) {
      expect(commands).toHaveBeenCalledWith(
        { dataSize: `${factory.db.db.databaseName}.${name}`, estimate: true },
        { readPreference: undefined }
      );
    }
  });

  test.each([
    { code: 59, errmsg: 'no such command: dataSize' },
    { code: 115, errmsg: 'Command dataSize is not supported' },
    { code: 8000, errmsg: 'dataSize is not allowed in this atlas tier' }
  ])('caches unsupported dataSize errors ($code) and falls back across union batches', async (response) => {
    await using factory = await INITIALIZED_MONGO_STORAGE_FACTORY.factory();
    const before = await factory.getStorageMetrics();
    const document = { _id: new mongo.ObjectId(), value: 'payload' };
    for (let index = 0; index < 101; index++) {
      await factory.db.db.collection(`bucket_data_999_metrics_${index}`).insertOne(document);
    }
    const expectedSize = 101 * mongo.BSON.calculateObjectSize(document);
    const command = factory.db.db.command.bind(factory.db.db);
    // Fail after a successful size query to check that fallback doesn't double-count partial results.
    using commands = vi
      .spyOn(factory.db.db, 'command')
      .mockImplementationOnce((...args) => command(...args))
      .mockRejectedValueOnce(new mongo.MongoServerError(response));

    const first = await factory.getStorageMetrics();
    expect(first.operations_size_bytes - before.operations_size_bytes).toBe(expectedSize);
    expect(commands).toHaveBeenCalledTimes(2);

    const additionalDocument = { _id: new mongo.ObjectId() };
    await factory.db.db.collection('bucket_data_999_metrics_0').insertOne(additionalDocument);
    const second = await factory.getStorageMetrics();
    expect(second.operations_size_bytes - first.operations_size_bytes).toBe(
      mongo.BSON.calculateObjectSize(additionalDocument)
    );
    expect(commands).toHaveBeenCalledTimes(2);
  });

  test.each([
    { code: 13, errmsg: 'not authorized to execute dataSize' },
    { code: 8000, errmsg: 'unrelated Atlas error' },
    { code: 11600, errmsg: 'interrupted at shutdown' }
  ])('propagates other errors ($code) and retries dataSize on the next refresh', async (response) => {
    await using factory = await INITIALIZED_MONGO_STORAGE_FACTORY.factory();
    await factory.db.db.collection('bucket_data_999_metrics').insertOne({ _id: new mongo.ObjectId() });
    const error = new mongo.MongoServerError(response);
    using commands = vi.spyOn(factory.db.db, 'command').mockRejectedValueOnce(error);

    await expect(factory.getStorageMetrics()).rejects.toBe(error);
    await expect(factory.getStorageMetrics()).resolves.toHaveProperty('operations_size_bytes');
    expect(commands.mock.calls.length).toBeGreaterThan(1);
  });
});
