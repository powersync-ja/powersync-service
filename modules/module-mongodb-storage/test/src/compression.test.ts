import { createMongoClient } from '@powersync/lib-service-mongodb';
import { describe, test } from 'vitest';
import { env } from './env.js';

describe('MongoDB connection compression', () => {
  for (let compressor of ['none', 'zlib', 'zstd', 'snappy']) {
    test(`connection with compressors=${compressor}`, async () => {
      const client = createMongoClient({
        uri: env.MONGO_TEST_URL + '?compressors=' + compressor,
        type: 'mongodb'
      });
      await client.connect();
      console.log(client.options.compressors);
      await client.db('admin').command({ ping: 1 });
      await client.close();
    });
  }
});
