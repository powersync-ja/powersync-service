import { MongoCompactOptions } from '@module/storage/implementation/MongoCompactor.js';
import { register } from '@powersync/service-core-tests';
import { describe } from 'node:test';
import { INITIALIZED_MONGO_STORAGE_FACTORY } from './util.js';

describe('Mongo Sync Bucket Storage Compact', () => register.registerCompactTests<MongoCompactOptions>(INITIALIZED_MONGO_STORAGE_FACTORY, { clearBatchLimit: 2, moveBatchLimit: 1, moveBatchQueryLimit: 1 }));