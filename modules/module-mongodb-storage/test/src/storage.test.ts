import { register } from '@powersync/service-core-tests';
import { describe } from 'vitest';
import { INITIALIZED_MONGO_STORAGE_FACTORY } from './util.js';

describe('Mongo Sync Bucket Storage', () => register.registerDataStorageTests(INITIALIZED_MONGO_STORAGE_FACTORY));

describe('Sync Bucket Validation', register.registerBucketValidationTests);
