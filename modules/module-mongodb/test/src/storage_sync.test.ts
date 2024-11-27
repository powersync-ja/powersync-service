import { register } from '@powersync/service-core-tests';
import { describe } from 'node:test';
import { INITIALIZED_MONGO_STORAGE_FACTORY } from './util.js';

describe('Mongo Sync Bucket Storage', () => register.registerSyncTests(INITIALIZED_MONGO_STORAGE_FACTORY));