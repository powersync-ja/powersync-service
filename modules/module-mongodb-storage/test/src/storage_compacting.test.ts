import { register } from '@powersync/service-core-tests';
import { describe } from 'vitest';
import { INITIALIZED_MONGO_STORAGE_FACTORY } from './util.js';

describe('Mongo Sync Bucket Storage Compact', () => register.registerCompactTests(INITIALIZED_MONGO_STORAGE_FACTORY));
describe('Mongo Sync Parameter Storage Compact', () =>
  register.registerParameterCompactTests(INITIALIZED_MONGO_STORAGE_FACTORY));
